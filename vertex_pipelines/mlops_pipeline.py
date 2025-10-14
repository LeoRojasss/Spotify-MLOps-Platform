"""
Vertex AI MLOps Pipeline - Clasificación 3 clases de popularidad Spotify
(0 = low, 1 = medium, 2 = high)
"""

from google.cloud import aiplatform
from kfp.dsl import component, pipeline, Input, Output, Artifact, Model, Metrics
from typing import NamedTuple
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# ============================================
# CONFIGURACIÓN GLOBAL
# ============================================
PROJECT_ID = os.getenv("PROJECT_ID", "spotify-mlops-platform")
REGION = os.getenv("REGION", "us-central1")
BUCKET_NAME = os.getenv("BUCKET_NAME", "gs://spotify-mlops-platform-bucket")
PIPELINE_ROOT = os.getenv("PIPELINE_ROOT", f"{BUCKET_NAME}/pipelines")
BQ_PROJECT = os.getenv("BQ_PROJECT", "spotify-mlops-platform")
BQ_DATASET = os.getenv("BQ_DATASET", "spotify")
BQ_TABLE = os.getenv("BQ_TABLE", "spotify_events")

aiplatform.init(project=PROJECT_ID, location=REGION)

# ============================================
# COMPONENTE 1: PREPARAR DATA (3 CLASES)
# ============================================
@component(
    base_image="gcr.io/deeplearning-platform-release/base-cpu",
    packages_to_install=[
        "google-cloud-bigquery==3.15.0",
        "pandas==2.2.2",
        "scikit-learn==1.3.2",
    ],
)
def prepare_data(
    project_id: str,
    dataset_id: str,
    table_id: str,
    run_id: str,
    output_train: Output[Artifact],
    output_test: Output[Artifact],
    output_metadata: Output[Artifact],
) -> NamedTuple("Outputs", [("n_samples", int), ("n_features", int)]):
    """Lee datos de BigQuery y genera dataset multiclase (0=low, 1=medium, 2=high)."""
    from google.cloud import bigquery
    import pandas as pd
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler
    import pickle, json

    client = bigquery.Client(project=project_id)

    query = f"""
        SELECT
            track_id,
            track_name,
            artist,
            album,
            SAFE_CAST(popularity AS INT64) AS popularity,
            event_type,
            country,
            SAFE_CAST(timestamp AS FLOAT64) AS timestamp
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE popularity IS NOT NULL
          AND event_type IS NOT NULL
          AND country IS NOT NULL
          AND timestamp IS NOT NULL
          AND SAFE_CAST(ingestion_timestamp AS TIMESTAMP) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
        LIMIT 20000
    """

    df = client.query(query).to_dataframe()
    print(f"✅ Datos leídos desde BigQuery: {len(df)} filas")

    if df.empty:
        raise ValueError("❌ No hay datos recientes en BigQuery. Verifica la tabla spotify_events.")

    # Convertir timestamp UNIX → hora del día
    df["hour_of_day"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.hour

    # Crear variable target (popularidad)
    def map_popularity(p):
        if p < 40:
            return 0
        elif p < 70:
            return 1
        else:
            return 2

    df["label"] = df["popularity"].apply(map_popularity)

    # Codificar categóricas
    df = pd.get_dummies(df, columns=["event_type", "country"], drop_first=True)

    drop_cols = ["label", "popularity", "timestamp", "track_id", "track_name", "artist", "album"]
    feature_cols = [c for c in df.columns if c not in drop_cols]
    X = df[feature_cols].copy()
    y = df["label"].copy()

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # Escalar hora del día
    from sklearn.preprocessing import StandardScaler
    scaler = StandardScaler()
    X_train["hour_of_day"] = scaler.fit_transform(X_train[["hour_of_day"]])
    X_test["hour_of_day"] = scaler.transform(X_test[["hour_of_day"]])

    # Guardar artefactos
    with open(output_train.path, "wb") as f:
        pickle.dump((X_train.values, y_train.values), f)
    with open(output_test.path, "wb") as f:
        pickle.dump((X_test.values, y_test.values), f)

    meta = {
        "n_samples": len(X_train),
        "n_features": X_train.shape[1],
        "feature_columns": feature_cols,
        "run_id": run_id,
        "target_classes": {0: "low", 1: "medium", 2: "high"},
    }
    with open(output_metadata.path, "w") as f:
        json.dump(meta, f)

    print(f"✅ Dataset preparado ({len(X_train)} train, {len(X_test)} test)")
    return (len(X_train), X_train.shape[1])


# ============================================
# COMPONENTE 2: ENTRENAR MODELO
# ============================================
@component(
    base_image="gcr.io/deeplearning-platform-release/base-cpu",
    packages_to_install=["scikit-learn==1.3.2"],
)
def train_model(input_train: Input[Artifact], model_output: Output[Model]) -> NamedTuple("Outputs", [("train_accuracy", float)]):
    """Entrena RandomForest multiclase."""
    import os, pickle
    from sklearn.ensemble import RandomForestClassifier

    with open(input_train.path, "rb") as f:
        X_train, y_train = pickle.load(f)

    model = RandomForestClassifier(n_estimators=200, max_depth=15, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)
    acc = model.score(X_train, y_train)
    print(f"🎯 Training accuracy: {acc:.4f}")

    os.makedirs(model_output.path, exist_ok=True)
    model_path = os.path.join(model_output.path, "model.pkl")
    with open(model_path, "wb") as f:
        pickle.dump(model, f)

    model_output.metadata["train_accuracy"] = acc
    print(f"✅ Modelo guardado en: {model_path}")
    return (acc,)


# ============================================
# COMPONENTE 3: EVALUAR MODELO (MULTICLASE)
# ============================================
@component(
    base_image="gcr.io/deeplearning-platform-release/base-cpu",
    packages_to_install=[
        "scikit-learn==1.3.2",
        "google-cloud-bigquery==3.15.0",
    ],
)
def evaluate_model(
    input_test: Input[Artifact],
    input_model: Input[Model],
    metrics_output: Output[Metrics],
    deploy_decision: Output[Artifact],
    project_id: str,
    dataset_id: str,
    table_name: str,
    run_id: str = "manual",
) -> NamedTuple("Outputs", [("test_accuracy", float)]):
    """Evalúa el modelo multiclase y guarda métricas en BigQuery."""
    import pickle, os
    from sklearn.metrics import accuracy_score, f1_score, classification_report
    from google.cloud import bigquery

    with open(input_test.path, "rb") as f:
        X_test, y_test = pickle.load(f)

    model_path = os.path.join(input_model.path, "model.pkl")
    with open(model_path, "rb") as f:
        model = pickle.load(f)

    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred, average="macro")

    print(f"📊 Accuracy: {acc:.4f} | F1: {f1:.4f}")
    print(classification_report(y_test, y_pred))

    metrics_output.log_metric("accuracy", acc)
    metrics_output.log_metric("macro_f1", f1)

    # Decisión de deploy
    decision = "yes" if acc >= 0.65 and f1 >= 0.6 else "no"
    with open(deploy_decision.path, "w") as f:
        f.write(decision)

    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        rows_to_insert = [{"run_id": run_id, "accuracy": acc, "f1_score": f1}]
        client.insert_rows_json(table_id, rows_to_insert)
        print(f"✅ Métricas insertadas en {table_id}")
    except Exception as e:
        print(f"⚠️ No se pudieron insertar métricas: {e}")

    return (acc,)


# ============================================
# COMPONENTE 4: DEPLOY MODELO
# ============================================
@component(
    base_image="gcr.io/deeplearning-platform-release/base-cpu",
    packages_to_install=["google-cloud-aiplatform==1.43.0"],
)
def deploy_model(project_id: str, region: str, model_input: Input[Model], deploy_decision: Input[Artifact]) -> str:
    """Sube el modelo al registro y lo despliega en Vertex AI."""
    from google.cloud import aiplatform

    with open(deploy_decision.path, "r") as f:
        decision = f.read().strip().lower()

    if decision != "yes":
        print("⚠️ Modelo no desplegado (métricas insuficientes).")
        return "not_deployed"

    aiplatform.init(project=project_id, location=region)

    artifact_uri = model_input.path.replace("/gcs/", "gs://").rstrip("/")
    model = aiplatform.Model.upload(
        display_name="spotify-popularity-3class-model",
        artifact_uri=artifact_uri,
        serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-3:latest",
    )

    endpoints = aiplatform.Endpoint.list(filter='display_name="spotify-popularity-endpoint"')
    endpoint = endpoints[0] if endpoints else aiplatform.Endpoint.create(display_name="spotify-popularity-endpoint")

    endpoint.deploy(
        model=model,
        deployed_model_display_name="spotify-popularity-classifier",
        machine_type="n1-standard-2",
        traffic_split={"0": 100},
        sync=True,
    )

    print(f"✅ Endpoint desplegado: {endpoint.resource_name}")
    return endpoint.resource_name


# ============================================
# PIPELINE PRINCIPAL
# ============================================
@pipeline(name="spotify-mlops-3class", pipeline_root=PIPELINE_ROOT)
def spotify_mlops_pipeline(run_id: str = "manual"):
    prep = prepare_data(
        project_id=BQ_PROJECT, dataset_id=BQ_DATASET, table_id=BQ_TABLE, run_id=run_id
    )
    train = train_model(input_train=prep.outputs["output_train"])
    eval_ = evaluate_model(
        input_test=prep.outputs["output_test"],
        input_model=train.outputs["model_output"],
        project_id=PROJECT_ID,
        dataset_id=BQ_DATASET,
        table_name="spotify_model_metrics",
        run_id=run_id,
    )
    _ = deploy_model(
        project_id=PROJECT_ID,
        region=REGION,
        model_input=train.outputs["model_output"],
        deploy_decision=eval_.outputs["deploy_decision"],
    )


# ============================================
# EJECUTAR PIPELINE
# ============================================
if __name__ == "__main__":
    from kfp import compiler
    from datetime import datetime

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    compiler.Compiler().compile(
        pipeline_func=spotify_mlops_pipeline, package_path="spotify_pipeline.yaml"
    )

    job = aiplatform.PipelineJob(
        display_name=f"spotify-mlops-3class-{run_id}",
        template_path="spotify_pipeline.yaml",
        pipeline_root=PIPELINE_ROOT,
        location=REGION,
        parameter_values={"run_id": run_id},
    )

    job.run(sync=False)
    print("✅ Pipeline lanzado correctamente.")
