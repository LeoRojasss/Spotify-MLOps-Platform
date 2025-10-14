from google.cloud import bigquery, aiplatform
from datetime import datetime
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID", "spotify-mlops-platform")
REGION = os.getenv("REGION", "us-central1")
PIPELINE_TEMPLATE = os.getenv("PIPELINE_TEMPLATE", "gs://spotify-mlops-platform-bucket/pipelines/spotify_pipeline.yaml")
PIPELINE_ROOT = os.getenv("PIPELINE_ROOT", "gs://spotify-mlops-platform-bucket/pipelines")
BQ_DATASET = os.getenv("BQ_DATASET", "spotify")
BQ_TABLE = os.getenv("BQ_DATASET_MONITORING", "spotify_model_metrics")

def trigger_retrain(request):
    """Reentrena automáticamente si accuracy < threshold o manual si ?force=true"""

    force = request.args.get("force", "false").lower() == "true"
    threshold = float(os.getenv("THRESHOLD", 0.75))

    print(f"📡 Trigger recibido | force={force} | threshold={threshold}")

    # 1️⃣ Leer la última métrica desde BigQuery
    accuracy = 1.0
    bq = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT accuracy
        FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
        ORDER BY timestamp DESC
        LIMIT 1
    """
    try:
        rows = list(bq.query(query))
        if rows:
            accuracy = rows[0].accuracy
    except Exception as e:
        print(f"⚠️ No se pudo leer métrica de BigQuery: {e}")

    print(f"📊 Último accuracy: {accuracy}")

    # 2️⃣ Si baja del umbral o se fuerza → lanzar el pipeline
    if force or accuracy < threshold:
        aiplatform.init(project=PROJECT_ID, location=REGION)
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        job = aiplatform.PipelineJob(
            display_name=f"spotify-mlops-retrain-{run_id}",
            template_path=PIPELINE_TEMPLATE,
            pipeline_root=PIPELINE_ROOT,
            location=REGION,
            parameter_values={"run_id": run_id},
        )

        job.run(sync=False)
        msg = f"🚀 Reentrenamiento lanzado (accuracy={accuracy:.2f})"
        print(msg)
        return msg, 200

    msg = f"✅ Modelo estable (accuracy={accuracy:.2f}), no se reentrena."
    print(msg)
    return msg, 200
