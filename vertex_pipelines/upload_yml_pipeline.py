from google.cloud import aiplatform, storage
from kfp import compiler
from mlops_pipeline import spotify_mlops_pipeline  # importa tu pipeline Python
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID", "spotify-mlops-platform")
REGION = os.getenv("REGION", "us-central1")
BUCKET_NAME = os.getenv("BUCKET_NAME", "spotify-mlops-platform-bucket")
PIPELINE_FILENAME = os.getenv("PIPELINE_FILENAME", "spotify_pipeline.yaml")
PIPELINE_LOCAL_PATH = f"/tmp/{PIPELINE_FILENAME}"
DESTINATION_BLOB = f"pipelines/{PIPELINE_FILENAME}"

aiplatform.init(project=PROJECT_ID, location=REGION)

compiler.Compiler().compile(
    pipeline_func=spotify_mlops_pipeline,
    package_path=PIPELINE_LOCAL_PATH,
)
print(f"Pipeline compilado: {PIPELINE_LOCAL_PATH}")

storage_client = storage.Client(project=PROJECT_ID)
bucket = storage_client.bucket(BUCKET_NAME)
blob = bucket.blob(DESTINATION_BLOB)
blob.upload_from_filename(PIPELINE_LOCAL_PATH)

print(f"Pipeline subido a: gs://{BUCKET_NAME}/{DESTINATION_BLOB}")