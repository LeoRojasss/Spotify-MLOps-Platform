from google.cloud import aiplatform
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID", "spotify-mlops-platform")
REGION = os.getenv("REGION", "us-central1")

aiplatform.init(project=PROJECT_ID, location=REGION)

model = aiplatform.Model.upload(
    display_name="spotify-events-model-test",
    artifact_uri="gs://spotify-mlops-platform-bucket/pipelines/259336751727/spotify-mlops-pipeline-20251012035921/train-model_2502149093682315264/model_output.pkl",
    serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-3:latest",
)

endpoint = model.deploy(
    deployed_model_display_name="spotify-events-endpoint-test",
    machine_type="n1-standard-2",
)
print(f"Modelo desplegado: {model.resource_name}")