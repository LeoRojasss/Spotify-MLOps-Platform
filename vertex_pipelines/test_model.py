from google.cloud import aiplatform
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID", "spotify-mlops-platform")
REGION = os.getenv("REGION", "us-central1")
ENDPOINT_ID = os.getenv("ENDPOINT_ID", "6532184286966054912")

aiplatform.init(project=PROJECT_ID, location=REGION)
endpoint = aiplatform.Endpoint(f"projects/{PROJECT_ID}/locations/{REGION}/endpoints/{ENDPOINT_ID}")

feature_columns = [
    "hour_of_day",
    "event_type_search_result",
    "country_CO",
    "country_ES",
    "country_MX",
    "country_US"
]

print("🧩 Orden de features:", feature_columns)

instances = [
    [15.0, 1, 1, 0, 0, 0],  # search desde CO, 3 p.m.
    [22.0, 0, 0, 0, 1, 0],  # play desde MX, 10 p.m.
]

response = endpoint.predict(instances=instances)
print("🎯 Predicciones:", response.predictions)
