from google.cloud import aiplatform, bigquery
from pymongo import MongoClient
import random, time
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID", "spotify-mlops-platform")
REGION = os.getenv("REGION", "us-central1")
GCS_BUCKET = os.getenv("GCS_BUCKET", "gs://spotify-mlops-platform-bucket")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "spotify_db")

# ========================================
# CONFIGURACIÓN GLOBAL
# ========================================
ENDPOINT_ID = "6532184286966054912"
BQ_TABLE = "spotify-mlops-platform.spotify.spotify_predictions"
MONGO_COLLECTION = "spotify_predictions"

aiplatform.init(project=PROJECT_ID, location=REGION)
endpoint = aiplatform.Endpoint(ENDPOINT_ID)
bq = bigquery.Client(project=PROJECT_ID)
mongo = MongoClient(MONGO_URI)[MONGO_DB][MONGO_COLLECTION]

# ========================================
# ESTRUCTURA DE FEATURES
# Basado en tu metadata.json
# hour_of_day, event_type_search_result, country_CO, country_ES, country_MX, country_US
# ========================================
def generate_random_instance():
    hour_of_day = random.randint(0, 23)
    event_type_search_result = random.choice([0, 1])
    country_flags = {
        "country_CO": 0,
        "country_ES": 0,
        "country_MX": 0,
        "country_US": 0,
    }
    random_country = random.choice(list(country_flags.keys()))
    country_flags[random_country] = 1
    instance = [hour_of_day, event_type_search_result] + list(country_flags.values())
    return instance, random_country

# ========================================
# LOOP DE PREDICCIÓN Y LOGGING
# ========================================
while True:
    instance, country = generate_random_instance()
    print(f"Enviando instancia: {instance}")

    response = endpoint.predict(instances=[instance])
    prediction = int(response.predictions[0])
    label = {0: "low", 1: "medium", 2: "high"}.get(prediction, "unknown")

    event = {
        "timestamp": time.time(),
        "features": {
            "hour_of_day": instance[0],
            "event_type_search_result": instance[1],
            "country": country
        },
        "prediction": label
    }

    mongo.insert_one(event.copy())

    bq.insert_rows_json(BQ_TABLE, [event])

    print(f"Guardado → Predicción: {label}")
    time.sleep(5)
