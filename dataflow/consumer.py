import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam import window, trigger
from datetime import datetime
import logging, json, os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID", "spotify-mlops-platform")
REGION = os.getenv("REGION", "us-central1")
GCS_BUCKET = os.getenv("GCS_BUCKET", "gs://spotify-mlops-platform-bucket")
BQ_DATASET = os.getenv("BQ_DATASET", "spotify")
BQ_TABLE = f"{PROJECT_ID}:{BQ_DATASET}.{os.getenv('BQ_TABLE', 'spotify_events')}"

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "spotify_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "events")

class ParseAndEnrich(beam.DoFn):
    """Parsea mensajes JSON desde Kafka y agrega metadatos"""
    def process(self, element):
        try:
            key, value = element
            record = json.loads(value.decode("utf-8"))

            record["ingestion_timestamp"] = datetime.utcnow().isoformat()

            logging.info("[PARSED] %s", json.dumps(record))
            yield record
        except Exception as e:
            logging.error("[ERROR ParseAndEnrich] %s", e)


class SaveToMongo(beam.DoFn):
    """Envía cada evento a MongoDB Atlas"""
    def setup(self):
        try:
            from pymongo import MongoClient
            self.client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
            self.client.admin.command("ping")
            self.db = self.client[MONGO_DB]
            self.collection = self.db[MONGO_COLLECTION]
            logging.info("[MongoDB] ✅ Conexión establecida correctamente")
        except Exception as e:
            logging.error("[MongoDB] ❌ Error de conexión: %s", e)
            self.client = None

    def process(self, element):
        if not self.client:
            logging.warning("[MongoDB] No conectado, se omite inserción")
            yield element
            return
        try:
            self.collection.insert_one(element)
            logging.info("[MongoDB] Insertado: %s", element.get("track_id"))
        except Exception as e:
            logging.error("[MongoDB] Error insertando documento: %s", e)
        yield element

    def teardown(self):
        if self.client:
            self.client.close()
            logging.info("[MongoDB] 🔒 Conexión cerrada")


class FormatForBigQuery(beam.DoFn):
    """Formatea los datos al esquema de BigQuery"""
    def process(self, element):
        try:
            yield {
                "track_id": element.get("track_id"),
                "track_name": element.get("track_name"),
                "artist": element.get("artist"),
                "album": element.get("album"),
                "popularity": int(element.get("popularity", 0)),
                "event_type": element.get("event_type"),
                "country": element.get("country"),
                "timestamp": float(element.get("timestamp", 0)),
                "ingestion_timestamp": str(element.get("ingestion_timestamp")),
            }
        except Exception as e:
            logging.error("[ERROR FormatForBigQuery] %s", e)

if __name__ == "__main__":
    options = PipelineOptions(
        runner="DataflowRunner",
        project=PROJECT_ID,
        region=REGION,
        job_name="spotify-realtime-consumer",
        temp_location=f"{GCS_BUCKET}/temp",
        staging_location=f"{GCS_BUCKET}/staging",
        streaming=True,
        requirements_file="requirements.txt",
        experiments=["use_runner_v2"],
    )
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        parsed = (
            p
            | "ReadFromKafka" >> ReadFromKafka(
                consumer_config={
                    "bootstrap.servers": "35.225.42.144:29092",
                    "group.id": "spotify-realtime-consumer",
                    "auto.offset.reset": "latest",
                },
                topics=["spotify-events"],
            )
            | "ParseAndEnrich" >> beam.ParDo(ParseAndEnrich())
        )

        # ---- Sink 1: MongoDB ----
        _ = parsed | "SaveToMongo" >> beam.ParDo(SaveToMongo())

        # ---- Sink 2: BigQuery ----
        _ = (
            parsed
            | "FormatForBigQuery" >> beam.ParDo(FormatForBigQuery())
            | "WriteToBigQuery" >> WriteToBigQuery(
                table=BQ_TABLE,
                schema=(
                    "track_id:STRING, "
                    "track_name:STRING, "
                    "artist:STRING, "
                    "album:STRING, "
                    "popularity:INTEGER, "
                    "event_type:STRING, "
                    "country:STRING, "
                    "timestamp:FLOAT, "
                    "ingestion_timestamp:STRING"
                ),
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        # ---- Sink 3: GCS (raw JSON) ----
        json_lines = parsed | "ToJson" >> beam.Map(json.dumps)

        windowed = json_lines | "Window1m" >> beam.WindowInto(
            window.FixedWindows(60),
            trigger=trigger.Repeatedly(trigger.AfterProcessingTime(60)),
            accumulation_mode=trigger.AccumulationMode.DISCARDING,
        )

        _ = (
            windowed
            | "WriteRawToGCS" >> fileio.WriteToFiles(
                path=f"{GCS_BUCKET}/datalake/raw/events",
                destination=lambda _: "events",
                file_naming=fileio.destination_prefix_naming(suffix=".json"),
                shards=5,
            )
        )

        logging.info("🚀 Dataflow corriendo con 3 sinks: MongoDB + BigQuery + GCS")
