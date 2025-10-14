import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
from pymongo import MongoClient
from apache_beam.io.kafka import ReadFromKafka
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ParseJSON(beam.DoFn):
    """Convierte el mensaje binario de Kafka en JSON válido"""
    def process(self, element):
        try:
            if isinstance(element, tuple) and len(element) == 2:
                key, value = element
                logger.info(f"RECIBIDO: key={key}, partition=N/A")
                logger.info(f"VALOR: {str(value)[:200]}...")
                
                if isinstance(value, bytes):
                    record = json.loads(value.decode("utf-8"))
                else:
                    record = json.loads(str(value))
            else:
                logger.info(f"RECIBIDO: elemento directo de tipo {type(element)}")
                if hasattr(element, 'value'):
                    record = json.loads(element.value.decode("utf-8"))
                else:
                    if isinstance(element, bytes):
                        record = json.loads(element.decode("utf-8"))
                    else:
                        record = json.loads(str(element))
            
            record["timestamp"] = datetime.fromtimestamp(record["timestamp"]).isoformat()
            logger.info(f"Registro parseado exitosamente: track_id={record.get('track_id', 'N/A')}, user_id={record.get('user_id', 'N/A')}")
            yield record
        except Exception as e:
            logger.error(f"Error parsing record: {e}")
            logger.error(f"Element type: {type(element)}")
            logger.error(f"Element: {element}")
            pass


class WriteToMongoDB(beam.DoFn):
    """Guarda cada evento procesado en MongoDB"""
    def __init__(self, uri, db, coll):
        self.uri = uri
        self.db_name = db
        self.coll_name = coll

    def setup(self):
        logger.info("Conectando a MongoDB...")
        self.client = MongoClient(self.uri)
        self.collection = self.client[self.db_name][self.coll_name]
        logger.info("Conectado a MongoDB")

    def process(self, element):
        try:
            result = self.collection.insert_one(element)
            logger.info(f"Guardado en MongoDB: {result.inserted_id}")
            yield element
        except Exception as e:
            logger.error(f"MongoDB write error: {e}")

    def teardown(self):
        self.client.close()


class WriteToFile(beam.DoFn):
    """Guarda eventos localmente en un archivo JSON"""
    def __init__(self, output_path):
        self.output_path = output_path

    def setup(self):
        import os
        os.makedirs(self.output_path, exist_ok=True)
        logger.info(f"Directorio preparado: {self.output_path}")

    def process(self, element):
        try:
            filename = f"{self.output_path}/events_{datetime.utcnow().strftime('%Y%m%d_%H%M%S%f')}.json"
            with open(filename, "w") as f:
                json.dump(element, f, indent=2)
            logger.info(f"Archivo guardado: {filename}")
            yield filename
        except Exception as e:
            logger.error(f"Error escribiendo archivo: {e}")


def run():
    import os
    import time

    kafka_broker = os.getenv("KAFKA_BROKER", "localhost:29092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "spotify-events")
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    mongo_db = os.getenv("MONGO_DB", "spotify")
    output_path = os.getenv("OUTPUT_PATH", "./data/raw")

    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    microseconds = int(time.time() * 1000000) % 1000000
    consumer_group = f"beam-batch-historical-{timestamp}-{microseconds}"
    
    logger.info("Iniciando pipeline BATCH (TODOS LOS MENSAJES HISTÓRICOS)...")
    logger.info(f"Kafka: {kafka_broker}")
    logger.info(f"Topic: {kafka_topic}")
    logger.info(f"MongoDB: {mongo_uri}")
    logger.info(f"Guardando archivos en: {output_path}")
    logger.info(f"Consumer Group: {consumer_group}")
    logger.info(f"Modo: BATCH - procesará desde offset 0 hasta 5918")
    logger.info(f"Esto NO es streaming - procesará TODOS los mensajes y terminará")

    options = PipelineOptions(
        streaming=False,
        save_main_session=True
    )

    pipeline = beam.Pipeline(options=options)
    
    events = (
        pipeline
        | "Leer desde Kafka" >> ReadFromKafka(
            consumer_config={
                "bootstrap.servers": kafka_broker,
                "group.id": consumer_group,  # Group ID único
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
                "session.timeout.ms": "30000",
                "request.timeout.ms": "40000",
                "max.poll.records": "100",
                "fetch.min.bytes": "1",
                "fetch.max.wait.ms": "1000"
            },
            topics=[kafka_topic],
            start_read_time=0,
            max_num_records=5919
        )
        | "Parsear JSON" >> beam.ParDo(ParseJSON())
    )

    # Guardar en MongoDB
    events | "Guardar en MongoDB" >> beam.ParDo(
        WriteToMongoDB(mongo_uri, mongo_db, "events")
    )

    # Guardar en archivos locales
    events | "Guardar en archivo local" >> beam.ParDo(WriteToFile(output_path))

    # Mostrar en consola y contar
    events | "Imprimir y contar" >> beam.Map(lambda x: logger.info(f"📄 Evento procesado: {x['track_id']} de {x['user_id']}") or x)

    logger.info("Pipeline BATCH configurado. Ejecutando...")
    logger.info("PROCESANDO TODOS LOS MENSAJES HISTÓRICOS (0-5918)...")
    logger.info("Esto debería terminar cuando procese todos los mensajes...")
    
    # Ejecutar el pipeline batch
    result = pipeline.run()
    result.wait_until_finish()
    
    logger.info("¡PIPELINE BATCH COMPLETADO!")
    logger.info("Revisa los archivos generados en ./data/raw/")
    logger.info("Revisa MongoDB para los registros guardados")


if __name__ == "__main__":
    run()