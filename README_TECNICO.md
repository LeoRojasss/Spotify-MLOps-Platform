# Documentación Técnica – Guía Operacional (Paso a Paso)
Reproduce el proyecto de analítica en streaming con MLOps para predecir popularidad musical (Alta/Media/Baja) usando GCP + Kafka + Dataflow + BigQuery + Vertex AI + Grafana.

> Este documento es operativo. Describe cómo levantar la infraestructura, ejecutar los pipelines y validar resultados.

---

## 1. Prerrequisitos

### 1.1. Accesos y permisos GCP
- Proyecto: `spotify-mlops-platform`
- Habilitar APIs: Vertex AI, Dataflow, BigQuery, Cloud Storage, IAM
- Rol recomendado: `Editor` o superior (o granular: Vertex AI Admin, Dataflow Developer, BigQuery Admin, Storage Admin)
- Se recomienda crear un env de python para instalar dependencias

### 1.2. Herramientas locales
- Python 3.10+
- Docker y Docker Compose
- gcloud CLI
- Dependencias Python:
```bash
pip install google-cloud-aiplatform apache-beam[gcp] kafka-python pymongo             google-cloud-bigquery google-cloud-storage
```

### 1.3. Autenticación
```bash
gcloud auth login
gcloud config set project spotify-mlops-platform
gcloud auth application-default login
```

---

## 2. Estructura del repositorio
```
spotify-mlops-platform/
├─ dataflow/
│  ├─ consumer.py               # Pipeline streaming Kafka → BigQuery/Mongo/GCS
|  ├─ ingest_batch_historical.py
├─ vertex_pipelines/
│  ├─ mlops_pipeline.py
├─ producers/
|  ├─ producers.py
├─ predictions/
|  ├─ predictions.py   
└─ README_TECNICO.md
└─ README.md
└─ docker-compose.yml
└─ requirements.txt
```

---

## 3. Infraestructura GCP

### 3.1. Cloud Storage (bucket)
```bash
gsutil mb -l us-central1 gs://spotify-mlops-platform-bucket
```

Estructura sugerida:
```
gs://spotify-mlops-platform-bucket/
├─ raw/                # dumps crudos
├─ staging/            # staging Dataflow
├─ temp/               # temp Dataflow
└─ pipelines/          # artefactos Vertex AI (model_output/)
```

### 3.2. BigQuery (dataset y tablas)
Crear dataset `spotify` y tablas base:

```sql
-- Dataset (si no existe, desde consola o bq CLI)
-- bq mk --dataset spotify-mlops-platform:spotify

-- Tabla de eventos
CREATE TABLE IF NOT EXISTS `spotify-mlops-platform.spotify.spotify_events` (
  event_timestamp TIMESTAMP,
  hour_of_day INT64,
  event_type_search_result STRING,
  country_CO BOOL,
  country_ES BOOL,
  country_MX BOOL,
  country_US BOOL,
  track_id STRING,
  user_id STRING,
  ingestion_timestamp TIMESTAMP
);

-- Tabla de predicciones
CREATE TABLE IF NOT EXISTS `spotify-mlops-platform.spotify.spotify_predictions` (
  prediction_timestamp TIMESTAMP,
  hour_of_day INT64,
  event_type_search_result STRING,
  country_CO BOOL,
  country_ES BOOL,
  country_MX BOOL,
  country_US BOOL,
  track_id STRING,
  popularity_level STRING,       -- Alta | Media | Baja
  model_version STRING
);

-- Tabla de métricas del modelo
CREATE TABLE IF NOT EXISTS `spotify-mlops-platform.spotify.spotify_model_metrics` (
  run_time TIMESTAMP,
  accuracy FLOAT64,
  f1_score FLOAT64,
  model_version STRING
);
```

### 3.3. MongoDB Atlas (opcional NoSQL)
- Cluster M0 gratuito
- Base: `spotify_db`, colección: `events`
- URI ejemplo: `mongodb+srv://<user>:<pass>@cluster.mongodb.net/`

---

## 4. Kafka local (Docker Compose)

### 4.1. docker-compose.yml
```yaml
version: "3.9"
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.4
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.4.4
    depends_on:
      - zookeeper
    ports:
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
```

### 4.2. Levantar servicios y topic
```bash
docker compose up -d
docker exec -it $(docker ps -qf "ancestor=confluentinc/cp-kafka:7.4.4")   kafka-topics --create --topic spotify-events --bootstrap-server localhost:29092
```

### 4.3. Probar productor (opcional)
```bash
python dataflow/producer.py
```

---

## 5. Pipeline de streaming (Apache Beam → Dataflow)
Archivo: `dataflow/consumer.py`

### 5.1. Ejecución local (prueba)
```bash
python dataflow/ingest_batch_historical.py
```

### 5.2. Ejecución en Dataflow (producción)
```bash
python dataflow/consumer.py   --runner DataflowRunner   --project spotify-mlops-platform   --region us-central1   --temp_location gs://spotify-mlops-platform-bucket/temp   --staging_location gs://spotify-mlops-platform-bucket/staging   --streaming
```

El pipeline debe:
- Leer mensajes de Kafka (topic `spotify-events`)
- Transformar y validar campos
- Escribir en BigQuery (`spotify_events`) y MongoDB (`spotify_db.events`)
- Opcional: volcar crudos en GCS (`raw/`)

---

## 6. Entrenamiento del modelo (Vertex AI)

### 6.1. Script de entrenamiento

````bash
python3 vertex_pipelines/mlops_pipeline.yml
````

Con este Script se crean todos los pasos necesarios para el pipeline MLOps donde se preparan los datos, se entrena, evalua y despliega el modelo para poder ser utilizado.
----

## 7. Predicciones automáticas (batch/stream → BigQuery)
Archivo: `predictors/predictions.py`

Objetivo:
- Tomar registros aleatorios con campos:
  `hour_of_day, event_type_search_result, country_CO, country_ES, country_MX, country_US`
- Llamar al endpoint de Vertex AI
- Persistir en `spotify.spotify_predictions` con `popularity_level` y `model_version`

---

## 8. Visualización (Grafana + BigQuery)

### 8.1. Despliegue de Grafana en VM Docker
Grafana se despliega en una VM utilizando Docker con un contenedor persistente:

```bash
sudo docker run -d \
  -p 3000:3000 \
  --name grafana \
  -v grafana-storage:/var/lib/grafana \
  grafana/grafana:latest
```

**Detalles del comando:**
- `-d`: Ejecuta el contenedor en modo detached (segundo plano)
- `-p 3000:3000`: Expone Grafana en el puerto 3000
- `--name grafana`: Asigna nombre al contenedor para facilitar gestión
- `-v grafana-storage:/var/lib/grafana`: Volumen persistente para configuraciones y dashboards
- `grafana/grafana:latest`: Imagen oficial de Grafana

**Acceso inicial:**
- URL: `http://<IP_VM>:3000`
- Usuario por defecto: `admin`
- Contraseña por defecto: `admin` (se solicitará cambio en primer login)

### 8.2. Configuración del conector BigQuery
1. En Grafana, ir a **Configuration → Data Sources → Add data source**
2. Seleccionar **Google BigQuery**
3. Opciones de autenticación:
   - **Opción A (recomendada):** Usar Service Account Key (JSON)
   - **Opción B:** ADC (Application Default Credentials) si la VM tiene permisos IAM configurados
4. Configurar proyecto: `spotify-mlops-platform`
5. Test & Save

### 8.3. Paneles sugeridos
Crear dashboards con las siguientes visualizaciones:

**Panel 1: Distribución de popularidad**
```sql
SELECT popularity_level, COUNT(*) AS total
FROM `spotify-mlops-platform.spotify.spotify_predictions`
GROUP BY popularity_level
ORDER BY total DESC;
```

**Panel 2: Volumen de eventos por minuto**
```sql
SELECT
  TIMESTAMP_TRUNC(event_timestamp, MINUTE) AS minute,
  COUNT(*) AS event_count
FROM `spotify-mlops-platform.spotify.spotify_events`
WHERE event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY minute
ORDER BY minute DESC;
```

**Panel 3: Métricas del modelo**
```sql
SELECT
  run_time,
  accuracy,
  f1_score,
  model_version
FROM `spotify-mlops-platform.spotify.spotify_model_metrics`
ORDER BY run_time DESC
LIMIT 20;
```

**Panel 4: Popularidad por país**
```sql
SELECT
  CASE
    WHEN country_CO THEN 'Colombia'
    WHEN country_ES THEN 'España'
    WHEN country_MX THEN 'México'
    WHEN country_US THEN 'Estados Unidos'
    ELSE 'Otros'
  END AS country,
  popularity_level,
  COUNT(*) AS total
FROM `spotify-mlops-platform.spotify.spotify_predictions`
GROUP BY country, popularity_level
ORDER BY total DESC;
```

### 8.4. Gestión del contenedor Grafana
```bash
# Ver logs
sudo docker logs grafana

# Detener contenedor
sudo docker stop grafana

# Iniciar contenedor existente
sudo docker start grafana

# Reiniciar contenedor
sudo docker restart grafana

# Backup del volumen
sudo docker run --rm -v grafana-storage:/data -v $(pwd):/backup ubuntu tar czf /backup/grafana-backup.tar.gz /data
```

---

## 9. Validaciones rápidas

### 9.1. Ver eventos en BigQuery
```sql
SELECT * FROM `spotify-mlops-platform.spotify.spotify_events`
ORDER BY ingestion_timestamp DESC
LIMIT 50;
```

### 9.2. Ver predicciones recientes
```sql
SELECT * FROM `spotify-mlops-platform.spotify.spotify_predictions`
ORDER BY prediction_timestamp DESC
LIMIT 50;
```

### 9.3. Ver métricas del modelo
```sql
SELECT * FROM `spotify-mlops-platform.spotify.spotify_model_metrics`
ORDER BY run_time DESC
LIMIT 20;
```

---

## 10. Troubleshooting común

- Dataflow: ruta `temp`/`staging` con soft-delete puede generar costos residuales.
- Vertex AI: error "expected to contain exactly one of: [model.pkl, model.joblib]" → verifique nombre y ubicación en `model_output/`.
- Kafka: `advertised.listeners` no debe ser `0.0.0.0`. Usar `localhost:29092` en local.
- BigQuery: validar tipos (STRING vs TIMESTAMP) al castear y al ordenar por tiempo.

---

## 11. Replicación en otro proyecto GCP (resumen)

1. Crear proyecto y habilitar APIs.
2. Crear bucket GCS y dataset BigQuery.
3. Levantar Kafka y topic `spotify-events`.
4. Ejecutar `dataflow/consumer.py` en Dataflow (streaming).
5. Ejecutar entrenamiento `vertex_pipelines/mlops_pipeline.py
6. Ejecutar `predictions/predictions.py`; verificar `spotify_predictions`.
7. Conectar Grafana a BigQuery; importar/crear paneles.
