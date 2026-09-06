# Lakehouse Retail Pipeline

End-to-End-Datenpipeline für Fashion-Retail Sales & Inventory Analytics — Portfolio-Projekt zum Aufbau von GCP- und Airflow-Kenntnissen neben bestehender Databricks/PySpark/Power-BI-Erfahrung.

## Architektur

Bronze/Silver/Gold-Lakehouse-Muster:

- **Bronze** (`data_lake/bronze/`) — Rohdaten aus `Data/` (7 CSVs: campaigns, channels, customers, products, sales, salesitems, stock), unverändert als Parquet
- **Silver** (`data_lake/silver/`) — bereinigte Einzeltabellen (z.B. `discount_percent` als float statt String)
- **Gold** (`data_lake/gold/`) — `transaktion_data.parquet`, denormalisierte Tabelle auf Item-Ebene für Reporting

Orchestriert über Apache Airflow (lokal, Docker, `LocalExecutor`). Geplant: GCP als paralleles Zielsystem (GCS + BigQuery), CI/CD via GitHub Actions, Dashboard (Power BI/Streamlit).

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Pipeline ausführen

```bash
cd src
python pipeline.py
```

Führt Bronze → Silver → Gold sequenziell aus und schreibt die Ergebnisse nach `data_lake/`.

## Airflow (lokal, Docker)

```bash
docker-compose up airflow-init   # einmalig
docker-compose up -d
```

Web-UI: http://localhost:8080 (Login: `airflow` / `airflow`)

Voraussetzung: `.env` mit `AIRFLOW_UID` (eigene User-ID, siehe `id -u`).

## Projektstruktur

```
src/
├── ingestion/    # CSV → Bronze
├── transform/    # Bronze → Silver, Merge zu Gold
├── gcp/          # geplant: GCS/BigQuery-Integration
└── governance/   # geplant: Unity-Catalog-artige Governance
dags/             # Airflow-DAGs
notebooks/        # explorative Analyse (EDA)
tests/            # geplant: Pytest für Transform-Logik
```

## Status

- [x] Bronze/Silver/Gold-Pipeline (lokal, pandas)
- [x] Airflow lokal via Docker (LocalExecutor)
- [ ] Airflow-DAG für die Pipeline
- [ ] Tests + CI/CD (GitHub Actions)
- [ ] GCP-Integration (GCS, BigQuery)
- [ ] Dashboard (Power BI/Streamlit)

---

## Notizen (temporär, zum Entfernen)

```
mkdir -p ./logs ./plugins ./config
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.4/docker-compose.yaml'
```
- genau richtig. mkdir -p ./logs ./plugins ./config erstellt drei Ordner im aktuellen Verzeichnis:

- logs/
- plugins/
- config/

- curl — lädt Inhalte von einer URL herunter
- L — folgt Weiterleitungen (falls die URL umgeleitet wird, z.B. auf eine andere Version)
- f — "fail silently": wenn der Server einen Fehler zurückgibt (z.B. 404, falsche Version), bricht curl ab statt eine Fehlerseite als Datei zu speichern
- O — speichert die heruntergeladene Datei unter ihrem Original-Dateinamen aus der URL (hier: docker-compose.yaml), statt sie z.B. im Terminal auszugeben.

##### neue Env instalieren
##### cd dein-projekt-ordner
##### python -m venv .venv
##### source .venv/bin/activate
##### pip install -r requirements.txt


### API aktivieren for storage in Google Cloud
gcloud services enable storage.googleapis.com --project=lakehouse-retail-pipeline

#Aktivierung verifizieren

gcloud services list --enabled --project=lakehouse-retail-pipeline 2>&1 | grep -i storage

oder

gcloud services list --enabled


#### Ressourcen krieren nach der Aktivierung GCS-Bucket

gcloud storage buckets create gs://lakehouse-retail-pipeline-raw-hh \
  --location=europe-west3 \
  --default-storage-class=STANDARD

### Verifizieren
gcloud storage buckets list 

#### CSVs hochladen in gcloud storage bucket
gcloud storage cp Data/*.csv gs://lakehouse-retail-pipeline-raw-hh/raw/


#### Verifizieren 
gcloud storage ls gs://lakehouse-retail-pipeline-raw-hh/raw/


### BigQuery API aktivieren
gcloud services enable bigquery.googleapis.com --project=lakehouse-retail-pipeline

### Ein Dataset anlegen
bq mk --dataset --location=europe-west3 lakehouse-retail-pipeline:retail_lakehouse

In BigQuery ist ein Dataset die oberste Organisationsebene innerhalb eines Projekts — vergleichbar mit einem Schema in klassischem SQL oder einer Datenbank in Databricks/Unity Catalog. Tabellen liegen immer innerhalb eines Datasets, nie direkt im Projekt.

bq mk --dataset --location=europe-west3 lakehouse-retail-pipeline:retail_lakehouse

#### Tabellen hochladen in BigQuery
bq load \
  --source_format=CSV \
  --autodetect \
  --skip_leading_rows=1 \
  lakehouse-retail-pipeline:retail_lakehouse.campaigns \
  gs://lakehouse-retail-pipeline-raw-hh/raw/dataset_fashion_store_campaigns.csv

##### Danach zur Kontrolle:

bq query --use_legacy_sql=false 'SELECT * FROM `lakehouse-retail-pipeline.retail_lakehouse.campaigns`'
### Hier liegen alle wichtige Info 
ls -la ~/.gcp-keys/

