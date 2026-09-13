# Lakehouse Retail Pipeline

End-to-End-Datenpipeline für Fashion-Retail Sales & Inventory Analytics — Portfolio-Projekt zum Aufbau von GCP- und Airflow-Kenntnissen neben bestehender Databricks/PySpark/Power-BI-Erfahrung.

## Architektur

Bronze/Silver/Gold-Lakehouse-Muster:

- **Bronze** (`data_lake/bronze/`) — Rohdaten aus `Data/` (7 CSVs: campaigns, channels, customers, products, sales, salesitems, stock), unverändert als Parquet
- **Silver** (`data_lake/silver/`) — bereinigte Einzeltabellen (z.B. `discount_percent` als float statt String)
- **Gold** (`data_lake/gold/`) — `transaktion_data.parquet`, denormalisierte Tabelle auf Item-Ebene für Reporting

Orchestriert über Apache Airflow (lokal, Docker, `LocalExecutor`) — inkl. eines Tasks, der Gold-Daten automatisch nach GCS + BigQuery synct. Details siehe [docs/architecture.md](docs/architecture.md).

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
├── gcp/          # GCS-Upload + BigQuery-Load (Service Account)
└── governance/   # geplant: Unity-Catalog-artige Governance
dags/             # Airflow-DAGs
notebooks/        # explorative Analyse (EDA)
tests/            # Pytest für Transform-Logik
dashboards/       # Superset-Dashboard-Export (Dashboard, Charts, Dataset-Metadaten)
```

Das Dashboard ist mit Apache Superset gebaut (läuft lokal in Docker, separat vom Pipeline-Code) und nutzt die Gold-Daten aus `data_lake/gold/transaktion_data.parquet` über MySQL. Der Export (Dashboard-Layout, Charts, Dataset-Referenz) liegt in [`dashboards/superset_export/`](dashboards/superset_export/), siehe [dashboards/README.md](dashboards/README.md) zum Re-Import.

## Status

- [x] Bronze/Silver/Gold-Pipeline (lokal, pandas)
- [x] Airflow lokal via Docker (LocalExecutor)
- [x] Airflow-DAG für die Pipeline (5 Tasks, inkl. GCP-Sync)
- [x] Tests + CI/CD (GitHub Actions)
- [x] GCP-Integration (GCS, BigQuery) — in die DAG eingebunden
- [x] Dashboard (Apache Superset)
