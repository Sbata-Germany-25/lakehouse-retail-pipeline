# Architektur — Lakehouse Retail Pipeline

Dieses Dokument beschreibt, welche Tools im Projekt zusammenspielen, welche Rolle jedes davon hat, und wie sie miteinander kommunizieren. Stand: lokales Setup mit Airflow in Docker (GCP-Teil ist geplant, siehe Abschnitt 4).

## 1. Überblick

```mermaid
flowchart TD
    CSV["Data/*.csv<br/>7 Rohdateien"] -->|"1 · ingest()"| BRONZE[("Bronze<br/>data_lake/bronze/*.parquet")]

    BRONZE -->|"2 · load_bronze() + quality()"| QCHECK{"Nullwerte?"}
    QCHECK -->|nein| BRONZE2["Bronze (unverändert)"]

    BRONZE2 -->|"3 · load_bronze() + clean_and_save()"| SILVER[("Silver<br/>data_lake/silver/*.parquet")]

    SILVER -->|"4 · load_silver() + merge()"| GOLD[("Gold<br/>data_lake/gold/transaktion_data.parquet")]

    subgraph DOCKER["Docker-Container (eigenes Image: apache/airflow + pandas/pyarrow)"]
        SCHED["Scheduler<br/>(führt Tasks aus, LocalExecutor)"]
        WEB["Webserver<br/>(UI, Port 8080)"]
        TRIG["Triggerer"]
    end

    SCHED -->|"liest & startet Tasks aus"| DAGFILE["dags/pipeline_dag.py"]
    DAGFILE -.->|orchestriert Reihenfolge| CSV
    DAGFILE -.-> BRONZE
    DAGFILE -.-> SILVER
    DAGFILE -.-> GOLD

    SCHED <-->|"Task-Status, Run-Historie"| METADB[("Postgres<br/>NUR Airflow-Metadaten,<br/>keine Fachdaten!")]
    WEB <-->|"zeigt Status an"| METADB
    TRIG <--> METADB

    DEV["Entwickler (du)"] -->|"git push"| GITHUB[("GitHub Repo")]
    GITHUB -->|"Push / Pull Request"| ACTIONS["GitHub Actions (CI)"]
    ACTIONS -->|"pytest gegen"| SRC["src/ingestion/, src/transform/"]
```

## 2. Rollen der Tools

| Tool / Komponente | Rolle im Projekt | Kommuniziert mit |
|---|---|---|
| **`Data/*.csv`** | Rohdaten-Quelle, statisch, synthetischer Fashion-Retail-Datensatz | wird von `ingest()` gelesen |
| **pandas** (in `src/ingestion/`, `src/transform/`) | Führt die eigentliche Transformationslogik aus (Lesen, Bereinigen, Mergen, Schreiben) | liest/schreibt Parquet-Dateien in `data_lake/` |
| **PyArrow** | Bibliothek, die pandas zum Lesen/Schreiben von Parquet nutzt | wird von pandas intern aufgerufen |
| **`data_lake/{bronze,silver,gold}/`** | Der eigentliche "Lakehouse"-Speicher — reine Parquet-Dateien auf der Festplatte, kein Datenbank-Server | wird von Airflow-Tasks gelesen/geschrieben (per Docker-Volume in den Container gemountet) |
| **Apache Airflow (Scheduler)** | Orchestriert die Reihenfolge der 4 Tasks (`ingest → quality → clean_and_save → merge_and_save`), startet sie, überwacht Erfolg/Fehler | liest `dags/pipeline_dag.py`, schreibt Status in Postgres |
| **Apache Airflow (Webserver)** | Web-UI zum Anschauen/manuellen Triggern von DAG-Runs | liest Status aus Postgres |
| **Apache Airflow (Triggerer)** | Verwaltet asynchrone/"deferred" Tasks (bei dieser einfachen DAG aktuell nicht aktiv genutzt, aber Teil des Standard-Setups) | Postgres |
| **Postgres** | **Nur** Airflows eigene Betriebsdatenbank — speichert DAG-Run-Historie, Task-Status, Verbindungen. **Enthält keine Fachdaten** (keine Sales/Customers etc.) — das ist ein häufiger Verwechslungspunkt! | Scheduler, Webserver, Triggerer |
| **Docker / docker-compose** | Verpackt Airflow + Postgres als isolierte, reproduzierbare Container; mountet `dags/`, `src/`, `Data/`, `data_lake/` in die Container hinein | Host-Dateisystem (via Volumes) |
| **Eigenes Dockerfile** | Erweitert das Standard-Airflow-Image um `pandas`/`pyarrow`, damit die Tasks im Container die Transform-Logik überhaupt ausführen können | Basis für alle 4 Airflow-Container (webserver, scheduler, triggerer, init) |
| **`.venv`** | Lokale Python-Umgebung für Entwicklung/Notebook/manuelles Testen **außerhalb** von Docker (z.B. `python pipeline.py`, `pytest`) | unabhängig von den Docker-Containern — bewusst getrennt, um Versionskonflikte zu vermeiden |
| **Pytest** | Prüft die Transform-Logik (`quality`, `clean_and_save`, `merge`) automatisiert, ohne echte Daten/Airflow zu brauchen | läuft gegen `src/`, lokal und in GitHub Actions |
| **GitHub Repo** | Zentrale Quelle der Wahrheit für den Code, Historie via Commits/Branches | Entwickler pusht hierhin, GitHub Actions reagiert darauf |
| **GitHub Actions (CI)** | Führt bei jedem Push/PR automatisch `pytest` in einer frischen Cloud-VM aus — verhindert, dass kaputter Code nach `main` gelangt | checkt Code aus GitHub aus, installiert `requirements.txt`, ruft `pytest` auf |

## 3. Der Datenfluss im Detail (die 4 Airflow-Tasks)

1. **`ingest_task`** — liest die 7 CSVs aus `Data/`, schreibt sie unverändert als Parquet nach `data_lake/bronze/`
2. **`quality_task`** — liest Bronze erneut ein (`load_bronze()`), prüft auf Nullwerte, bricht bei Problemen mit `ValueError` ab
3. **`clean_and_save_task`** — liest Bronze erneut ein, bereinigt `discount_percent` (String → Float), schreibt nach `data_lake/silver/`
4. **`merge_and_save_task`** — liest Silver ein (`load_silver()`), führt alle Tabellen zu `transaktion_data` zusammen (Item-Ebene), speichert als Gold in `data_lake/gold/`

**Wichtig zum Verständnis:** Jede Task liest ihre Eingabedaten selbst neu von der Festplatte — es werden **keine DataFrames zwischen Tasks über Airflow selbst transportiert** (kein XCom für große Daten). Das ist bewusst so gebaut, weil Airflow-Tasks isolierte Prozesse sind und XCom nur für kleine Metadaten gedacht ist, nicht für komplette pandas-DataFrames.

## 4. Geplante Erweiterung: GCP (noch nicht gebaut)

```mermaid
flowchart LR
    LOCAL["Lokales Docker-Airflow<br/>(aktueller Stand)"] -.->|"perspektivisch ersetzt durch"| COMPOSER["Cloud Composer<br/>(gemanagtes Airflow)"]

    COMPOSER -->|"liest DAGs aus"| GCS_DAGS[("GCS-Bucket<br/>gs://.../dags/")]
    GITHUB2[("GitHub Actions")] -.->|"CD: gcloud composer ... import"| GCS_DAGS

    GOLD2["lokales Gold-Parquet"] -.->|"perspektivisch zusätzlich"| BQ[("BigQuery<br/>Data Warehouse")]
    BQ -.-> DASH["Power BI / Streamlit<br/>Dashboard"]
```

- **GCS (Cloud Storage)** — würde Rohdaten/Bronze aufnehmen, parallel oder statt der lokalen `data_lake/`
- **BigQuery** — alternatives/zusätzliches Ziel für Silver/Gold, SQL-basiertes Data Warehouse statt lokaler Parquet-Dateien
- **Cloud Composer** — GCP-gehostetes Airflow; würde das lokale Docker-Setup für eine "echte" Cloud-Umgebung ablösen
- **CD via GitHub Actions** — nach erfolgreichem CI-Lauf automatisch die DAG-Datei in den Composer-GCS-Bucket kopieren (`gcloud composer environments storage dags import`)

Dieser Teil ist bewusst noch nicht umgesetzt — Cloud Composer verursacht laufende Kosten (siehe Kostenabschnitt aus dem Chat), daher wird das eher kurz für eine Demo aufgesetzt als dauerhaft betrieben.
