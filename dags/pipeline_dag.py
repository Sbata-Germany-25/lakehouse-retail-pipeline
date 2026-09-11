from datetime import datetime
from airflow.decorators import dag, task
from ingestion.ingest import ingest , load_bronze
from transform.silver import load_silver, quality, clean_and_save
from pipeline import Pipeline, merge



@dag(
    dag_id="retail_lakehouse_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["retail", "lakehouse"],
)
def retail_lakehouse_pipeline():

    @task
    def ingest_task():
        ingest()

    @task
    def quality_task():
        dataframes = load_bronze()
        quality(dataframes)

    @task
    def clean_and_save_task():
        dataframes = load_bronze()
        clean_and_save(dataframes)

    @task
    def merge_and_save_task():
        dataframes = load_silver()
        transaktion_data = merge(
            sales=dataframes["sales"],
            customers=dataframes["customers"],
            sales_items=dataframes["salesitems"],
            products=dataframes["products"],
            channels=dataframes["channels"],
            stock=dataframes["stock"])
        Pipeline().save_transaktion_data(transaktion_data)
        
    @task
    def gcp_sync_task():
        from gcp.storage import GCPStorage
        from gcp.bigquery import GCPBigQuery
        from pathlib import Path

        gold_path = Path("/opt/airflow/project/data_lake/gold/transaktion_data.parquet")
        storage = GCPStorage("lakehouse-retail-pipeline-raw-hh")
        storage.upload_to_gcs(gold_path, "gold/transaktion_data.parquet")

        bq = GCPBigQuery("lakehouse-retail-pipeline", "retail_lakehouse")
        bq.load_to_bigquery(
            "gs://lakehouse-retail-pipeline-raw-hh/gold/transaktion_data.parquet",
            "transaktion_data"
        )


        

    ingest_task() >> quality_task() >> clean_and_save_task() >> merge_and_save_task() >> gcp_sync_task()


retail_lakehouse_pipeline()


