from google.cloud import bigquery


class GCPBigQuery:
    def __init__(self, project_id: str, dataset : str):
        self.project_id = project_id
        self.dataset = dataset
        self.client = bigquery.Client(project=project_id)
        
        
    
    def load_to_bigquery(self, gcs_uri: str, table_name: str) -> None:
        """Lädt eine Datei aus GCS in eine BigQuery-Tabelle."""
        table_id = f"{self.client.project}.{self.dataset}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True,
        )
        job = self.client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        job.result()  # wartet, bis der Load-Job fertig ist
        
        
        