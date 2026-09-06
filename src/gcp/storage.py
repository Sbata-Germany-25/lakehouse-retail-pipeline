from pathlib import Path
from google.cloud import storage
import pandas as pd




class GCPStorage:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)



    def upload_to_gcs(self,local_path: Path,  blob_name: str) -> None:
        """Lädt eine lokale Datei nach GCS hoch."""
        #blob" ist GCS' Begriff für eine einzelne Datei/Objekt;
        blob = self.bucket.blob(blob_name)
        blob.upload_from_filename(str(local_path))
        
