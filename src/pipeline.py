

from ingestion.ingest import ingest
from transform.silver import quality, clean_and_save, merge

class Pipeline:
    def __init__(self):
        self.dataframes = {}

    def run(self):
        self.dataframes = ingest()
        
    def quality_check(self):
        # Implement quality checks on the ingested dataframes
        quality(self.dataframes)

    def clean_and_save(self):
        clean_and_save(self.dataframes)

    def merge_dataframes(self):
        self.transaktion_data = merge(
            sales=self.dataframes["sales"],
            customers=self.dataframes["customers"],
            sales_items=self.dataframes["salesitems"],
            products=self.dataframes["products"],
            channels=self.dataframes["channels"],
            stock=self.dataframes["stock"])
        return self.transaktion_data



if __name__ == "__main__":
    
    pipeline = Pipeline()
    pipeline.run()
    pipeline.quality_check()
    pipeline.clean_and_save()
    df = pipeline.merge_dataframes()
    
