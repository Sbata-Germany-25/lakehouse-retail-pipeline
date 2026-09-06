from pathlib import Path
import pandas as pd



RAW_DIR = Path(__file__).resolve().parents[2] / "Data"
BRONZE_DIR = Path(__file__).resolve().parents[2] / "data_lake" / "bronze"

TABLES = ["campaigns", "channels", "customers", "products", "sales", "salesitems", "stock"]


def load_csv(name: str, parse_dates: list[str] | None = None) -> pd.DataFrame:
    """Lädt eine der 7 Fashion-Store-CSVs aus Data/."""
    return pd.read_csv(RAW_DIR / f"dataset_fashion_store_{name}.csv", parse_dates=parse_dates)




def save_bronze(df: pd.DataFrame, name: str) -> None:
    """Speichert ein DataFrame als Parquet-Datei in data_lake/bronze/."""
    df.to_parquet(BRONZE_DIR / f"{name}.parquet", index=False)
    
    
    
    
    
def load_bronze()  -> dict[str, pd.DataFrame]:
    """Lädt alle 7 Rohtabellen aus data_lake/bronze/."""
    return {
        name: pd.read_parquet(BRONZE_DIR / f"{name}.parquet")
        for name in TABLES
    }
    
def ingest() -> dict[str, pd.DataFrame]:
    """Lädt alle 7 Rohtabellen und schreibt sie unverändert nach data_lake/bronze/."""
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    dataframes = {
        "campaigns": load_csv("campaigns", parse_dates=["start_date", "end_date"]),
        "channels": load_csv("channels"),
        "customers": load_csv("customers", parse_dates=["signup_date"]),
        "products": load_csv("products"),
        "sales": load_csv("sales", parse_dates=["sale_date"]),
        "salesitems": load_csv("salesitems", parse_dates=["sale_date"]),
        "stock": load_csv("stock"),
    }

    for name, df in dataframes.items():
        save_bronze(df, name)

    return dataframes


if __name__ == "__main__":
    ingest()

