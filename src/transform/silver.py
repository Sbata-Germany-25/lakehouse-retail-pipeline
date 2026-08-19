
from pathlib import Path
import pandas as pd


SILVER_DIR = Path(__file__).resolve().parents[2] / "data_lake" / "silver"



def quality(dict_of_dfs: dict[str, pd.DataFrame]) -> None:
    """Prüft jeden DataFrame auf Nullwerte und bricht ab, sobald einer welche enthält."""
    for name, df in dict_of_dfs.items():
        nulls = df.isnull().sum()
        nulls = nulls[nulls > 0]
        if not nulls.empty:
            for col, count in nulls.items():
                raise ValueError(f"{name} enthält {count} Nullwerte in Spalte '{col}'.")
            
            
def clean_and_save(dict_of_dfs: dict[str, pd.DataFrame]) -> None:
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    for name, df in dict_of_dfs.items():
        if name == "salesitems":
            df['discount_percent'] = df['discount_percent'].str.rstrip('%').astype(float)
        df.to_parquet(SILVER_DIR / f"{name}.parquet", index=False)

def merge(
    sales: pd.DataFrame,
    customers: pd.DataFrame,
    sales_items: pd.DataFrame,
    products: pd.DataFrame,
    channels: pd.DataFrame,
    stock: pd.DataFrame,
) -> pd.DataFrame:
    """Führt sales, customers, salesitems, products, channels und stock zu transaktion_data auf Item-Ebene zusammen."""
    transaktion_data = sales\
        .merge(customers[['customer_id', 'age_range', 'signup_date']], on="customer_id", how="left")\
        .merge(sales_items[['item_id', 'sale_id', 'product_id', 'quantity', 'original_price',
                             'unit_price', 'discount_applied', 'discount_percent', 'item_total',
                             'channel_campaigns']], on="sale_id", how="left")\
        .merge(products[['product_id', 'product_name', 'category', 'brand', 'color', 'size']], on="product_id", how="left")\
        .merge(channels, on="channel", how="left")\
        .merge(stock, on=["product_id", "country"], how="left")

    transaktion_data['has_stock_data'] = transaktion_data['stock_quantity'].notna()
    
    return  transaktion_data[['customer_id', 'product_id',  'product_name', 'item_total',
        'quantity', 'original_price', 'unit_price','sale_id', 'channel', 'discounted', 
        'brand','size', 'description', 'stock_quantity', 'has_stock_data',
        'country', 'signup_date', 'sale_date','category', 'item_id',
        'discount_applied', 'discount_percent', 'channel_campaigns',  'color', 'age_range']]

        
        
