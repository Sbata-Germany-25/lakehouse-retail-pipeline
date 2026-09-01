import pandas as pd
import pytest

from transform.silver import quality, merge


def test_quality_raises_on_nulls():
    dataframes = {
        "customers": pd.DataFrame({"customer_id": [1, 2, None]})
    }
    with pytest.raises(ValueError):
        quality(dataframes)


def test_quality_passes_without_nulls():
    dataframes = {
        "customers": pd.DataFrame({"customer_id": [1, 2, 3]})
    }
    quality(dataframes)  # sollte nicht raisen


def test_clean_and_save_converts_discount_percent(tmp_path, monkeypatch):
    import transform.silver as silver
    monkeypatch.setattr(silver, "SILVER_DIR", tmp_path)

    dataframes = {
        "salesitems": pd.DataFrame({"discount_percent": ["10.00%", "0.00%"]})
    }
    silver.clean_and_save(dataframes)

    assert dataframes["salesitems"]["discount_percent"].tolist() == [10.0, 0.0]
    
def test_merge_produces_item_level_rows_matching_sales_totals():
    sales = pd.DataFrame({
        "sale_id": [1],
        "customer_id": [100],
        "channel": ["online"],
        "country": ["DE"],
        "sale_date": ["2026-01-01"],
        "discounted": [False],
        "total_amount": [30.0],
    })
    customers = pd.DataFrame({
        "customer_id": [100],
        "age_range": ["25-34"],
        "signup_date": ["2025-01-01"],
    })
    sales_items = pd.DataFrame({
        "item_id": [1, 2],
        "sale_id": [1, 1],
        "product_id": [10, 20],
        "quantity": [1, 1],
        "original_price": [20.0, 15.0],
        "unit_price": [20.0, 10.0],
        "discount_applied": [False, True],
        "discount_percent": [0.0, 33.3],
        "item_total": [20.0, 10.0],
        "channel_campaigns": [None, None],
    })
    products = pd.DataFrame({
        "product_id": [10, 20],
        "product_name": ["Shirt", "Jeans"],
        "category": ["Tops", "Bottoms"],
        "brand": ["BrandA", "BrandB"],
        "color": ["Blue", "Black"],
        "size": ["M", "L"],
    })
    channels = pd.DataFrame({
        "channel": ["online"],
        "description": ["Online Shop"],
    })
    stock = pd.DataFrame({
        "product_id": [10],
        "country": ["DE"],
        "stock_quantity": [50],
    })

    result = merge(sales, customers, sales_items, products, channels, stock)

    # 1 sale mit 2 Items -> 2 Zeilen auf Item-Ebene
    assert len(result) == 2

    # Integritätscheck: Summe item_total je sale_id == sales.total_amount
    check = result.groupby("sale_id")["item_total"].sum().round(2)
    expected = sales.set_index("sale_id")["total_amount"].round(2)
    assert (check == expected).all()

    # has_stock_data: True für product_id 10 (in stock), False für 20 (fehlt in stock)
    assert result.loc[result["product_id"] == 10, "has_stock_data"].iloc[0] == True
    assert result.loc[result["product_id"] == 20, "has_stock_data"].iloc[0] == False

