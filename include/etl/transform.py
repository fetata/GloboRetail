import pandas as pd

from include.validations.products_schema import validate_input_product_schema, validate_output_product_schema
from include.validations.sales_schema import validate_input_sales_schema, validate_output_sales_schema



def transform_sales_data(sales_df: pd.DataFrame) -> pd.DataFrame:
    sales_df = validate_input_sales_schema(sales_df)
    sales_df.columns = sales_df.columns.str.strip().str.lower().str.replace(" ", "_")
    sales_df = sales_df.rename(columns={"time_stamp": "timestamp", "qty": "quantity"})
    sales_df["region"] = sales_df["region"].astype(str).str.strip().str.lower()
    sales_df["timestamp"] = pd.to_datetime(sales_df["timestamp"], format="mixed", errors="coerce")
    sales_df = sales_df.dropna(subset=["region", "timestamp"])
    sales_df = sales_df[(sales_df["price"] > 0) & (sales_df["quantity"] > 0)]
    sales_df["order_status"] = sales_df["order_status"].astype(str).str.strip().str.capitalize()
    sales_df = sales_df[sales_df["order_status"].isin(["Completed", "Shipped", "Pending"])]
    sales_df["total_sales"] = sales_df["price"] * sales_df["quantity"]
    return validate_output_sales_schema(sales_df)





def transform_products_data(products_df: pd.DataFrame) -> pd.DataFrame:
    products_df = validate_input_product_schema(products_df)
    products_df.columns = products_df.columns.str.strip().str.lower().str.replace(" ", "_")
    products_df["brand"] = products_df["brand"].astype(str).str.strip().str.upper()
    products_df["category"] = products_df["category"].astype(str).str.strip().str.lower()
    products_df["launch_date"] = pd.to_datetime(products_df["launch_date"], format="mixed", errors="coerce")
    products_df = products_df.dropna(subset=["product_id", "rating", "launch_date"]).drop_duplicates()
    return validate_output_product_schema(products_df)

