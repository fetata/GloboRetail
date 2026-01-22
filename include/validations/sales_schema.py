import pandas as pd
import pandera as pa

from pandera import Column, Check
from pandera.errors import SchemaError
from ..logger import setup_logger

logging = setup_logger("etl.validations.sales")

sales_input_schema = pa.DataFrameSchema({
    "sales_id": Column(int),
    "product_id": Column(int),
    "region": Column(str),
    "quantity": Column(int),
    "price": Column(float),
    "timestamp": Column(pa.DateTime),
    "discount": Column(float),
    "order_status": Column(str),
})

sales_output_schema = pa.DataFrameSchema({
    "sales_id": Column(int),
    "product_id": Column(int),
    "region": Column(str),
    "quantity": Column(int, Check.greater_than(0)),
    "price": Column(float, Check.greater_than(0)),
    "timestamp": Column(pa.DateTime),
    "discount": Column(float, Check.in_range(0, 1)),
    "order_status": Column(str, Check.isin(["Completed", "Shipped", "Pending"])),
    "total_sales": Column(float, Check.greater_than(0))
})


def validate_input_sales_schema(sales_df: pd.DataFrame) -> pd.DataFrame:
    try:
        return sales_input_schema.validate(sales_df)
    except SchemaError as e:
        logging.warning("Pre-sales schema validation failed",extra={"failure_cases": e.failure_cases})
        return sales_df


def validate_output_sales_schema(sales_df: pd.DataFrame) -> pd.DataFrame:
    return sales_output_schema.validate(sales_df)
