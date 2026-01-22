import pandas as pd
import pandera as pa

from pandera import Column, Check
from pandera.errors import SchemaError
from ..logger import setup_logger

logging = setup_logger("etl.validations.products")

product_input_schema = pa.DataFrameSchema({
    "product_id": Column(int),
    "category": Column(str),
    "brand": Column(str),
    "rating": Column(float),
    "in_stock": Column(bool),
    "launch_date": Column(str)
})

product_output_schema = pa.DataFrameSchema({
    "product_id": Column(int),
    "category": Column(str, Check(lambda s: s.dropna().str.islower())),
    "brand": Column(str, Check(lambda s: s.dropna().str.isupper())),
    "rating": Column(float, Check.greater_than_or_equal_to(0)),
    "in_stock": Column(bool, nullable=False),
    "launch_date": Column(pa.DateTime)
})


def validate_input_product_schema(product_df: pd.DataFrame) -> pd.DataFrame:
    try:
        return product_input_schema.validate(product_df)
    except SchemaError as e:
        logging.warning("Pre-product schema validation failed",extra={"failure_cases": e.failure_cases})
        return product_df


def validate_output_product_schema(product_df: pd.DataFrame) -> pd.DataFrame:
    return product_output_schema.validate(product_df)
