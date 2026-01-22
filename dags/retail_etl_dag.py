import pandas as pd

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.utils import yaml
from pendulum import datetime

from include.etl.extract_s3 import extract_data_from_s3, get_storage_options
from include.etl.transform import (transform_sales_data, transform_products_data)

from include.etl.load_s3_csv import load_df_to_s3_csv

with open("include/config.yaml") as file:
    config = yaml.safe_load(file)


@dag(
    start_date=datetime(2021, 12, 1),
    schedule=None,
    catchup=False,
    tags=["retail", "etl"],
)
def retail_etl_dag():
    s3_hook, storage_options = get_storage_options(config["aws_conn_id"])

    @task_group(group_id="extract")
    def extract_group():

        @task()
        def extract_sales():
            paths = extract_data_from_s3(
                bucket=config["s3"]["bucket"],
                folder=config["s3"]["folder"],
                aws_conn_id=config["aws_conn_id"],
                file_type="csv",
            )
            for p in paths:
                if "sales" in p.lower():
                    return p
            raise AirflowException("Sales file not found in S3 RAW zone")

        @task()
        def extract_products():
            paths = extract_data_from_s3(
                bucket=config["s3"]["bucket"],
                folder=config["s3"]["folder"],
                aws_conn_id=config["aws_conn_id"],
                file_type="json",
            )
            for p in paths:
                if "product" in p.lower():
                    return p
            raise AirflowException("Products file not found in S3 RAW zone")

        return {
            "sales_path": extract_sales(),
            "products_path": extract_products(),
        }

    @task_group(group_id="transform")
    def transform_group(sales_path: str, products_path: str):

        @task()
        def transform_sales(path: str):
            df = pd.read_csv(path, storage_options=storage_options)
            df = transform_sales_data(df)
            sales_output = f"s3://{config['s3']['bucket']}/{config['s3']['output_folder']}/sales_clean.csv"
            load_df_to_s3_csv(df, sales_output, config["aws_conn_id"])
            return sales_output

        @task()
        def transform_products(path: str):
            df = pd.read_json(path, storage_options=storage_options)
            df = transform_products_data(df)
            products_output = f"s3://{config['s3']['bucket']}/{config['s3']['output_folder']}/products_clean.csv"
            load_df_to_s3_csv(df, products_output, config["aws_conn_id"])
            return products_output

        transform_sales(sales_path)
        transform_products(products_path)

    extracted = extract_group()
    transform_group(extracted["sales_path"], extracted["products_path"])


retail_etl_dag()
