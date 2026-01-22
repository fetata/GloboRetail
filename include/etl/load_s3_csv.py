import pandas as pd

from include.etl.extract_s3 import get_storage_options
from ..logger import setup_logger

logging = setup_logger("etl.load.s3")


def load_df_to_s3_csv(df: pd.DataFrame, s3_path: str, aws_conn_id: str) -> None:
    s3_hook, storage_options = get_storage_options(aws_conn_id)

    logging.info(
        "Starting upload to S3",extra={"s3_path": s3_path,"rows": len(df), "columns": list(df.columns), } )

    try:
        df.to_csv(s3_path, index=False, storage_options=storage_options)
    except Exception as e:
        logging.error("Failed to upload CSV to S3",extra={"s3_path": s3_path},exc_info=True )
        raise

    logging.info(
        "Successfully uploaded CSV to S3",
        extra={"s3_path": s3_path}
    )
