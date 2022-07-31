from celery import Celery
from celery.utils.log import get_task_logger
from time import sleep
import pandas as pd

from app.constants import *
from app.redis_setup import *

logger = get_task_logger(__name__)

app = Celery('tasks', backend=f'redis://{redis_host}:{redis_port}/0', broker=f'redis://{redis_host}:{redis_port}/0')

@app.task
def file_format_converter(queryID: str, df_input: str, file_format: SupportedFileFormats, cache_key: str, id: str):
    try:
        logger.info(f"Celery task id:{id} status:PROCESSING func:file_format_converter params:{{file_format:{file_format}}}")
        r.set(cache_key, "PROCESSING")

        df = pd.read_csv(df_input, low_memory=False)
        s3_key = AWS_S3_OUTPUT_DIR + f"{queryID}.{file_format}"
        if (file_format == "tsv"): df.to_csv(s3_key, sep="\t", index=False)
        elif (file_format == "xlsx"): df.to_excel(s3_key, index=False)
        elif (file_format == "json"): df.to_json(s3_key, index=False, orient="split")
        elif (file_format == "xml"): df.to_xml(s3_key, index=False)
        elif (file_format == "feather"): df.to_feather(s3_key)
        elif (file_format == "parquet"): df.to_parquet(s3_key, index=False)

        logger.info(f"Celery task id:{id} status:DONE func:file_format_converter params:{{file_format:{file_format}}}")
        r.set(cache_key, "DONE")
    except Exception as err:
        logger.info(f"Celery task id:{id} status:FAILED func:file_format_converter params:{{file_format:{file_format}}} error_detail:{str(err)}")
        r.set(cache_key, "FAILED")

@app.task
def delete_key_from_cache(cache_key, wait_time = 0):
    sleep(wait_time)  # in seconds
    r.delete(cache_key)
