from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pyathena import connect
from typing import Union
import pandas as pd
import base64
import boto3
import redis

app = FastAPI()
r = redis.Redis(host='localhost', port=6379, db=0)
athena_client = boto3.client('athena')
s3_client = boto3.client('s3')

AWS_DATA_CATALOG = "AwsDataCatalog"
AWS_SCHEMA_DATABASE_NAME = "ensembl-parquet-meta-schema"
AWS_S3_OUTPUT_DIR = "s3://ensembl-athena-results/"

@app.get("/")
def root():
    return "Ensembl's data lakehouse backend"


@app.get("/data")
async def read_all_filters():
    query_response = athena_client.list_table_metadata(CatalogName=AWS_DATA_CATALOG, DatabaseName=AWS_SCHEMA_DATABASE_NAME)["TableMetadataList"]
    # remove unnecessary data from AWS response
    for table in query_response:
        del table["CreateTime"]
        del table["LastAccessTime"]
        del table["TableType"]
        del table["Parameters"]
    return query_response


@app.get("/{data_type}/columns")
async def read_filters(data_type: str):
    try:
        conn = connect(s3_staging_dir=AWS_S3_OUTPUT_DIR, schema_name=AWS_SCHEMA_DATABASE_NAME)
        species = pd.read_sql_query(f"SELECT DISTINCT species from {data_type}", conn)["species"].tolist()
        return {'columns': athena_client.get_table_metadata(CatalogName=AWS_DATA_CATALOG, DatabaseName=AWS_SCHEMA_DATABASE_NAME, TableName=data_type)["TableMetadata"]["Columns"],
                'species': species}
    except Exception as err:
        print(err)
        if "does not exist" in str(err):
            raise HTTPException(status_code=404, detail="Data type not found!")
        raise HTTPException(status_code=500, detail=str(err))


@app.get("/query/{queryId}/status")
async def query_status(queryId: str):
    try:
        query_response = athena_client.get_query_results(
            QueryExecutionId=queryId
        )
        # remove unnecessary data from AWS response
        del query_response['ResultSet']['ResultSetMetadata']
        # fetch temporary presigned S3 result object URL (expires in 1hr)
        result_file_temp_presigned_url = s3_client.generate_presigned_url('get_object', Params={'Bucket': 'ensembl-athena-results', 'Key': f'{queryId}.csv'}, ExpiresIn=3600)
        return {'status': 'DONE', 'result': result_file_temp_presigned_url, 'preview': query_response['ResultSet']}
    except Exception as err:
        print(err)
        if "not yet finished" in str(err):
            return {'status': "IN PROGRESS"}
        elif "was not found" in str(err):
            raise HTTPException(status_code=404, detail="Invalid queryID / queryID does not exist!")
        elif "Query did not finish successfully. Final query state: FAILED" in str(err):
            return JSONResponse(
                status_code=400,
                content={"status": "FAILED", "detail": "Query was invalid, it could not be processed."},
            )
        raise HTTPException(status_code=500, detail=str(err))


@app.get("/query/{data_type}/{species}")
async def query_data(data_type: str , species: str, q: Union[str, None] = None):
    cache_key = base64.b64encode(bytes(data_type + species + q, 'utf-8')) if q else base64.b64encode(bytes(data_type + species, 'utf-8'))
    if(r.exists(cache_key)):
        query_id = r.get(cache_key)
    else:
        filters = "AND " + q.replace("&", " AND ") if q else ""
        query_id = athena_client.start_query_execution(
            QueryString=f"SELECT * FROM {data_type} WHERE species='{species}' {filters}",
            QueryExecutionContext={"Database": AWS_SCHEMA_DATABASE_NAME},
            ResultConfiguration={
                "OutputLocation": AWS_S3_OUTPUT_DIR,
                "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
            },
        )["QueryExecutionId"]
        r.set(cache_key, query_id)
    return {'query_id': query_id}
