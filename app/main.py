from contextlib import suppress
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request, Response
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from pyathena import connect
from redis import Redis
from time import time, sleep
from typing import Optional
from uuid import uuid4
import pandas as pd
import base64
import boto3
import json
import logging
import os
import uvicorn


# constants
AWS_DATA_CATALOG = "AwsDataCatalog"
AWS_SCHEMA_DATABASE_NAME = "ensembl-parquet-meta-schema"
AWS_S3_OUTPUT_DIR = "s3://ensembl-athena-results/"
SUPPORTED_FILE_FORMATS = ["csv", "tsv", "xlsx", "json", "xml", "feather", "parquet"]

logging.basicConfig(filename="log.txt", level=logging.DEBUG, format='%(asctime)s %(levelname)s %(module)s %(name)s %(message)s')
logger = logging.getLogger(__name__)

redis_host = os.getenv("REDIS_HOST", "localhost")
redis_port = os.getenv("REDIS_PORT", 6379)
r = Redis(host=redis_host, port=redis_port, db=0)

app = FastAPI(debug=True)

athena_client = boto3.client('athena')
s3_client = boto3.client('s3')


# log all requests
@app.middleware("http")
async def log_requests(request: Request, call_next) -> Response:
    request.state.id = str(uuid4())

    start_time = time()
    response = await call_next(request)
    end_time = (time() - start_time) * 1000

    request_query_params = f"?{str(request.query_params)}" if request.query_params else ""
    logger.info(f"{request.state.id} {request.url.path}{request_query_params} Time={'{0:.2f}'.format(end_time)} ms status_code={response.status_code}")

    response.headers['X-Correlation-ID'] = request.state.id
    return response

def log_error(err: str, request: Request) -> None:
    request_query_params = f"?{str(request.query_params)}" if request.query_params else ""
    logger.error(f"{request.state.id} {request.url.path}{request_query_params} err=\"{err}\"")

def log_cache(bool: bool, request: Request, key: str = None) -> None:
    request_query_params = f"?{str(request.query_params)}" if request.query_params else ""
    logger.info(f"{request.state.id} {request.url.path}{request_query_params} cache={bool} key={key}" if key else f"{request.state.id} {request.url.path}?{request.query_params} cache={bool}")

# customise OpenAPI schema doc
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Ensembl lakehouse",
        version="0.1.0",
        description="Ensembl's data lakehouse backend",
        routes=app.routes,
    )
    # remove 422 error codes from doc
    for method in openapi_schema["paths"]:
        with suppress(KeyError):
            del openapi_schema["paths"][method]["get"]["responses"]["422"]
            del openapi_schema["paths"][method]["post"]["responses"]["422"]
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi


# background tasks
def file_format_converter(queryID: str, df_input: str, file_format: str, cache_key: str, id: str):
    try:
        logger.info(f"Background task id:{id} status:PROCESSING func:file_format_converter params:{{file_format:{file_format}}}")
        r.set(cache_key, "PROCESSING")

        df = pd.read_csv(df_input, low_memory=False)
        s3_key = AWS_S3_OUTPUT_DIR + f"{queryID}.{file_format}"
        if (file_format == "tsv"): df.to_csv(s3_key, sep="\t", index=False)
        elif (file_format == "xlsx"): df.to_excel(s3_key, index=False)
        elif (file_format == "json"): df.to_json(s3_key, index=False, orient="split")
        elif (file_format == "xml"): df.to_xml(s3_key, index=False)
        elif (file_format == "feather"): df.to_feather(s3_key)
        elif (file_format == "parquet"): df.to_parquet(s3_key, index=False)

        logger.info(f"Background task id:{id} status:DONE func:file_format_converter params:{{file_format:{file_format}}}")
        r.set(cache_key, "DONE")
    except Exception as err:
        logger.info(f"Background task id:{id} status:FAILED func:file_format_converter params:{{file_format:{file_format}}} error_detail:{str(err)}")
        r.set(cache_key, "FAILED")

def delete_key_from_cache_after_one_min(cache_key):
    sleep(60)
    r.delete(cache_key)


@app.get("/",
         responses={
             200: {
                 "content": {
                     "application/json": {
                         "example": {"Ensembl's data lakehouse backend"}
                     }
                 },
             },
         },
         )
def root():
    return "Ensembl's data lakehouse backend"


@app.get("/data",
         responses={
             200: {
                 "content": {
                     "application/json": {
                         "example": [
                             {
                                 "Name": "table_name",
                                 "Columns": [
                                     {
                                         "Name": "column_name",
                                         "Type": "column_data_type"
                                     }
                                 ],
                                 "PartitionKeys": [
                                     {
                                         "Name": "column_name",
                                         "Type": "column_data_type"
                                     }
                                 ]
                             }
                         ]
                     }
                 },
             }
         }
         )
async def read_all_filters(request: Request):
    try:
        if(r.exists('data')):
            query_response = json.loads(r.get('data').decode('ascii'))
            log_cache(True, request, 'data')
        else:
            query_response = athena_client.list_table_metadata(
                CatalogName=AWS_DATA_CATALOG, DatabaseName=AWS_SCHEMA_DATABASE_NAME)["TableMetadataList"]
            # remove unnecessary data from AWS response
            for table in query_response:
                del table["CreateTime"]
                del table["LastAccessTime"]
                del table["TableType"]
                del table["Parameters"]
            r.set('data', json.dumps(query_response))
            log_cache(False, request)
        return query_response
    except Exception as err:
        log_error(str(err), request)
        raise HTTPException(status_code=500) from err


@app.get(
    "/{data_type}/columns",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "columns": [
                            {
                                "Name": "column_name",
                                "Type": "column_data_type"
                            }
                        ],
                        "species": [
                            "species_1",
                            "species_2"
                        ]
                    }
                }
            },
        },
        400: {
            "content": {
                "application/json": {
                    "example": {"detail": "Invalid data type!"}
                }
            },
        },
        404: {
            "content": {
                "application/json": {
                    "example": {"detail": "Data type not found!"}
                }
            },
        },
    }
)
async def read_filters(data_type: str, request: Request):
    data_type = data_type.strip()
    if not data_type: raise HTTPException(status_code=400, detail="Invalid data type!")

    try:
        species_cache_key = f'{data_type}_species'
        if r.exists(species_cache_key):
            species = json.loads(r.get(species_cache_key).decode('ascii'))
            log_cache(True, request, species_cache_key)
        else:
            conn = connect(s3_staging_dir=AWS_S3_OUTPUT_DIR, schema_name=AWS_SCHEMA_DATABASE_NAME)
            species = pd.read_sql_query(f"SELECT DISTINCT species from {data_type}", conn)["species"].tolist()
            r.set(species_cache_key, json.dumps(species))
            log_cache(False, request)

        table_metadata_cache_key = f'{data_type}_table_metadata'
        if r.exists(table_metadata_cache_key):
            table_metadata = json.loads(r.get(table_metadata_cache_key).decode('ascii'))
            log_cache(True, request, table_metadata_cache_key)
        else:
            table_metadata = athena_client.get_table_metadata(CatalogName=AWS_DATA_CATALOG, DatabaseName=AWS_SCHEMA_DATABASE_NAME, TableName=data_type)["TableMetadata"]["Columns"]
            r.set(table_metadata_cache_key, json.dumps(table_metadata))
            log_cache(False, request)

        return {'columns': table_metadata, 'species': species}
    except Exception as err:
        log_error(str(err), request)
        if "does not exist" in str(err): raise HTTPException(status_code=404, detail="Data type not found!") from err
        elif "InvalidRequestException" in str(err): raise HTTPException(status_code=400, detail="Invalid data type!") from err
        raise HTTPException(status_code=500) from err


@app.get(
    "/query/{queryID}/status",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "'QUEUED'|'RUNNING'|'SUCCEEDED'|'FAILED'|'CANCELLED'",
                        "result": "https://example.com/?expiry=1hr"
                    }
                }
            },
        },
        400: {
            "content": {
                "application/json": {
                    "example": {"detail": "Invalid queryID!"}
                }
            },
        },
        404: {
            "content": {
                "application/json": {
                    "example": {"detail": "QueryID does not exist / invalid queryID!"}
                }
            },
        },
    }
)
async def query_status(queryID: str, request: Request):
    queryID = queryID.strip()
    if not queryID: raise HTTPException(status_code=400, detail="Invalid queryID!")
    try:
        # 'State': 'QUEUED'|'RUNNING'|'SUCCEEDED'|'FAILED'|'CANCELLED'
        query_response = athena_client.get_query_execution( QueryExecutionId = queryID )
        if query_response['QueryExecution']['Status']['State'] != 'SUCCEEDED':
            return {'status': query_response['QueryExecution']['Status']['State']}
        # fetch temporary pre-signed S3 result object URL (expiry = 1hr)
        result_file_temp_presigned_url = s3_client.generate_presigned_url('get_object', Params={'Bucket': 'ensembl-athena-results', 'Key': f'{queryID}.csv'}, ExpiresIn=3600)
        return {'status': query_response['QueryExecution']['Status']['State'], 'result': result_file_temp_presigned_url}
    except Exception as err:
        log_error(str(err), request)
        if "was not found" in str(err): raise HTTPException(status_code=404, detail="QueryID does not exist / invalid queryID!") from err
        raise HTTPException(status_code=500) from err


@app.get(
    "/query/{queryID}/export",
    responses={
        202: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "ACCEPTED",
                    }
                }
            },
        },
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "'PROCESSING' | 'DONE' | 'FAILED, you can try again after one minute interval!'",
                        "result": "https://example.com/?expiry=1hr (Available only if status='DONE')"
                    }
                }
            },
        },
        400: {
            "content": {
                "application/json": {
                    "example": {"detail_example_1": "Invalid queryID!",
                                "detail_example_2": "Cannot export (NOTE: Result can only be exported, if query execution status state = SUCCEEDED).",
                                "detail_example_3": f"Invalid result file format, supported file formats: {SUPPORTED_FILE_FORMATS}"}
                }
            },
        },
        404: {
            "content": {
                "application/json": {
                    "example": {"detail": "QueryID does not exist / invalid queryID!"}
                }
            },
        }
    }
)
async def export_query_result(queryID: str, request: Request, background_tasks: BackgroundTasks, file_format: str = "csv"):
    # validate queryID and query execution status state
    queryID = queryID.strip()
    if not queryID: raise HTTPException(status_code=400, detail="Invalid queryID!")
    try:
        query_response = athena_client.get_query_execution( QueryExecutionId = queryID )
        if query_response['QueryExecution']['Status']['State'] != 'SUCCEEDED':
            raise Exception("Cannot export (NOTE: Result can only be exported, if query execution status state = SUCCEEDED).")
    except Exception as err:
        log_error(str(err), request)
        if "Cannot export" in str(err):
            raise HTTPException(status_code=400, detail=str(err)) from err
        elif "was not found" in str(err):
            raise HTTPException(status_code=404, detail="QueryID does not exist / invalid queryID!") from err
        raise HTTPException(status_code=500) from err

    # validate file_format
    if file_format not in SUPPORTED_FILE_FORMATS: raise HTTPException(status_code=400, detail=f"Invalid result file format, supported file formats: {SUPPORTED_FILE_FORMATS}")

    try:
        # validate if file exists in S3
        s3_client.head_object(Bucket='ensembl-athena-results', Key=f'{queryID}.{file_format}')
        result_file_temp_presigned_url = s3_client.generate_presigned_url('get_object', Params={'Bucket': 'ensembl-athena-results', 'Key': f'{queryID}.{file_format}'}, ExpiresIn=3600)
        return {'status': "DONE", 'result': result_file_temp_presigned_url}
    except Exception as err:
        if "An error occurred (404) when calling the HeadObject operation: Not Found" in str(err):
            try:
                cache_key = f"{queryID}.{file_format}"
                if(r.exists(cache_key) and r.get(cache_key).decode('ascii') == "PROCESSING"):
                    return {"status": "PROCESSING"}
                if(r.exists(cache_key) and r.get(cache_key).decode('ascii') == "FAILED"):
                    # delete key after one minute
                    background_tasks.add_task(delete_key_from_cache_after_one_min, cache_key)
                    return {"status": "FAILED, you can try again after one minute interval!"}
                # start a background process with csv result file as input
                df_input = s3_client.generate_presigned_url('get_object', Params={'Bucket': 'ensembl-athena-results', 'Key': f'{queryID}.csv'}, ExpiresIn=3600)
                background_tasks.add_task(file_format_converter, queryID, df_input, file_format, cache_key, request.state.id)
                r.set(cache_key, "QUEUED")
                return JSONResponse(content={"status": "ACCEPTED"}, status_code=202)
            except Exception as e:
                log_error(str(e), request)
        raise HTTPException(status_code=500) from err


@app.get(
    "/query/{queryID}/preview",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "Rows": [
                            {
                                "Data": [
                                    {
                                        "VarCharValue": "column_name"
                                    }
                                ]
                            },
                            {
                                "Data": [
                                    {
                                        "VarCharValue": "data_1"
                                    }
                                ]
                            }
                        ]
                    }
                }
            },
        },
        400: {
            "content": {
                "application/json": {
                    "example": {"detail_example_1": "Cannot retrieve result preview (NOTE: Result preview is only available, if query execution status state = SUCCEEDED).",
                                "detail_example_2": "Invalid queryID!"}
                }
            },
        },
        404: {
            "content": {
                "application/json": {
                    "example": {"detail": "QueryID does not exist / invalid queryID!"}
                }
            },
        }
    }
)
async def query_result_preview(queryID: str, request: Request, maxResults: int = 26):
    queryID = queryID.strip()
    if not queryID: raise HTTPException(status_code=400, detail="Invalid queryID!")
    if maxResults>1000 or maxResults<1: raise HTTPException(status_code=400, detail="Allowed range for maxResults is 1-1000!")
    try:
        query_response = athena_client.get_query_results(
            QueryExecutionId=queryID,
            MaxResults=maxResults
        )
        # remove unnecessary data from AWS response
        del query_response['ResultSet']['ResultSetMetadata']
        return query_response['ResultSet']
    except Exception as err:
        log_error(str(err), request)
        if "InvalidRequestException" in str(err):
            raise HTTPException(status_code=400, detail="Cannot retrieve result preview (NOTE: Result preview is only available, if query execution status state = SUCCEEDED).") from err
        elif "was not found" in str(err):
            raise HTTPException(status_code=404, detail="QueryID does not exist / invalid queryID!") from err
        raise HTTPException(status_code=500) from err


@app.get(
    "/query/{data_type}/{species}",
    response_class=Response,
    responses={
        200: {
            "content": {
                "application/hal+json": {
                    "example": {
                        "queryID": "abc-1234567890-xyz",
                        "_links": {
                            "self": {
                                "href": "/query/gene/homo_sapiens"
                            },
                            "status": {
                                "href": "/query/abc-1234567890-xyz/status"
                            },
                            "preview": {
                                "href": "/query/abc-1234567890-xyz/preview"
                            }
                        }
                    }
                }
            },
        },
        400: {
            "content": {
                "application/json": {
                    "example": {"detail": "Invalid data_type/species!"}
                }
            },
        }
    }
)
async def request_query(data_type: str, species: str, request: Request, fields: Optional[str] = Query(
        default="*",
        description="Comma seperated fields ex.: gene_id,gene_stable_id",
    ), condition: Optional[str] = Query(
        default=None,
        description="Condition to filter the data on, similar to SQL WHERE clause ex.: gene_id=554 AND gene_stable_id='ENSG00000210049'",
    )
):
    data_type = data_type.strip()
    species = species.strip()
    if ((not data_type) or (not species)): raise HTTPException(status_code=400, detail="Invalid data_type/species!")
    try:
        if condition: cache_key = base64.b64encode(bytes(''.join(sorted(data_type + species + fields + condition.replace("AND", "and").replace("BETWEEN", "between").replace("LIKE", "like").replace("IN", "in"))), 'utf-8'))
        else: cache_key = base64.b64encode(bytes(''.join(sorted(data_type + species + fields)) + species, 'utf-8'))
        if(r.exists(cache_key)):
            query_id = r.get(cache_key).decode('ascii')
            log_cache(True, request, cache_key)
        else:
            filters = "AND " + condition if condition else ""
            query_id = athena_client.start_query_execution(
                QueryString = f"SELECT {fields} FROM {data_type} WHERE species='{species}' {filters};",
                QueryExecutionContext = {"Database": AWS_SCHEMA_DATABASE_NAME},
                ResultConfiguration = {
                    "OutputLocation": AWS_S3_OUTPUT_DIR,
                    "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
                },
            )["QueryExecutionId"]
            r.set(cache_key, query_id)
            log_cache(False, request)

        # https://tools.ietf.org/id/draft-kelly-json-hal-01.html
        return JSONResponse(content={
            'queryID': query_id,
            '_links': {
                'self': {'href': str(request.url.path)},
                'status': {'href': app.url_path_for('query_status', queryID=query_id)},
                'preview': {'href': app.url_path_for('query_result_preview', queryID=query_id)},
                'export': {'href': app.url_path_for('export_query_result', queryID=query_id), "supported_file_formats": SUPPORTED_FILE_FORMATS}
            }
        }, media_type="application/hal+json")
    except Exception as err:
        log_error(str(err), request)
        raise HTTPException(status_code=500) from err


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
