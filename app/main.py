from contextlib import suppress
from xmlrpc.client import boolean
from fastapi import FastAPI, HTTPException, Request, Response, Query
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from pyathena import connect
from redis import Redis
from time import time
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


logging.basicConfig(filename="log.txt", level=logging.DEBUG, format='%(asctime)s %(levelname)s %(module)s %(name)s %(message)s')
logger = logging.getLogger(__name__)

redis_host = os.getenv("REDIS_HOST", "localhost")
refis_port = os.getenv("REDIS_PORT", 6379)
r = Redis(host=redis_host, port=refis_port, db=0)

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

    if (request.query_params): logger.info(f"{request.state.id} {request.url.path}?{request.query_params} Time={'{0:.2f}'.format(end_time)} ms status_code={response.status_code}")
    else: logger.info(f"{request.state.id} {request.method} {request.url.path} Time={'{0:.2f}'.format(end_time)}ms status_code={response.status_code}")

    response.headers['X-Correlation-ID'] = request.state.id
    return response

def log_error(err: str, request: Request) -> None:
    if (request.query_params): logger.error(f"{request.state.id} {request.url.path}?{request.query_params} err=\"{err}\"")
    else: logger.error(f"{request.state.id} {request.method} {request.url.path} err=\"{err}\"")

def log_cache(bool: boolean, request: Request, key: str = None) -> None:
    if (request.query_params): logger.info(f"{request.state.id} {request.url.path}?{request.query_params} cache={bool} key={key}" if key else f"{request.state.id} {request.url.path}?{request.query_params} cache={bool}")
    else:logger.info(f"{request.state.id} {request.method} {request.url.path} cache={bool} key={key}" if key else f"{request.state.id} {request.method} {request.url.path} cache={bool}")

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
                    "example": {"detail_example_1": "Cannot retrieve result preview (NOTE: Result preview is only available, if query status state is SUCCEEDED).",
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
async def query_result_preview(queryID: str, request: Request):
    queryID = queryID.strip()
    if not queryID: raise HTTPException(status_code=400, detail="Invalid queryID!")
    try:
        query_response = athena_client.get_query_results(
            QueryExecutionId=queryID,
            MaxResults=26
        )
        # remove unnecessary data from AWS response
        del query_response['ResultSet']['ResultSetMetadata']
        return query_response['ResultSet']
    except Exception as err:
        log_error(str(err), request)
        if "InvalidRequestException" in str(err):
            raise HTTPException(status_code=400, detail="Cannot retrieve result preview (NOTE: Result preview is only available, if query status state is SUCCEEDED).") from err
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
        description="Condition to filter the data on, similar to SQL WHERE clause ex.: gene_id=554 AND gene_stable_id='eNSG00000210049'",
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
                'preview': {'href': app.url_path_for('query_result_preview', queryID=query_id)}
            }
        }, media_type="application/hal+json")
    except Exception as err:
        log_error(str(err), request)
        raise HTTPException(status_code=500) from err


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
