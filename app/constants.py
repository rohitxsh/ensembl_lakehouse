from enum import Enum

class SupportedFileFormats(str, Enum):
    csv = "csv"
    tsv = "tsv"
    xlsx = "xlsx"
    json = "json"
    xml = "xml"
    feather = "feather"
    parquet = "parquet"

AWS_DATA_CATALOG = "AwsDataCatalog"
AWS_SCHEMA_DATABASE_NAME = "ensembl-parquet-meta-schema"
AWS_S3_OUTPUT_DIR = "s3://ensembl-athena-results/"
SUPPORTED_FILE_FORMATS = [elem.value for elem in SupportedFileFormats]
PRESIGNED_URL_EXPIRATION_TIME = 3600