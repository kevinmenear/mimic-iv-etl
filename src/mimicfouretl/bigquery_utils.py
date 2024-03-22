from google_auth_oauthlib.flow import InstalledAppFlow
from google.cloud import bigquery

from pyspark.sql import SparkSession

# Global variables to store credentials and project ID
_credentials_path = None
_project_id = None
_access_token = None

def set_credentials_file(path):
    global _credentials_path
    _credentials_path = path

def get_credentials_file():
    return _credentials_path

def set_project_id(id):
    global _project_id
    _project_id = id

def get_project_id():
    return _project_id

def get_client():
    if _credentials_path is None:
        raise ValueError("Credentials file path is not set.")
    if _project_id is None:
        raise ValueError("Project ID is not set.")

    flow = InstalledAppFlow.from_client_secrets_file(_credentials_path, scopes=["https://www.googleapis.com/auth/bigquery"])
    credentials = flow.run_local_server(port=0)
    client = bigquery.Client(credentials=credentials, project=_project_id)
    global _access_token
    _access_token = client._credentials.token
    return client

def list_tables(client, dataset_id):
    tables = client.list_tables(dataset_id)
    return [f"{dataset_id}.{table.table_id}" for table in tables]

def get_spark_session(client):
    spark = SparkSession.builder \
    .appName("BigQuery with OAuth") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:latest.version") \
    .getOrCreate()
    spark.read.format("bigquery").option("credentialsFile", "_credentials_path")
    spark.conf.set("gcpAccessToken", _access_token)
    materialization_dataset = "mimiciv_materialization"
    spark.conf.set("viewsEnabled", "true")
    spark.conf.set("materializationDataset", materialization_dataset)
    return spark

def run_query(spark, query):
    # DataFrame with results
    df = spark.read.format("bigquery") \
    .option("query", query) \
    .load()
    return df

    