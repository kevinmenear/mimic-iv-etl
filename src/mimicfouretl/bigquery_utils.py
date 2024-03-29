from google_auth_oauthlib.flow import InstalledAppFlow
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound

from pyspark.sql import SparkSession

import os
import re

_credentials_path = None
_project_id = None
_access_token = None
_client = None


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


def get_client(use_service_account_auth=False):
    if _credentials_path is None:
        raise ValueError("Credentials file path is not set.")
    if _project_id is None:
        raise ValueError("Project ID is not set.")

    if use_service_account_auth:
        credentials = service_account.Credentials.from_service_account_file(_credentials_path)
    else: 
        flow = InstalledAppFlow.from_client_secrets_file(_credentials_path, 
                                                        scopes=["https://www.googleapis.com/auth/bigquery"])
        credentials = flow.run_local_server(port=0)
    global _client
    _client = bigquery.Client(credentials=credentials, project=_project_id)
    global _access_token
    _access_token = _client._credentials.token
    return _client


def list_tables(dataset_id, client=None, use_local_data=False):
    if use_local_data:
        tables = []
        for file_name in os.listdir(f'../data/sample/'):
            if file_name.startswith(dataset_id):
                tables.append(file_name)    
        return tables
    else:
        tables = client.list_tables(dataset_id)
        return [f"{dataset_id}.{table.table_id}" for table in tables]


def get_spark_session(materialization_dataset="mimiciv_materialization", use_service_account_auth=False, use_local_data=False):
    if use_local_data:
        spark = SparkSession.builder \
            .appName("Local Physionet copy") \
            .getOrCreate()
        return spark
    else:
        spark = SparkSession.builder \
                    .appName("BigQuery with OAuth") \
                    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:latest.version") \
                    .getOrCreate()
        spark.read.format("bigquery").option("credentialsFile", "_credentials_path")
        
        # Check if the materialization dataset exists, create it if it doesn't
        dataset_ref = _client.dataset(materialization_dataset)
        
        try:
            dataset = _client.get_dataset(dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset = _client.create_dataset(dataset)
        
        if not use_service_account_auth: 
            spark.conf.set("gcpAccessToken", _access_token)
        spark.conf.set("materializationDataset", materialization_dataset)
        spark.conf.set("viewsEnabled", "true")
        return spark


def run_query(spark, query, use_local_data=False):  
    if use_local_data:
        for file_name in os.listdir(f'../data/sample/'):
            df = spark.read.csv(f"../data/sample/{file_name}", header=True, inferSchema=True)
            df.createOrReplaceTempView(file_name.split('.')[1])
            
        query = query.replace('mimiciv_hosp.', '')
        query = query.replace('mimiciv_icu.', '')
                        
        result = spark.sql(query)
        return result
    else:          
        # DataFrame with results
        df = spark.read.format("bigquery") \
                .option("query", query) \
                .load()
    return df


def display_sampled_df(spark_df, sample_type='random', number=10, seed=12):
    """
    Displays a sample of a PySpark DataFrame as a Pandas DataFrame.

    :param spark_df: The PySpark DataFrame to sample from.
    :param sample_type: Type of sample. Options: 'random_fraction', 'random_fixed', 'head'.
    :param fraction: Fraction of rows to sample (for 'random_fraction').
    :param number: Number of rows to sample (for 'random_fixed').
    :param seed: Seed for random number generator.
    """
    if sample_type == 'random':
        sampled_df = spark_df.sample(withReplacement=False, fraction=1.0, seed=seed).limit(number)
    elif sample_type == 'head':
        sampled_df = spark_df.limit(number)
    else:
        raise ValueError("Invalid sample type. Choose from 'random' or 'head'.")

    display(sampled_df.toPandas())


    