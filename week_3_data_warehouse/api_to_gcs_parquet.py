from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import requests
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_google_cloud_storage(**kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    
    uris_base = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-'
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

    bucket_name = 'ammar-demo-bucket-1'
    
    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'congestion_surcharge': float ,
    }
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    for month in months:
        url = uris_base + month + '.parquet'
        print(f'Fetching {url}...')
        df = pd.read_parquet(url)
        #update the columns data types
        df = df.astype(taxi_dtypes)
        #update the columns data types date
        df[parse_dates] = df[parse_dates].apply(pd.to_datetime)
        
        object_key = 'green_tripdata_2022-' + month + '.parquet'
        print(f'Exporting {object_key} to {bucket_name}...')
        GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
            df,
            bucket_name,
            object_key)