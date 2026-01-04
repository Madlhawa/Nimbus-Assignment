import os
import requests
import pandas as pd
from airflow.models import Variable

def fetch_ev_charging_data(api_key, latitude, longitude, distance, unit='km', maxresults=50000):
    """Fetches data from the OpenChargeMap API."""
    url = "https://api.openchargemap.io/v3/poi/"
    params = {
        'key': api_key,
        'latitude': latitude,
        'longitude': longitude,
        'distance': distance,
        'distanceunit': unit,
        'maxresults': maxresults,
        'output': 'json'
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error fetching data: {response.status_code}")

def normalize_ev_charging_data(data):
    """Flattens and cleans the JSON response into a DataFrame."""
    meta_fields = [
        'ID', 'UUID', 'UsageCost', 'NumberOfPoints',
        ['OperatorInfo', 'Title'], ['UsageType', 'Title'],
        ['AddressInfo', 'Title'], ['AddressInfo', 'AddressLine1'],
        ['AddressInfo', 'Town'], ['AddressInfo', 'Postcode'],
        ['AddressInfo', 'Latitude'], ['AddressInfo', 'Longitude'],
        ['AddressInfo', 'Country', 'Title'], 'DateLastVerified'
    ]

    df = pd.json_normalize(
        data, 
        record_path=['Connections'],
        meta=meta_fields,
        record_prefix='conn_',
        errors='ignore'
    )

    # Clean column names for BigQuery compatibility
    df.columns = [c.replace('.', '_') for c in df.columns]
    df['DateLastVerified'] = pd.to_datetime(df['DateLastVerified'])
    # Add current UTC insert timestamp for downstream auditing
    df['insert_datetime'] = pd.Timestamp.utcnow()
    return df

def extract():
    """Orchestration function called by Airflow."""
    # Retrieve the API key from Airflow Variables
    api_key = Variable.get("openchargemap_api_key")
    
    # Configuration using original Central London parameters
    latitude = 51.5145215458141
    longitude = -0.09063526851277263
    radius = 5
    
    # Path inside the Cloud Composer environment
    output_directory = '/home/airflow/gcs/data/'
    file_path = os.path.join(output_directory, 'ev_charging_data_normalized.csv')

    os.makedirs(output_directory, exist_ok=True)

    # Execution flow
    data = fetch_ev_charging_data(api_key, latitude, longitude, radius)
    if data:
        normalized_df = normalize_ev_charging_data(data)
        normalized_df.to_csv(file_path, index=False)
        print(f"Extraction successful: {file_path}")
    else:
        raise Exception('No data fetched from API; stopping pipeline to prevent downstream errors.')