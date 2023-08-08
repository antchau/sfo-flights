from prefect import task, flow, get_run_logger

from io import StringIO
import requests
import os
import logging

import boto3
import pandas as pd

import constants

DATA_SF_API_URL=constants.URL_DATA_SF_GOV
DATA_SF_API_KEY=constants.API_DATA_SF_GOV
AWS_S3_BUCKET = constants.AWS_S3_BUCKET
AWS_ACCESS_KEY_ID = constants.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = constants.AWS_SECRET_ACCESS_KEY

@task(retries = 1)
def upload_pandas_to_s3(df, bucket_name, object_name=None):
    """Upload a pandas data frame to an S3 bucket

    :param df: Pandas data frame
    :param bucket_name: S3 Bucket to upload to
    :param object_name: S3 object name. 
    """

    # bucket_name = sfo-flights
    # object_name = 'experimental/flights_test.csv'    
    s3_path = os.path.join('s3://', bucket_name, object_name)
    logger = get_run_logger()

    # Upload the file
    try:
        df.to_csv(s3_path, 
                  index = False,
                  storage_options={
                      "key": AWS_ACCESS_KEY_ID,
                      "secret": AWS_SECRET_ACCESS_KEY}
                      )
        logger.info(f"Writing data frame to {s3_path}")
    except:
        logger.info("Unable to write data frame to S3")
        return False
    
    

@flow(name = "Get flights", retries = 2, log_prints=True)
def get_sfo_flights(api_url = DATA_SF_API_URL, api_key = DATA_SF_API_KEY):
    headers = {'X-Access-Token': api_key}
    payload = {'$limit': '1500',
               '$where': "time > '2022-07-03' and airline = 'United'"}

    try:
        response = requests.get(api_url, params = payload, headers = headers)
        response_json = response.json()
        
        print(response.url)
        flights = pd.DataFrame.from_dict(response_json)

        logger = get_run_logger()
        logger.info(f"SFO flights data statistics 🤓:")
        logger.info(f"Number of rows: {flights.shape[0]}")
        logger.info(f"Columns: {flights.columns}")

        upload_pandas_to_s3(flights, "sfo-flights", "experimental/flights_test.csv")
        logger.info("Writing data to S3")
    
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)


if __name__ == "__main__":
    get_sfo_flights()

