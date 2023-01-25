import io
import os
from pathlib import Path
import logging
import csv
from datetime import datetime
import boto3
from helper import generate_access_token
from data_last_7days import fetch_visited_pages

# Set environment variables
S3_BUCKET = os.getenv('S3_BUCKET')
s3_client = boto3.client('s3')

def handler(event, context):
    try:
        data = []
        # Fetch data from Google Analytics.
        access_token = generate_access_token()
        most_visited_pages = fetch_visited_pages(access_token=access_token)
        data.append(most_visited_pages)

        # Generate csv  file.
        fileName = "most-visited-pages-7days" + ".csv"
        csvio = io.StringIO()
        writer = csv.writer(csvio)
        headers = list(data[0][0].keys())
        writer.writerow(headers)
        for item in data[0]:
            values = list(item.values())
            writer.writerow(values)
            # Upload csv file to s3 bucket.
            s3_client.put_object(Bucket=S3_BUCKET, ContentType='text/csv', Key=fileName, Body=csvio.getvalue())
    except Exception as e:
        logging.error(e)

