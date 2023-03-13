import io
import logging
import csv
import boto3
from data.google_analytics_token import generate_access_token
from data.data_last_7days import fetch_visited_pages
from data.data_last_365days import fetch_visited_pages_365days
from data.date_formatter import formatDate

S3_BUCKET = "my-s3-bucket-2024"
s3_client = boto3.client('s3')

def handler(event, context):
    try:
        data = []
        # Fetch data from Google Analytics.
        access_token = generate_access_token()
        format_date = formatDate()
        most_visited_pages = fetch_visited_pages(access_token=access_token, format_date=format_date)
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

            # Upload data to s3 bucket CSV file.
            s3_client.put_object(Bucket=S3_BUCKET, ContentType='text/csv', Key=fileName, Body=csvio.getvalue())

    except Exception as e:
        logging.error("Error uploading Google Analytics data to s3", e)

    try:
        data = []
        # Fetch data from Google Analytics.
        access_token = generate_access_token()
        format_date = formatDate()
        most_visited_pages_365days = fetch_visited_pages_365days(access_token=access_token, format_date=format_date)
        data.append(most_visited_pages_365days)
        
        # Generate csv  file.
        fileName = "most-visited-pages-365days" + ".csv"
        csvio = io.StringIO()
        writer = csv.writer(csvio)
        headers = list(data[0][0].keys())
        writer.writerow(headers)
        for item in data[0]:
            values = list(item.values())
            writer.writerow(values)

            # Upload data to s3 bucket CSV file.
            s3_client.put_object(Bucket=S3_BUCKET, ContentType='text/csv', Key=fileName, Body=csvio.getvalue())
    except Exception as e:
        logging.error("Error uploading Google Analytics data to s3", e)