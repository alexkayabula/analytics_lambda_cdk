import io
import os
import json
import logging
import csv
from datetime import datetime
import boto3
from oauth2client.service_account import ServiceAccountCredentials
import requests

S3_BUCKET = os.getenv('S3_BUCKET')
SCOPE = os.getenv('SCOPE')
GOOGLE_ANALYTICS_URL = os.getenv('GOOGLE_ANALYTICS_URL')
s3_client = boto3.client('s3')

def handler(event, context):
    try:
        data = []
        # Fetch data from Google Analytics.
        access_token = generate_access_token()
        most_visited_pages = fetch_visited_pages(access_token=access_token)
        data.append(most_visited_pages)

        # Generate csv  file.
        now = datetime.now()
        date = now.strftime("%d-%m-%Y")
        fileName = "test-" + date + ".csv"
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


# Connect to s3, generate Google analytics access token from json secret file.
def generate_access_token():
    """Generate an access token from a service that communicates to a Google API.

    Args:
        scopes: A list of scopes to authorize for the application.
        keyfile_dict: The parsed dictionary-like object containing the contents of the JSON keyfile.

    Returns:
        An access token that is associated with the service account credentials.
    """

    s3_json_object = s3_client.get_object(Bucket=S3_BUCKET, Key="google_analytics_secrets.json")
    json_file = s3_json_object['Body'].read().decode("utf-8")
    json_data = json.loads(json_file)
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(keyfile_dict=json_data, scopes=[SCOPE])
    access_token = credentials.get_access_token()[0]
    return access_token


# Fetch Most visited pages in the last 7 days data.
def fetch_visited_pages(access_token):
    url = f'{GOOGLE_ANALYTICS_URL}{access_token}'
    try:
        logging.info("[Google Analytics] Fetching user mos visited pages data.")
        request_body = {"dimensions": [{"name": "unifiedPagePathScreen"}, 
                                       {"name": "unifiedScreenName"}],  "metrics": [{"name": "averageSessionDuration"}, 
                                      {"name": "screenPageViews"}, {"name": "screenPageViewsPerSession"}, 
                                      {"name": "totalUsers"}], "dateRanges": [{"startDate": "7daysAgo", "endDate": "today"}]}
        response = requests.post(url, json=request_body)
    except Exception as e:
        logging.debug("[Google Analytics] Error fetching data", e)
    else:
        result = json.loads(response.text)
        data = result["rows"]
        most_visited_pages = []
        for item in data:
            views = item["metricValues"][0]["value"]
            users = item["metricValues"][1]['value']
            if float(users) != 0.0:
                views_per_user = float(views) / float(users)
                total_engagement_time = item["metricValues"][2]["value"]
                avg_engagement_time = float(total_engagement_time)/ float(users)

                most_visited_pages.append({"page": item["dimensionValues"][1]['value'], 
                                                                        "views": item["metricValues"][0]["value"],
                                                                         "users" : users,
                                                                        "info": item["dimensionValues"][0]['value'],
                                                                        "views_per_user": views_per_user,
                                                                        "average_engagement_time": avg_engagement_time})
            else:
                views_per_user = 'n/a'
                avg_engagement_time = 'n/a'

                most_visited_pages.append({"page": item["dimensionValues"][1]['value'], 
                                                                        "views": item["metricValues"][0]["value"],
                                                                         "users" : users,
                                                                        "info": item["dimensionValues"][0]['value'],
                                                                        "views_per_user": views_per_user,
                                                                        "average_engagement_time": avg_engagement_time})
    
        return most_visited_pages
