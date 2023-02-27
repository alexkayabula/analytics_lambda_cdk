
import os
import json
import boto3
from oauth2client.service_account import ServiceAccountCredentials

S3_BUCKET = "my-s3-bucket-2024"
SCOPE = "https://www.googleapis.com/auth/analytics.readonly"
s3_client = boto3.client('s3')

# Connect to s3, generate Google analytics access token from json secret file
def generate_access_token():
    """Generate an access token from a service that communicates to a Google API.

    Args:
        scopes: A list of scopes to authorize for the application.
        keyfile_dict: The parsed dictionary-like object containing the contents of the JSON keyfile.

    Returns:
        An access token that is associated with the service account credentials.
    """

    # Note: The google_analytics_secrets.json file containing google analytics credentials is 
    # uploaded into the bucket from Amazon management console .
    s3_json_object = s3_client.get_object(Bucket=S3_BUCKET, Key="google_analytics_secrets.json")
    json_file = s3_json_object['Body'].read().decode("utf-8")
    json_data = json.loads(json_file)
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(keyfile_dict=json_data, scopes=[SCOPE])
    access_token = credentials.get_access_token()[0]
    return access_token
    