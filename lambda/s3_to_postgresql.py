import json
import sys
import logging
import psycopg2
import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(logging.INFO)

secret_name = "mysecret"
region_name = "eu-west-1"
session = boto3.session.Session()

try:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/secrets-manager.html
    # Secrets Manager decrypts the secret value using the associated KMS CMK
    # Depending on whether the secret was a string or binary, only one of these fields will be populated
    client = session.client(service_name='secretsmanager',region_name=region_name,)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    if 'SecretString' in get_secret_value_response:
        text_secret_data = get_secret_value_response['SecretString']
        text_secret_json_data = json.loads( text_secret_data )
        rds_host  =  text_secret_json_data["host"]
        db_name =  text_secret_json_data["dbname"]
        name =  text_secret_json_data["username"]
        password =  text_secret_json_data["password"]
except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.error("The requested secret " + secret_name + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            logger.error("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            logger.error("The request had invalid params:", e)
        elif e.response['Error']['Code'] == 'DecryptionFailure':
            logger.error("The requested secret can't be decrypted using the provided KMS key:", e)
        elif e.response['Error']['Code'] == 'InternalServiceError':
            logger.error("An error occurred on service side:", e)
try:
    text_secret_data = get_secret_value_response['SecretString']
    text_secret_json_data = json.loads( text_secret_data )
    rds_host  =  text_secret_json_data["host"]
    db_name =  text_secret_json_data["dbname"]
    name =  text_secret_json_data["username"]
    password =  text_secret_json_data["password"]
    conn = psycopg2.connect(host=rds_host, user=name, password=password, database=db_name)
except psycopg2.DatabaseError as e:
    logger.error("ERROR: Unexpected error: Could not connect to PostgreSQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS PostgreSQL instance succeeded.")

def handler(event, context):
    """
    Fetch content from s3 to RDS PostgreSQL instance database.
    """

    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;")
        cur.execute("DROP TABLE IF EXISTS most_visited_pages_7days;")
        cur.execute("CREATE TABLE most_visited_pages_7days(page varchar(255), views varchar(255), users varchar(255), views_per_user varchar(255), avg_engagement_time varchar(255));")
        cur.execute("SELECT aws_s3.table_import_from_s3( 'most_visited_pages_7days','','(format csv)','(my-s3-bucket-2024,most-visited-pages-7days.csv,eu-west-1)');")

        cur.execute("DROP TABLE IF EXISTS most_visited_pages_365days;")
        cur.execute("CREATE TABLE most_visited_pages_365days(date varchar(255), page varchar(255), views varchar(255), users varchar(255), views_per_user varchar(255), avg_engagement_time varchar(255));")
        cur.execute("SELECT aws_s3.table_import_from_s3( 'most_visited_pages_365days','','(format csv)','(my-s3-bucket-2024,most-visited-pages-365days.csv,eu-west-1)');")
    conn.commit()

    logger.info("SUCCESS: Added items to RDS PostgreSQL table.")
    return "SUCCESS: Added items to RDS PostgreSQL table." 
