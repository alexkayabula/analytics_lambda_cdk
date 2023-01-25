import os
import sys
import logging
import psycopg2


#AWS RDS environment settings
rds_host  = os.getenv("RDS_POSTGRES_HOST")
db_name = os.getenv("RDS_POSTGRES_DB_NAME")
name = os.getenv("RDS_POSTGRES_DB_USER")
password = os.getenv("RDS_POSTGRES_DB_PASSWORD")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
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
        cur.execute("DROP TABLE most_visited_pages_7days;")
        cur.execute("CREATE TABLE most_visited_pages_7days(page varchar(255), views varchar(255), users varchar(255), viewsPerUser varchar(255), avgEngagementTime varchar(255));")
        cur.execute("SELECT aws_s3.table_import_from_s3( 'most_visited_pages_7days','','(format csv)','(ga-bucket,most-visited-pages-7days.csv,ap-northeast-1)');")
    conn.commit()

    logger.info("SUCCESS: Added items to RDS PostgreSQL table.")
    return "SUCCESS: Added items to RDS PostgreSQL table." 