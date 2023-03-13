import logging
from database.database import database_connection

# Initialize info and error logging.
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS RDS PostgreSQL database connection.
connection = database_connection()
def handler(event, context):
    """
    Fetch content from s3 to RDS PostgreSQL instance database.
    """

    with connection.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;")
        cur.execute("DROP TABLE IF EXISTS most_visited_pages_7days;")
        cur.execute("CREATE TABLE most_visited_pages_7days(page varchar(255), views varchar(255), users varchar(255), views_per_user varchar(255), avg_engagement_time varchar(255));")
        cur.execute("SELECT aws_s3.table_import_from_s3( 'most_visited_pages_7days','','(format csv)','(my-s3-bucket-2024,most-visited-pages-7days.csv,eu-west-1)');")

        cur.execute("DROP TABLE IF EXISTS most_visited_pages_365days;")
        cur.execute("CREATE TABLE most_visited_pages_365days(date varchar(255), page varchar(255), views varchar(255), users varchar(255), views_per_user varchar(255), avg_engagement_time varchar(255));")
        cur.execute("SELECT aws_s3.table_import_from_s3( 'most_visited_pages_365days','','(format csv)','(my-s3-bucket-2024,most-visited-pages-365days.csv,eu-west-1)');")
    connection.commit()

    logger.info("SUCCESS: Added items to RDS PostgreSQL table.")
    return "SUCCESS: Added items to RDS PostgreSQL table." 
