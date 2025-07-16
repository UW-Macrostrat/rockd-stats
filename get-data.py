import pymysql
from dotenv import load_dotenv
import os

# Load variables from .env file
load_dotenv()

# Get DB credentials from environment
host = os.getenv("MARIADB_HOST")
user = os.getenv("MARIADB_USER")
password = os.getenv("MARIADB_PASSWORD")
database = os.getenv("MARIADB_DATABASE")

# Connect to MariaDB using env vars
conn = pymysql.connect(
    host=host,
    user=user,
    password=password,
    database=database,
)

# Example query: list all tables
with conn:
    with conn.cursor() as cursor:
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print("Tables:")
        for t in tables:
            print(f" - {list(t.values())[0]}")
