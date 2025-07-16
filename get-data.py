import pymysql
from dotenv import load_dotenv
import os
import requests

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


API_URL = "http://localhost:5500/usage-stats"

BATCH_SIZE = 1000
last_id = 0 

with conn:
    with conn.cursor() as cursor:
        while True:
            # Keyset-based pagination using idvisit
            cursor.execute(f"""
                SELECT idvisit, location_latitude, location_longitude,
                       visit_first_action_time, location_ip
                FROM matomo_log_visit
                WHERE idvisit > %s
                  AND location_latitude IS NOT NULL
                  AND location_latitude != 43.071000
                ORDER BY idvisit ASC
                LIMIT %s
            """, (last_id, BATCH_SIZE))
            
            rows = cursor.fetchall()
            if not rows:
                break 

            # Prepare and send batch to API
            payload = [
                {
                    "lat": float(row[1]),
                    "lng": float(row[2]),
                    "date": str(row[3]),
                    "ip": str(row[4])
                }
                for row in rows
            ]

            data = {
                "data": payload,
            }

            try:
                response = requests.post(API_URL, json=data)
                response.raise_for_status()
                print(f"Sent batch starting with idvisit {rows[0][0]}, count: {len(rows)}")
            except requests.exceptions.RequestException as e:
                print(e)
                break 

            last_id = rows[-1][0]