from snowflake.snowpark import Session  # type: ignore
from kafka import KafkaConsumer
import json
import os
import time
from dotenv import load_dotenv

load_dotenv()

connection_parameters = {
    "account": os.getenv("SF_ACCOUNT"),
    "user": os.getenv("SF_USER"),
    "password": os.getenv("SF_PASSWORD"),
    "warehouse": os.getenv("SF_WAREHOUSE"),
    "database": os.getenv("SF_DATABASE"),
    "schema": "BRONZE"
}

def start_bridge():

    session = Session.builder.configs(connection_parameters).create()
    print("Connected to Snowflake")

    while True:
        try:
            consumer = KafkaConsumer(
                'weather-data',
                bootstrap_servers=['localhost:9092'],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=10000
            )

            print("Connected to Kafka")

            batch = []

            for message in consumer:
                # Convert JSON to string
                json_str = json.dumps(message.value)

                # Escape single quotes (VERY IMPORTANT)
                json_str = json_str.replace("'", "''")

                batch.append(json_str)

                if len(batch) >= 50:
                    # ✅ FIXED VALUES FORMAT
                    values = ",".join([f"('{x}')" for x in batch])

                    # ✅ FIXED SQL (PARSE_JSON moved outside)
                    session.sql(f"""
                        INSERT INTO RAW_WEATHER (raw_content)
                        SELECT PARSE_JSON(column1)
                        FROM VALUES {values}
                    """).collect()

                    print(f"Inserted {len(batch)} records")
                    batch.clear()

        except Exception as e:
            print("[ERROR]", e)
            time.sleep(120)

if __name__ == "__main__":
    start_bridge()