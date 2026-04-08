from snowflake.snowpark import Session  # type: ignore
from kafka import KafkaConsumer
import json
import streamlit as st
import snowflake.connector # type: ignore
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()

# Snowflake connection
conn = snowflake.connector.connect(
    user=os.getenv("SF_USER"),
    password=os.getenv("SF_PASSWORD"),
    account=os.getenv("SF_ACCOUNT"),
    warehouse=os.getenv("SF_WAREHOUSE"),
    database=os.getenv("SF_DATABASE"),
    schema="GOLD"
)

print(os.getenv("SF_ACCOUNT"))

query = """
SELECT *
FROM WEATHER_DB.GOLD.WEATHER_AGG
"""

df = pd.read_sql(query, conn)

st.title("🌦️ Live Weather Dashboard")

st.dataframe(df)

st.bar_chart(df.set_index("COUNTRY"))