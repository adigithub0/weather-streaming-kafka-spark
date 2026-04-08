import requests
import time
import os
import json
from kafka import KafkaProducer
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

API_KEY = os.getenv("API_KEY")
CITIES = json.loads(os.getenv("CITIES"))

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_weather(city):
    url = "http://api.openweathermap.org/data/2.5/weather"
    params = {"q": city, "appid": API_KEY, "units": "metric"}
    return requests.get(url, params=params).json()

while True:
    for city in CITIES:
        data = get_weather(city)

        if data.get("main"):
            payload = {
                "city": city,
                "country": data["sys"]["country"],
                "temperature": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "timestamp": str(datetime.now())
            }

            print("Sending:", payload)
            producer.send("weather-data", value=payload)

    producer.flush()
    time.sleep(120)