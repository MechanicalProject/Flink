import requests
import json
from kafka import KafkaProducer
from datetime import datetime
import time

producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

base_url = "https://api.binance.com"

# 현재가 
api = """/api/v3/ticker/price?symbols=["BTCUSDC","ETHBTC","XRPBTC","ADABTC"]"""

def start():
    while True:
        response = requests.get(base_url  + api)
        data = response.json()

        date_str =response.headers['Date']
        date_obj = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S GMT')
        timestamp_str = date_obj.isoformat()

        message_data = {
            'data': data,
            'timestamp': timestamp_str
        }
        producer.send('price', value=message_data)
        producer.flush()
        print("send price")
        time.sleep(1)