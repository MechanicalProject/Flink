import requests
from datetime import datetime
import time
import pandas as pd
from kafka import KafkaProducer
import json

COLUMNS = ['Open_time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore']
URL = 'https://api.binance.com/api/v3/klines'


def get_bitcoin_data(end_date, tic_time, symbol):
    data = []
    start = end_date - tic_time

    params = {
        'symbol': symbol,
        'interval': '1s',
        'limit': 1000,
        'startTime': start,
        'endTime': end_date
    }
    params['startTime'] = start
    result = requests.get(URL, params = params)
    js = result.json()
    data.extend(js)

    if not data:
        print('해당 기간에 일치하는 데이터가 없습니다.')
        return -1
    
    df = pd.DataFrame(data)
    df.columns = COLUMNS
    df['Open_time'] = df.apply(lambda x:datetime.fromtimestamp(x['Open_time'] // 1000), axis=1)
    df = df.drop(columns = ['Close_time', 'ignore'])
    df['Symbol'] = symbol
    df.loc[:, 'Open':'tb_quote_av'] = df.loc[:, 'Open':'tb_quote_av'].astype(float)  # string to float
    df['trades'] = df['trades'].astype(int)
    
    return df


def kafka_produce(data):
    bootstrap_servers = ['localhost:19092']
    topic_name = 'source'
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                            value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    for index, row in data.iterrows():
        message = row.to_json()
        producer.send(topic_name, value=message)

    producer.flush()
    producer.close()


def coin_info_produce(sleep_sec:int, symbol:str):
    # D-1 
    start_time = int(time.time() - (60 * 60 * 24))*1000
    while True:
        df = get_bitcoin_data(start_time, 0, symbol)
        start_time += 1000
        
        time.sleep(sleep_sec)
        
        kafka_produce(df)
        

if __name__ == "__main__":
    coin_info_produce(1, 'BTCUSDT')