from api import get_symbols, get_raw_data, get_raw_data_columns
from type import ApiData
from kafka import KafkaProducer
import json

from config import KAFKA_SERVERS, KAFKA_TOPIC

producer = KafkaProducer(bootstrap_servers=[KAFKA_SERVERS],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


def extract(start_date:str, end_date:str, symbol:str) -> ApiData:
    symbols = get_symbols(symbol)
    symbol = symbols[0]
    return ApiData(get_raw_data(start_date, end_date, symbol), symbol, get_raw_data_columns())


def transform(api_data: ApiData) -> ApiData:
    return api_data


def load(data: ApiData):
    symbol = data.symbol
    columns = data.columns
    for row in data.data:
        json_data = {columns[i]: row[i] for i in range(len(row))}
        print(json_data)
        producer.send(KAFKA_TOPIC, json_data)




if __name__ == '__main__':
    start_date = '2024-01-01'
    end_date = '2024-01-04'
    convert_data = transform(extract(start_date, end_date, 'USDT'))
    LD.load(convert_data)
    print(convert_data)