import requests
import time
from datetime import datetime
from config import RAW_DATA_API_COLUMNS, CONFIG_DATA_API_URL, RAW_DATA_API_URL


def get_symbols(condition:str = 'USDT'):
  result = requests.get(CONFIG_DATA_API_URL)
  js = result.json()
  symbols = [x['symbol'] for x in js]
  symbols_usdt = [x for x in symbols if condition in x]
  return symbols_usdt


def get_raw_data(start:str, end:str, symbol:str):
    start = int(time.mktime(datetime.strptime(start + ' 00:00', '%Y-%m-%d %H:%M').timetuple())) * 1000
    end = int(time.mktime(datetime.strptime(end +' 23:59', '%Y-%m-%d %H:%M').timetuple())) * 1000
    params = {
        'symbol': symbol,
        'interval': '1m',
        'limit': 1000,
        'startTime': start,
        'endTime': end
    }
    data = []
    while start < end:
      print(datetime.fromtimestamp(start // 1000))
      params['startTime'] = start
      result = requests.get(RAW_DATA_API_URL, params = params)
      js = result.json()
      if not js:
          break
      data.extend(js)  # result에 저장
      start = js[-1][0] + 60000  # 다음 step으로
      
    return data
  
  
def get_raw_data_columns():
  return RAW_DATA_API_COLUMNS