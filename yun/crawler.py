from datetime import datetime
import time
import pandas as pd
from tqdm import tqdm

from api import get_symbols, get_raw_data, get_raw_data_columns
from type import ApiData


def extract(start_date:str, end_date:str, symbol:str) -> ApiData:
    symbols = get_symbols(symbol)
    symbol = symbols[0]
    return ApiData(get_raw_data(start_date, end_date, symbol), symbol)


def transform(api_data: ApiData) -> pd.DataFrame:
    data = api_data.data
    symbol = api_data.symbol
    if not data:  # 해당 기간에 데이터가 없는 경우
        print('해당 기간에 일치하는 데이터가 없습니다.')
        return pd.DataFrame()
    df = pd.DataFrame(data)
    df.columns = get_raw_data_columns()
    df['Open_time'] = df.apply(lambda x:datetime.fromtimestamp(x['Open_time'] // 1000), axis=1)
    df = df.drop(columns = ['Close_time', 'ignore'])
    df['Symbol'] = symbol
    df.loc[:, 'Open':'tb_quote_av'] = df.loc[:, 'Open':'tb_quote_av'].astype(float)  # string to float
    df['trades'] = df['trades'].astype(int)
    return df


def load():
    pass




if __name__ == '__main__':
    start_date = '2024-01-01'
    end_date = '2024-01-04'
    convert_data = transform(extract(start_date, end_date, 'USDT'))
    print(convert_data)