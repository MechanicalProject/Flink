from datetime import datetime
import time
import pandas as pd
from tqdm import tqdm

from api import get_symbols, get_raw_data, get_raw_data_columns


def get_data(start_date, end_date, symbol):
    data = get_raw_data(start_date, end_date, symbol)
    # 전처리
    if not data:  # 해당 기간에 데이터가 없는 경우
        print('해당 기간에 일치하는 데이터가 없습니다.')
        return -1
    df = pd.DataFrame(data)
    df.columns = get_raw_data_columns()
    df['Open_time'] = df.apply(lambda x:datetime.fromtimestamp(x['Open_time'] // 1000), axis=1)
    df = df.drop(columns = ['Close_time', 'ignore'])
    df['Symbol'] = symbol
    df.loc[:, 'Open':'tb_quote_av'] = df.loc[:, 'Open':'tb_quote_av'].astype(float)  # string to float
    df['trades'] = df['trades'].astype(int)
    return df



if __name__ == '__main__':
    symbols = get_symbols('USDT')
    start_date = '2024-01-01'
    end_date = '2024-01-04'
    symbol = symbols[0]
    sample = get_data(start_date, end_date, symbol)
    print(sample)