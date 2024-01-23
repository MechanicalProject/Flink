BASE_DATA_API_URL = 'https://api.binance.com/api/v3'
CONFIG_DATA_API_URL = f'{BASE_DATA_API_URL}/ticker/price'
RAW_DATA_API_URL = f'{BASE_DATA_API_URL}/klines'
RAW_DATA_API_COLUMNS = ['Open_time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close_time', 'quote_av', 'trades',  'tb_base_av', 'tb_quote_av', 'ignore']