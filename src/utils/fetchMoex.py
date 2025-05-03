from datetime import datetime
import time
import pandas as pd
import pytz
import requests

def fetchMoex(ticker: str, start: int, end: int, interval: int):
    tz = pytz.timezone('Europe/Moscow')
    startDt = tz.localize(datetime.fromtimestamp(start))
    endDt = tz.localize(datetime.fromtimestamp(end))
    url = f'https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json'
    retries = 5
    
    for attempt in range(retries):
            try:
                response = requests.get(
                    url,
                    params={
                        'from': startDt.strftime('%Y-%m-%d %H:%M:%S'),
                        'till': endDt.strftime('%Y-%m-%d %H:%M:%S'),
                        'interval': interval,
                        'iss.meta': 'off',
                        'iss.only': 'candles',
                        'limit': 10000
                    },
                    timeout=5000
                )
                response.raise_for_status()
                json = response.json()
                
                candles = []
                columns = json["candles"]["columns"]
                data = json["candles"]["data"]
                for candle in data:
                    begin = candle[columns.index("begin")]
                    dict = {
                        "open": candle[columns.index("open")],
                        "close": candle[columns.index("close")],
                        "high": candle[columns.index("high")],
                        "low": candle[columns.index("low")],
                        "volume": candle[columns.index("volume")],
                        "time": datetime.strptime(begin, "%Y-%m-%d %H:%M:%S").timestamp(),
                    }
                    candles.append(dict)
                return candles
            
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    print(f'Ошибка загрузки {startDt}-{endDt}: {str(e)}')
                    return pd.DataFrame()