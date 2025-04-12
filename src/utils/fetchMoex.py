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
                data = response.json()
                
                df = pd.DataFrame(data['candles']['data'], columns=data['candles']['columns'])
                if not df.empty:
                    df['begin'] = pd.to_datetime(df['begin']).dt.tz_localize(tz)
                    df.set_index('begin', inplace=True)
                    return df[['open', 'high', 'low', 'close', 'volume']]
                return pd.DataFrame()
            
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    print(f'Ошибка загрузки {startDt}-{endDt}: {str(e)}')
                    return pd.DataFrame()