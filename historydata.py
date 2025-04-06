import pandas as pd
import requests
from datetime import datetime, timedelta
import pytz
import time

def fetch_moex_chunk(ticker, start_dt, end_dt, interval, timeout=10):
    #Выполняет один запрос к MOEX API с пагинацией
    tz = pytz.timezone('Europe/Moscow')
    url = f'https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json'
    params = {
        'from': start_dt.strftime('%Y-%m-%d %H:%M:%S'),
        'till': end_dt.strftime('%Y-%m-%d %H:%M:%S'),
        'interval': interval,
        'iss.meta': 'off',
        'iss.only': 'candles',
        'limit': 10000
    }
    
    try:
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data['candles']['data'], columns=data['candles']['columns'])
        
        if not df.empty:
            df['begin'] = pd.to_datetime(df['begin']).dt.tz_localize(tz)
            df.set_index('begin', inplace=True)
            return df[['open', 'high', 'low', 'close', 'volume']]
        return pd.DataFrame()
    
    except Exception as e:
        print(f'Ошибка: {str(e)}')
        return pd.DataFrame()

def get_moex_history(ticker, start_date, end_date, interval=1):
    """Получает полную историю данных с пагинацией"""
    tz = pytz.timezone('Europe/Moscow')
    current_start = tz.localize(datetime.strptime(start_date, '%Y-%m-%d'))
    end_dt = tz.localize(datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1))
    
    all_data = []
    max_retries = 3
    retry_delay = 5
    
    while current_start < end_dt:
        # Рассчитываем конечную дату для чанка
        chunk_end = min(
            current_start + calculate_chunk_duration(interval),
            end_dt
        )
        
        for attempt in range(max_retries):
            chunk = fetch_moex_chunk(
                ticker=ticker,
                start_dt=current_start,
                end_dt=chunk_end,
                interval=interval
            )
            if not chunk.empty:
                break
            time.sleep(retry_delay)
        else:
            print(f"Не удалось получить данные после {max_retries} попыток")
            break
        
        if not chunk.empty:
            all_data.append(chunk)
            # Сдвигаем начало на следующий период
            current_start = chunk.index[-1] + timedelta(minutes=interval)
        else:
            # Если данные закончились, выходим из цикла
            break
        
        # Задержка для соблюдения лимитов API
        time.sleep(0.5)
    
    return pd.concat(all_data) if all_data else pd.DataFrame()

def calculate_chunk_duration(interval):
    """Определяет оптимальный период для одного запроса"""
    if interval == 1:    # 1 минута: ~20 дней
        return timedelta(days=20)
    elif interval == 10: # 10 минут: ~200 дней
        return timedelta(days=200)
    elif interval == 60: # 1 час: ~400 дней
        return timedelta(days=400)
    else:                # Дневные данные: 10 лет
        return timedelta(days=365*10)

# Пример использования
if __name__ == "__main__":
    df = get_moex_history(
        ticker='SBER',
        start_date='2023-01-01',
        end_date='2024-03-01',
        interval=1
    )
    
    print(f"Получено данных: {len(df)} записей")
    print(f"Период данных: {df.index[0]} - {df.index[-1]}")
    print(df.head())