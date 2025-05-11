import pandas as pd
import requests
from datetime import datetime, timedelta
import pytz
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_moex_chunk(args):
    """Выполняет один запрос к MOEX API с возможностью повторных попыток"""
    ticker, start_dt, end_dt, interval, timeout, max_retries = args
    tz = pytz.timezone('Europe/Moscow')
    url = f'https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json'
    
    for attempt in range(max_retries):
        try:
            response = requests.get(
                url,
                params={
                    'from': start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                    'till': end_dt.strftime('%Y-%m-%d %H:%M:%S'),
                    'interval': interval,
                    'iss.meta': 'off',
                    'iss.only': 'candles',
                    'limit': 10000
                },
                timeout=timeout
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
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                print(f'Ошибка загрузки {start_dt}-{end_dt}: {str(e)}')
                return pd.DataFrame()

def get_moex_history_fast(ticker, start_date, end_date, interval=1, max_workers=4):
    """Параллельная загрузка исторических данных с ускорением"""
    tz = pytz.timezone('Europe/Moscow')
    start_dt = tz.localize(datetime.strptime(start_date, '%Y-%m-%d'))
    end_dt = tz.localize(datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1))
    
    chunk_size = calculate_chunk_size(interval)
    date_ranges = []
    current_start = start_dt
    
    while current_start < end_dt:
        current_end = min(current_start + chunk_size, end_dt)
        date_ranges.append((current_start, current_end))
        current_start = current_end
    
    args_list = [(ticker, start, end, interval, 10, 3) for start, end in date_ranges]
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_moex_chunk, args) for args in args_list]
        
        results = []
        for future in as_completed(futures):
            result = future.result()
            if not result.empty:
                results.append(result)
            time.sleep(0.1)  # Задержка между обработкой результатов
    
    final_df = pd.concat(results).sort_index() if results else pd.DataFrame()
    return final_df.loc[start_dt:end_dt]

def calculate_chunk_size(interval):
    """Определение оптимального размера чанка для параллельной загрузки"""
    if interval == 1:
        return timedelta(days=7)
    elif interval == 10:
        return timedelta(days=30)
    elif interval == 60:
        return timedelta(days=90)
    else:
        return timedelta(days=365)

def append_new_data(existing_df, ticker, new_end_date, interval=1):
    """Добавляет новые данные к существующему DataFrame"""
    if existing_df.empty:
        return get_moex_history_fast(ticker, new_end_date, new_end_date, interval)
    
    tz = pytz.timezone('Europe/Moscow')
    last_date = existing_df.index[-1].to_pydatetime()
    start_date = (last_date + timedelta(minutes=interval)).strftime('%Y-%m-%d')
    
    new_data = get_moex_history_fast(
        ticker=ticker,
        start_date=start_date,
        end_date=new_end_date,
        interval=interval
    )
    
    if not new_data.empty:
        combined_df = pd.concat([existing_df, new_data])
        combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
        return combined_df.sort_index()
    
    return existing_df

# Пример использования
if __name__ == "__main__":
    # Параллельная загрузка за большой период
    big_df = get_moex_history_fast('GAZP', '2023-01-01', '2024-01-01',interval=1, max_workers=6)
    print(f"Загружено данных за год: {len(big_df)} записей")
    print(big_df.tail())