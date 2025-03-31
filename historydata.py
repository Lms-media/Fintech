import pandas as pd
from datetime import datetime
import requests
import ccxt
from typing import Optional

def get_historical_data(
    exchange_id: str,
    symbol: str,
    timeframe: str = '1d',
    since: Optional[datetime] = None,
    limit: int = 1000,
    auto_paginate: bool = False
) -> pd.DataFrame:
    """
    Загружает исторические данные OHLCV с крипто-бирж (через CCXT) или MOEX.

    Параметры:
    - exchange_id: 'moex' или идентификатор биржи из CCXT (binance, coinbasepro)
    - symbol: Тикер (для MOEX: 'SBER', 'GAZP') или пара BTC/USDT
    - timeframe: Таймфрейм (для MOEX всегда дневные данные)
    - since: Дата начала загрузки
    - limit: Лимит записей (для MOEX макс. 100)
    - auto_paginate: Автоподгрузка (только для CCXT)

    Возвращает DataFrame с колонками: timestamp, open, high, low, close, volume
    """
    
    if exchange_id.lower() == 'moex':
        return _fetch_moex_data(symbol, since, limit)


def _fetch_moex_data(symbol, since, limit):
    """Загрузка данных с Московской Биржи"""
    url = "https://iss.moex.com/iss/history/engines/stock/markets/shares/securities/{}.json"
    
    params = {
        'from': since.strftime("%Y-%m-%d") if since else '1990-01-01',
        'till': datetime.now().strftime("%Y-%m-%d"),
        'start': 0,
        'limit': min(limit, 100)  # MOEX API ограничивает 100 записями за запрос
    }
    
    data = []
    while True:
        response = requests.get(url.format(symbol), params=params)
        response.raise_for_status()
        
        json_data = response.json()
        history = json_data['history']
        columns = history['columns']
        rows = history['data']
        
        if not rows: break
            
        # Преобразование в словарь с нужными полями
        for row in rows:
            item = dict(zip(columns, row))
            data.append({
                'timestamp': pd.to_datetime(item['TRADEDATE']),
                'open': item['OPEN'],
                'high': item['HIGH'],
                'low': item['LOW'],
                'close': item['CLOSE'],
                'volume': item['VOLUME']
            })
        
        if len(rows) < params['limit'] or len(data) >= limit: 
            break
            
        params['start'] += params['limit']
    
    df = pd.DataFrame(data)
    return df.set_index('timestamp').sort_index().iloc[:limit]

# Пример использования
if __name__ == "__main__":
    # Данные с MOEX
    df_moex = get_historical_data(
        exchange_id='moex',
        symbol='SBER',
        since=datetime(2018, 1, 1),
        limit=500
    )
    print("Данные MOEX:")
    print(df_moex.head())