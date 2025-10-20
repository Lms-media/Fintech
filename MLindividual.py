import pandas as pd
import numpy as np
import pytz
import requests
import time
import joblib
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import TimeSeriesSplit
import matplotlib.pyplot as plt
import seaborn as sns

class TradingModel:
    
    def __init__(self, ticker, interval=1, model_params=None, max_data_points=1000000, lookback_window = 30):
        self.ticker = ticker
        self.interval = interval
        self.model = None
        self.data = pd.DataFrame()
        self.feature_importances = None
        self.last_trained = None
        self.max_data_points = max_data_points
        self.featured_data= pd.DataFrame()
        self.model_params = model_params or {
            'n_estimators': 500,
            'max_depth': 20,
            'random_state': 42,
            'n_jobs': -1,
            'warm_start': True
        }
        self.lookback_window = lookback_window
    
    def fetch_moex_chunk(self, args):
        ticker, start_dt, end_dt, interval, timeout, max_retries = args
        tz = pytz.timezone('Europe/Moscow')
        url = f'https://iss.moex.com/iss/engines/currency/markets/selt/securities/{ticker}/candles.json'
        
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
    
    def calculate_chunk_size(self):
        if self.interval == 1:
            return timedelta(days=7)
        elif self.interval == 10:
            return timedelta(days=30)
        elif self.interval == 60:
            return timedelta(days=90)
        else:
            return timedelta(days=365)
    
    def get_historical_data(self, start_date, end_date, max_workers=8):
        tz = pytz.timezone('Europe/Moscow')
        start_dt = tz.localize(datetime.strptime(start_date, '%Y-%m-%d'))
        end_dt = tz.localize(datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1))
        
        chunk_size = self.calculate_chunk_size()
        date_ranges = []
        current_start = start_dt
        
        while current_start < end_dt:
            current_end = min(current_start + chunk_size, end_dt)
            date_ranges.append((current_start, current_end))
            current_start = current_end
        
        args_list = [
            (self.ticker, start, end, self.interval, 10, 3) 
            for start, end in date_ranges
        ]
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.fetch_moex_chunk, args) for args in args_list]
            
            results = []
            for future in as_completed(futures):
                result = future.result()
                if not result.empty:
                    results.append(result)
                time.sleep(0.1)
        
        if results:
            full_data = pd.concat(results).sort_index()
            return full_data.loc[start_dt:end_dt]
        return pd.DataFrame()
    
    def append_new_data(self, new_end_date):
        if self.data.empty:
            self.data =  self.get_historical_data('2020-01-12', new_end_date)
        
        tz = pytz.timezone('Europe/Moscow')
        last_date = self.data.index[-1].to_pydatetime()
        start_date = (last_date + timedelta(minutes=self.interval)).strftime('%Y-%m-%d')
        
        new_data = self.get_historical_data(
            start_date=start_date,
            end_date=new_end_date
        )
        
        if not new_data.empty:
            combined_df = pd.concat([self.data, new_data])
            combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
            self.data =  combined_df.sort_index()
    
    def generate_features(self):

            if self.data.empty:
                return pd.DataFrame()
            
            data = self.data.copy()
            
            data['close_lag1'] = data['close'].shift(1)
            data['close_lag2'] = data['close'].shift(2)
            data['close_lag3'] = data['close'].shift(3)
            
            data['pct_change'] = data['close'].pct_change()
            
            data['daily_range'] = data['high'] - data['low']
            data['volatility'] = data['daily_range'].rolling(3).max()
            
            data['sma5'] = data['close'].rolling(5).mean()
            
            data['vol_ma5'] = data['volume'].rolling(5).mean()
            data['vol_ratio'] = data['volume'] / data['vol_ma5']
            
            data['bullish'] = (data['close'] > data['open']).astype(int)
            
            data['next_close'] = data['close'].shift(-1)
            
            data.dropna(inplace=True)
            
            return data
    
    def prepare_sequences(self):
        if self.featured_data.empty:
            return None, None

        feature_columns = ['close', 'close_lag1', 'close_lag2', 'close_lag3', 
                          'pct_change', 'volatility', 'sma5', 'vol_ratio', 'bullish']
        
        if not all(col in self.featured_data.columns for col in feature_columns):
            print("Отсутствуют необходимые признаки")
            return None, None
        
        features = []
        targets = []
        
        for i in range(self.lookback_window, len(self.featured_data)):
            seq = self.featured_data[feature_columns].iloc[i-self.lookback_window:i]
            features.append(seq.values)
            
            targets.append(self.featured_data['next_close'].iloc[i])
        
        return np.array(features), np.array(targets)
    
    def train_model(self, incremental = True):
        if incremental and self.model:
            new_data = self.data.tail(self.lookback_window * 2)
            new_featured = self.generate_features(new_data)
            
            if len(new_featured) < 10:
                return
                
            X_new, y_new = self.prepare_sequences(new_featured)
            
            n_features = X_new.shape[1] * X_new.shape[2]
            X_new_2d = X_new.reshape(X_new.shape[0], n_features)
            
            self.model.n_estimators += 10 
            self.model.fit(X_new_2d, y_new)
            
            return
        self.featured_data = self.generate_features()
        
        if len(self.featured_data) < self.lookback_window + 50:
            print(f"Недостаточно данных. Требуется минимум {self.lookback_window + 50} записей.")
            return None, None
        
        X, y = self.prepare_sequences()
        
        if X is None or y is None:
            return None, None
        

        split_idx = int(len(X) * 0.8)
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]
        
        n_features = X.shape[1] * X.shape[2]
        X_train_2d = X_train.reshape(X_train.shape[0], n_features)
        X_test_2d = X_test.reshape(X_test.shape[0], n_features)
        
        if self.model is None:
            self.model = RandomForestRegressor(**self.model_params)
        
        self.model.fit(X_train_2d, y_train)
        self.last_trained = datetime.now()
        self.feature_importances = self.model.feature_importances_
        predictions = self.model.predict(X_test_2d)
        mae = mean_absolute_error(y_test, predictions)
        r2 = r2_score(y_test, predictions)
        
        print(f"Обучение завершено. MAE: {mae:.4f}, R²: {r2:.4f}")
        return mae, r2
    
    def predict_next(self):
        if self.model is None or self.featured_data.empty:
            return None
        
        if len(self.featured_data) < self.lookback_window:
            print(f"Недостаточно данных для прогноза. Требуется {self.lookback_window} записей.")
            return None
        
        feature_columns = ['close', 'close_lag1', 'close_lag2', 'close_lag3', 
                          'pct_change', 'volatility', 'sma5', 'vol_ratio', 'bullish']
        
        last_sequence = self.featured_data[feature_columns].iloc[-self.lookback_window:]
        last_sequence_2d = last_sequence.values.reshape(1, -1)
        
        prediction = self.model.predict(last_sequence_2d)[0]
        return prediction
    
    def generate_signal(self, threshold=0.0005):
        prediction = self.predict_next()
        if prediction is None:
            return None, None
        
        current_price = self.featured_data['close'].iloc[-1]
        predicted_change = (prediction - current_price) / current_price
        
        if predicted_change > threshold:
            return 'BUY', predicted_change
        elif predicted_change < -threshold:
            return 'SELL', predicted_change
        else:
            return 'HOLD', predicted_change
    
    def update_and_predict(self, new_end_date):
        if self.append_new_data(new_end_date):
            print("Данные успешно обновлены")
            
            self.train_model()
            
            return self.generate_signal()
        return None, None
    
    def save_model(self, filename):
        if self.model is None:
            return False
        
        save_data = {
            'model': self.model,
            'featured_data': self.featured_data,
            'data': self.data,
            'ticker': self.ticker,
            'interval': self.interval,
            'last_trained': self.last_trained,
            'lookback_window': self.lookback_window,
            'feature_importances': self.feature_importances
        }
        
        try:
            joblib.dump(save_data, filename)
            print(f"Модель сохранена в {filename}")
            return True
        except Exception as e:
            print(f"Ошибка сохранения: {str(e)}")
            return False
    
    def load_model(self, filename):
        try:
            save_data = joblib.load(filename)
            self.model = save_data['model']
            self.featured_data = save_data['featured_data']
            self.data = save_data['data']
            self.ticker = save_data['ticker']
            self.interval = save_data['interval']
            self.last_trained = save_data['last_trained']
            self.lookback_window = save_data.get('lookback_window', 30)
            self.feature_importances = save_data['feature_importances']
            print(f"Модель загружена из {filename}")
            return True
        except Exception as e:
            print(f"Ошибка загрузки: {str(e)}")
            return False
    
    def visualize_features_importance(self):
        if self.feature_importances is None:
            print("Модель не обучена")
            return
        
        plt.figure(figsize=(10, 6))
        feature_names = [f"lag{i}" if i < 4 else feat 
                        for i, feat in enumerate([
                            'close', 'close_lag1', 'close_lag2', 'close_lag3',
                            'pct_change', 'volatility', 'sma5', 'vol_ratio', 'bullish'
                        ])]
        
        full_feature_names = []
        for i in range(self.lookback_window):
            for feat in feature_names:
                full_feature_names.append(f"{feat}_t-{self.lookback_window-i-1}")
        
        importances = pd.Series(self.model.feature_importances_[:len(full_feature_names)], 
                               index=full_feature_names)
        
        top_importances = importances.sort_values(ascending=False).head(20)
        sns.barplot(x=top_importances.values, y=top_importances.index)
        plt.title('Топ-20 важнейших признаков')
        plt.xlabel('Важность')
        plt.tight_layout()
        plt.show()
    
    def visualize_predictions(self, num_points=100):
        if self.model is None or self.featured_data.empty:
            print("Модель не обучена или данные отсутствуют")
            return
    
        if len(self.featured_data) < self.lookback_window + num_points:
            num_points = len(self.featured_data) - self.lookback_window
        
        feature_columns = ['close', 'close_lag1', 'close_lag2', 'close_lag3', 
                        'pct_change', 'volatility', 'sma5', 'vol_ratio', 'bullish']
        
        features = []
        start_idx = max(self.lookback_window, len(self.featured_data) - num_points - self.lookback_window)
        
        for i in range(start_idx, len(self.featured_data)):
            seq = self.featured_data[feature_columns].iloc[i-self.lookback_window:i]
            features.append(seq.values)
        
        X = np.array(features)
        y = self.featured_data['next_close'].iloc[start_idx:]
        
        n_features = X.shape[1] * X.shape[2]
        X_2d = X.reshape(X.shape[0], n_features)
        predictions = self.model.predict(X_2d)
        
        timestamps = self.featured_data.index[start_idx:]
        
        plt.figure(figsize=(12, 6))
        plt.plot(timestamps, y, label='Реальная цена', alpha=0.7)
        plt.plot(timestamps, predictions, label='Прогноз', linestyle='--', alpha=0.7)
        plt.title('Сравнение прогнозов и реальных цен')
        plt.xlabel('Время')
        plt.ylabel('Цена')
        plt.legend()
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
if __name__ == "__main__":
    model = TradingModel(ticker='USD000000TOD', interval=1)
    model.append_new_data('2024-03-01')
    model.train_model()
    signal, change = model.generate_signal()
    print(f"Текущий сигнал: {signal}, Ожидаемое изменение: {change*100:.2f}%")
    
    model.visualize_features_importance()
    model.visualize_predictions()
    
    model.save_model('USD_RUB_model_v1.pkl')
    
    new_model = TradingModel(ticker='USD000000TOD', interval=1)
    new_model.load_model('USD_RUB_model_v1.pkl')
    new_model.visualize_features_importance()
    new_model.visualize_predictions()
    signal, change = new_model.generate_signal()
    print(f"Сигнал из загруженной модели: {signal}, Изменение: {change*100:.2f}%")
    