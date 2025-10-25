from src.managers.TrainingManager import TrainingManager
from src.data.DataSource import DataSource
from src.strategies.Strategy import Strategy
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from datetime import datetime, timedelta
import joblib
from sklearn.model_selection import train_test_split
class InvidualTrainingManager(TrainingManager):
    def __init__(
        self,
        instrumentsData: dict[str, DataSource],
        instruments: dict[str, Strategy],
        chunkSize: int,
        isStarted: bool,
        model_params=None,
        lookback_window=30
    ):
        self.model_params = model_params or {
            'n_estimators': 500,
            'max_depth': 20,
            'random_state': 42,
            'n_jobs': -1,
            'warm_start': True
        }
        self.lookback_window = lookback_window

        # state
        self.model = None
        self.featured_data = pd.DataFrame()
        self.data = {}               # dict[ticker] -> DataFrame
        self.ticker = None
        self.interval = None
        self.last_trained = None
        self.feature_importances = None
    @staticmethod
    def transform_candles_to_dataframe(self, candles):
       
        if isinstance(candles, DataSource):
            candles = candles.candles

        if isinstance(candles, pd.DataFrame):
            return candles.copy()

        try:
            data = {
                'datetime': [candle.datetime for candle in candles],
                'open': [candle.open for candle in candles],
                'high': [candle.high for candle in candles],
                'low': [candle.low for candle in candles],
                'close': [candle.close for candle in candles],
                'volume': [candle.volume for candle in candles],
            }
        except Exception:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        if 'datetime' in df.columns:
            try:
                df['datetime'] = pd.to_datetime(df['datetime'])
                df.set_index('datetime', inplace=True)
            except Exception:
                pass
        return df
    @staticmethod
    def generate_features(self, df: pd.DataFrame, ticker_id: int = None):
        data = df.copy()
        if 'datetime' in data.columns:
            data['datetime'] = pd.to_datetime(data['datetime'])
            data.set_index('datetime', inplace=True)

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

        if ticker_id is not None:
            data['ticker_id'] = ticker_id

        return data

    def prepare_sequences(self, featured_df: pd.DataFrame):
        if featured_df is None or featured_df.empty:
            return None, None

        feature_columns = ['close', 'close_lag1', 'close_lag2', 'close_lag3',
                           'pct_change', 'volatility', 'sma5', 'vol_ratio', 'bullish']

        if 'ticker_id' in featured_df.columns:
            feature_columns = feature_columns + ['ticker_id']

        if not all(col in featured_df.columns for col in feature_columns + ['next_close']):
            return None, None

        features = []
        targets = []

        for i in range(self.lookback_window, len(featured_df)):
            seq = featured_df[feature_columns].iloc[i - self.lookback_window:i]
            features.append(seq.values)
            targets.append(featured_df['next_close'].iloc[i])

        if len(features) == 0:
            return None, None

        return np.array(features), np.array(targets)

    def train_on_tickers(self, datasets: dict, incremental: bool = False, test_size: float = 0.2):
        all_X = []
        all_y = []
        ticker_to_id = {}
        for idx, (ticker, data_obj) in enumerate(datasets.items()):
            df = self.transform_candles_to_dataframe(data_obj)
            if df.empty:
                continue
            ticker_to_id[ticker] = idx
            feat = self.generate_features(df, ticker_id=idx)
            X_i, y_i = self.prepare_sequences(feat)
            if X_i is None:
                continue
            all_X.append(X_i)
            all_y.append(y_i)
            self.data[ticker] = df

        if len(all_X) == 0:
            print("Нет достаточно данных по тикерам для обучения")
            return None, None

        X = np.vstack(all_X)
        y = np.concatenate(all_y)

        n_samples, seq_len, n_feats = X.shape
        X_2d = X.reshape(n_samples, seq_len * n_feats)

        X_train, X_test, y_train, y_test = train_test_split(X_2d, y, test_size=test_size, random_state=42, shuffle=True)

        if self.model is None:
            self.model = RandomForestRegressor(**self.model_params)

        if incremental and getattr(self, 'model', None) is not None:
            try:
                self.model.n_estimators = int(self.model.n_estimators * 1.1) + 1
            except Exception:
                pass

        self.model.fit(X_train, y_train)
        self.last_trained = datetime.now()
        self.feature_importances = self.model.feature_importances_

        predictions = self.model.predict(X_test)
        mae = mean_absolute_error(y_test, predictions)
        r2 = r2_score(y_test, predictions)

        combined = []
        for ticker, df in self.data.items():
            tid = ticker_to_id.get(ticker, None)
            fdf = self.generate_features(df, ticker_id=tid)
            if not fdf.empty:
                fdf = fdf.copy()
                fdf['ticker'] = ticker
                combined.append(fdf)
        if combined:
            self.featured_data = pd.concat(combined, axis=0)
            try:
                self.featured_data.sort_index(inplace=True)
            except Exception:
                pass

        print(f"Обучение завершено на {len(datasets)} тикерах. MAE: {mae:.4f}, R²: {r2:.4f}")
        return mae, r2

    def predict_for_ticker(self, ticker, recent_candles):
        
        df = self.transform_candles_to_dataframe(recent_candles)
        if df.empty:
            return None
        feat = self.generate_features(df, ticker_id=None)
        if len(feat) < self.lookback_window:
            return None
        last_seq = feat[['close', 'close_lag1', 'close_lag2', 'close_lag3',
                         'pct_change', 'volatility', 'sma5', 'vol_ratio', 'bullish']].iloc[-self.lookback_window:]
        if 'ticker_id' in feat.columns:
            last_seq = pd.concat([last_seq, feat[['ticker_id']].iloc[-self.lookback_window:]], axis=1)
        X = last_seq.values.reshape(1, -1)
        if self.model is None:
            return None
        return self.model.predict(X)[0]

    def save_model(self, filename):
        if self.model is None:
            return False

        save_data = {
            'model': self.model,
            'featured_data': self.featured_data,
            'data': self.data,
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
            self.model = save_data.get('model')
            self.featured_data = save_data.get('featured_data', pd.DataFrame())
            self.data = save_data.get('data', {})
            self.last_trained = save_data.get('last_trained')
            self.lookback_window = save_data.get('lookback_window', self.lookback_window)
            self.feature_importances = save_data.get('feature_importances')
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
        base_feature_names = [
            'close', 'close_lag1', 'close_lag2', 'close_lag3',
            'pct_change', 'volatility', 'sma5', 'vol_ratio', 'bullish'
        ]
        if 'ticker_id' in (self.featured_data.columns if not self.featured_data.empty else []):
            base_feature_names = base_feature_names + ['ticker_id']

        full_feature_names = []
        for i in range(self.lookback_window):
            for feat in base_feature_names:
                full_feature_names.append(f"{feat}_t-{self.lookback_window - i - 1}")

        importances = pd.Series(self.feature_importances[:len(full_feature_names)],
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

        feature_columns = ['close', 'close_lag1', 'close_lag2', 'close_lag3',
                           'pct_change', 'volatility', 'sma5', 'vol_ratio', 'bullish']
        if 'ticker_id' in self.featured_data.columns:
            feature_columns = feature_columns + ['ticker_id']

        start_idx = max(self.lookback_window, len(self.featured_data) - num_points - self.lookback_window)
        features = []
        for i in range(start_idx, len(self.featured_data)):
            seq = self.featured_data[feature_columns].iloc[i - self.lookback_window:i]
            features.append(seq.values)

        if not features:
            print("Недостаточно данных для визуализации")
            return

        X = np.array(features)
        y = self.featured_data['next_close'].iloc[start_idx:]

        n_features = X.shape[1] * X.shape[2]
        X_2d = X.reshape(X.shape[0], n_features)
        predictions = self.model.predict(X_2d)

        timestamps = self.featured_data.index[start_idx:]

        plt.figure(figsize=(12, 6))
        plt.plot(timestamps, y, label='Реальная цена', alpha=0.7)
        plt.plot(timestamps, predictions, label='Прогноз', linestyle='--', alpha=0.7)
        plt.title('Сравнение прогнозов и реальных цен (много тикеров)')
        plt.xlabel('Время')
        plt.ylabel('Цена')
        plt.legend()
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()