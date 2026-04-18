from datetime import datetime

import numpy as np
import pandas as pd
# from sklearn.ensemble import RandomForestRegressor
# from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor
from typing import Optional
from Interfaces import ICandleSeries
from Entities import PredictionMeta
from ..Interfaces import ITrainablePredictorAlgo
from .RandomForestPredictorValue import RandomForestPredictorValue


class RandomForestAlgo(ITrainablePredictorAlgo[RandomForestPredictorValue]):
    _candlesCount: int
    _model: XGBRegressor
    _limits: list[tuple[float, float]]
    _lookback_window: int
    _is_trained: bool

    def __init__(self, candlesCount: int, lookback_window: int = 10, **rf_params):
        self._candlesCount = candlesCount
        self._lookback_window = lookback_window
        self._model = XGBRegressor(**rf_params) if rf_params else XGBRegressor(
            n_estimators=500,
            max_depth=8,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_lambda=1.5,
            tree_method="hist",
            device="cuda"
        )
        self._limits = [(0.0, 1.0)] * 4
        self._is_trained = False

    def calc(self, input: ICandleSeries) -> RandomForestPredictorValue:
        df = self._transform_candles_to_dataframe(input)
        if df.empty:
            raise ValueError("Input series is empty or invalid")

        feat = self._generate_features(df, ticker_id=None)
        if len(feat) < self._lookback_window:
            raise ValueError("Not enough candles for prediction")

        feature_columns = ['close', 'close_lag1', 'close_lag2', 'close_lag3',
                           'pct_change', 'volatility', 'sma5', 'vol_ratio', 'bullish']

        if 'ticker_id' in feat.columns:
            feature_columns = feature_columns + ['ticker_id']

        last_seq = feat[feature_columns].iloc[-self._lookback_window:]
        features = np.array([last_seq.values.flatten()])

        if not self._is_trained:
            raise ValueError("Model is not trained")

        pred = self._model.predict(features)
        result = [float(v) for v in pred[0]]

        lastCandle = input.getByIndex(input.getCount() - 1)
        if not lastCandle:
            raise ValueError("Cannot extract last candle from meta")

        timestamp = lastCandle.getOpenTimestamp() + lastCandle.getInterval().value
        meta = PredictionMeta(timestamp, input, 0.5)

        return RandomForestPredictorValue(meta, result, self._limits)
    
    def _prepare_sequences(self, featured_df: pd.DataFrame):
        if featured_df is None or featured_df.empty:
            return None, None

        feature_columns = ['close', 'close_lag1', 'close_lag2', 'close_lag3',
                           'pct_change', 'volatility', 'sma5', 'vol_ratio', 'bullish']

        if 'ticker_id' in featured_df.columns:
            feature_columns = feature_columns + ['ticker_id']

        target_columns = ['next_open_return', 'next_close_return', 'next_high_return', 'next_low_return']

        if not all(col in featured_df.columns for col in feature_columns + target_columns):
            return None, None

        features = []
        targets = []

        for i in range(self._lookback_window, len(featured_df)):
            seq = featured_df[feature_columns].iloc[i - self._lookback_window:i]
            features.append(seq.values)
            targets.append(featured_df[target_columns].iloc[i].values.astype(float))

        if len(features) == 0:
            return None, None

        return np.array(features), np.array(targets)

    def train(self, dataset: list[ICandleSeries], test_size: float = 0.3) -> None:
        if not dataset:
            raise ValueError("Dataset is empty")

        # try:
        #     self._initNormalization(dataset)
        # except Exception:
        #     pass

        feature_columns = ['close', 'close_lag1', 'close_lag2', 'close_lag3',
                           'pct_change', 'volatility', 'sma5', 'vol_ratio', 'bullish']
        target_columns = ['next_open_return', 'next_close_return', 'next_high_return', 'next_low_return']

        X_list = []
        y_list = []
        for series in dataset:
            df = self._transform_candles_to_dataframe(series)
            if df.empty:
                continue
            feat = self._generate_features(df)
            if feat.empty:
                continue

            if not all(col in feat.columns for col in feature_columns + target_columns):
                continue
            if len(feat) < self._lookback_window:
                continue

            seq = feat[feature_columns].iloc[-self._lookback_window:]
            target = feat[target_columns].iloc[-1].values.astype(float)
            X_list.append(seq.values)
            y_list.append(target)

        if not X_list:
            raise ValueError("No training samples were created from dataset")

        X = np.array(X_list)
        y = np.array(y_list)
        n_samples, seq_len, n_feats = X.shape
        X_2d = X.reshape(n_samples, seq_len * n_feats)

        X_train, X_test, y_train, y_test = train_test_split(
            X_2d, y, test_size=test_size, random_state=42, shuffle=False
        )

        self._model.fit(X_train, y_train)
        self.last_trained = datetime.now()
        self._is_trained = True


    def _transform_candles_to_dataframe(self, candles):
        # Only accept CandleSeries-like objects (ICandleSeries): getCount/getByIndex
        if not (hasattr(candles, 'getCount') and hasattr(candles, 'getByIndex')):
            return pd.DataFrame()

        try:
            n = int(candles.getCount())
        except Exception:
            return pd.DataFrame()

        datetimes = []
        opens = []
        highs = []
        lows = []
        closes = []
        volumes = []

        def _call_or_attr(obj, method_name, attr_name):
            m = getattr(obj, method_name, None)
            if callable(m):
                try:
                    return m()
                except Exception:
                    return None
            return getattr(obj, attr_name, None)

        for i in range(n):
            candle = candles.getByIndex(i)
            if candle is None:
                continue

            ts = _call_or_attr(candle, 'getOpenTimestamp', 'datetime')
            o = _call_or_attr(candle, 'getOpenPrice', 'open')
            h = _call_or_attr(candle, 'getHighPrice', 'high')
            l = _call_or_attr(candle, 'getLowPrice', 'low')
            c = _call_or_attr(candle, 'getClosePrice', 'close')
            v = _call_or_attr(candle, 'getVolume', 'volume')

            datetimes.append(ts)
            opens.append(o)
            highs.append(h)
            lows.append(l)
            closes.append(c)
            volumes.append(v)

        df = pd.DataFrame({
            'datetime': datetimes,
            'open': opens,
            'high': highs,
            'low': lows,
            'close': closes,
            'volume': volumes,
        })

        if 'datetime' in df.columns:
            try:
                df['datetime'] = pd.to_datetime(df['datetime'], unit='s', errors='coerce')
                df.set_index('datetime', inplace=True)
            except Exception:
                pass

        return df

    def _generate_features(self, df: pd.DataFrame, ticker_id: Optional[int] = None):
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

        data['next_open'] = data['open'].shift(-1)
        data['next_close'] = data['close'].shift(-1)
        data['next_high'] = data['high'].shift(-1)
        data['next_low'] = data['low'].shift(-1)
        data['next_close_return'] = (data['next_close'] - data['close']) / data['close']
        data['next_open_return'] = (data['next_open'] - data['open']) / data['open']
        data['next_high_return'] = (data['next_high'] - data['high']) / data['high']
        data['next_low_return'] = (data['next_low'] - data['low']) / data['low']
        data['trend'] = np.arange(len(data))
        data.dropna(inplace=True)

        if ticker_id is not None:
            data['ticker_id'] = ticker_id

        return data

    def _initNormalization(self, dataset: list[ICandleSeries]):
        for item in dataset:
            # item is expected to be CandleSeries-like
            if not (hasattr(item, 'getCount') and hasattr(item, 'getByIndex')):
                continue
            try:
                cnt = int(item.getCount())
            except Exception:
                continue

            for i in range(cnt):
                candle = item.getByIndex(i)
                if not candle:
                    continue

                def _val(obj, method_name, attr_name):
                    m = getattr(obj, method_name, None)
                    if callable(m):
                        try:
                            return m()
                        except Exception:
                            return None
                    return getattr(obj, attr_name, None)

                o = _val(candle, 'getOpenPrice', 'open')
                c = _val(candle, 'getClosePrice', 'close')
                h = _val(candle, 'getHighPrice', 'high')
                l = _val(candle, 'getLowPrice', 'low')

                if o is not None:
                    try:
                        fo = float(o)  # type: ignore[arg-type]
                        self._limits[0] = (min(fo, self._limits[0][0]), max(fo, self._limits[0][1]))
                    except Exception:
                        pass
                if c is not None:
                    try:
                        fc = float(c)  # type: ignore[arg-type]
                        self._limits[1] = (min(fc, self._limits[1][0]), max(fc, self._limits[1][1]))
                    except Exception:
                        pass
                if h is not None:
                    try:
                        fh = float(h)  # type: ignore[arg-type]
                        self._limits[2] = (min(fh, self._limits[2][0]), max(fh, self._limits[2][1]))
                    except Exception:
                        pass
                if l is not None:
                    try:
                        fl = float(l)  # type: ignore[arg-type]
                        self._limits[3] = (min(fl, self._limits[3][0]), max(fl, self._limits[3][1]))
                    except Exception:
                        pass
