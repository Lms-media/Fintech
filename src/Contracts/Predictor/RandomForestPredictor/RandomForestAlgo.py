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
    _target_bounds: list[tuple[float, float]]
    _blend_weights: list[float]
    _target_bias: list[float]
    _model_params: dict
    _vol_blend_intercept: float
    _vol_blend_slope: float

    def __init__(self, candlesCount: int, lookback_window: int = 10, **rf_params):
        self._candlesCount = candlesCount
        self._lookback_window = min(max(2, lookback_window), candlesCount)
        self._model_params = rf_params if rf_params else {
            "n_estimators": 1000,
            "max_depth": 5,
            "learning_rate": 0.03,
            "subsample": 0.85,
            "colsample_bytree": 0.85,
            "min_child_weight": 6,
            "gamma": 0.2,
            "reg_lambda": 3.0,
            "objective": "reg:absoluteerror",
            "random_state": 42,
            "tree_method": "hist",
            "device": "cpu",
        }
        self._model = XGBRegressor(**self._model_params)
        self._limits = [(0.0, 1.0)] * 4
        self._target_bounds = [(-0.05, 0.05)] * 4
        self._blend_weights = [1.0] * 4
        self._target_bias = [0.0] * 4
        self._vol_blend_intercept = 1.0
        self._vol_blend_slope = 0.0
        self._is_trained = False

    def calc(self, input: ICandleSeries) -> RandomForestPredictorValue:
        df = self._transform_candles_to_dataframe(input)
        if df.empty:
            raise ValueError("Input series is empty or invalid")

        feat = self._generate_features(df, ticker_id=None, include_targets=False)
        if len(feat) < self._lookback_window:
            raise ValueError("Not enough candles for prediction")

        feature_columns = self._feature_columns('ticker_id' in feat.columns)

        last_seq = feat[feature_columns].iloc[-self._lookback_window:]
        features = np.array([last_seq.values.flatten()])

        if not self._is_trained:
            raise ValueError("Model is not trained")

        pred = self._model.predict(features)
        raw_result = [float(v) for v in pred[0]]
        regime_weight = self._calc_regime_weight_from_feature_row(last_seq.iloc[-1])
        calibrated = [
            (self._blend_weights[i] * raw_result[i] * regime_weight) + self._target_bias[i]
            for i in range(4)
        ]
        result = [
            float(np.clip(v, lo, hi))
            for v, (lo, hi) in zip(calibrated, self._target_bounds)
        ]

        lastCandle = input.getByIndex(input.getCount() - 1)
        if not lastCandle:
            raise ValueError("Cannot extract last candle from meta")

        timestamp = lastCandle.getOpenTimestamp() + lastCandle.getInterval().value
        meta = PredictionMeta(timestamp, input, 0.5)

        return RandomForestPredictorValue(meta, result, self._limits)
    
    def _prepare_sequences(self, featured_df: pd.DataFrame):
        if featured_df is None or featured_df.empty:
            return None, None

        feature_columns = self._feature_columns('ticker_id' in featured_df.columns)
        target_columns = self._target_columns()

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

        feature_columns = self._feature_columns(False)
        target_columns = self._target_columns()

        X_list = []
        y_list = []
        for series in dataset:
            df = self._transform_candles_to_dataframe(series)
            if df.empty:
                continue
            feat = self._generate_features(df, include_targets=True)
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

        train_q_low = np.quantile(y_train, 0.005, axis=0)
        train_q_high = np.quantile(y_train, 0.995, axis=0)
        self._target_bounds = [
            (float(lo), float(hi))
            for lo, hi in zip(train_q_low, train_q_high)
        ]
        y_train = np.clip(y_train, train_q_low, train_q_high)

        sample_weight = np.geomspace(0.3, 1.7, len(X_train))
        self._model = self._fit_best_model(X_train, y_train, X_test, y_test, sample_weight)

        y_val_pred = self._model.predict(X_test)
        self._fit_output_calibration(y_val_pred, y_test, X_test)

        self.last_trained = datetime.now()
        self._is_trained = True


    def _transform_candles_to_dataframe(self, candles):
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

    def _feature_columns(self, has_ticker_id: bool) -> list[str]:
        columns = [
            'open', 'high', 'low', 'close',
            'close_lag1', 'close_lag2', 'close_lag3',
            'pct_change', 'volatility', 'volatility10',
            'sma5', 'ema5', 'ema10', 'ema_gap',
            'oc_return', 'body', 'range', 'range_ma5', 'range_ratio',
            'momentum3', 'vol_ratio', 'bullish'
        ]
        if has_ticker_id:
            columns.append('ticker_id')
        return columns

    def _target_columns(self) -> list[str]:
        return ['next_open_return', 'next_close_return', 'next_high_return', 'next_low_return']

    def _calc_regime_weight_from_feature_row(self, row: pd.Series) -> float:
        rr = row.get('range_ratio', 1.0)
        try:
            rr_f = float(rr)
        except Exception:
            rr_f = 1.0
        if not np.isfinite(rr_f):
            rr_f = 1.0
        weight = self._vol_blend_intercept + (self._vol_blend_slope * rr_f)
        return float(np.clip(weight, 0.15, 1.0))

    def _calc_regime_weights_from_matrix(self, x_flat: np.ndarray) -> np.ndarray:
        feature_columns = self._feature_columns(False)
        n_feats = len(feature_columns)
        seq_last_idx = (self._lookback_window - 1) * n_feats
        rr_idx = feature_columns.index('range_ratio')
        rr = x_flat[:, seq_last_idx + rr_idx]
        rr = np.where(np.isfinite(rr), rr, 1.0)
        weights = self._vol_blend_intercept + (self._vol_blend_slope * rr)
        return np.clip(weights, 0.15, 1.0)

    def _score_price_sse(self, y_pred: np.ndarray, y_true: np.ndarray, x_flat: np.ndarray) -> float:
        if len(y_pred) == 0:
            return float("inf")
        n_feats = len(self._feature_columns(False))
        seq_last_idx = (self._lookback_window - 1) * n_feats
        o = x_flat[:, seq_last_idx + 0]
        h = x_flat[:, seq_last_idx + 1]
        l = x_flat[:, seq_last_idx + 2]
        c = x_flat[:, seq_last_idx + 3]

        pred_o = o * (1.0 + y_pred[:, 0])
        pred_c = c * (1.0 + y_pred[:, 1])
        pred_h = h * (1.0 + y_pred[:, 2])
        pred_l = l * (1.0 + y_pred[:, 3])

        pred_h = np.maximum.reduce([pred_o, pred_c, pred_h, pred_l])
        pred_l = np.minimum.reduce([pred_o, pred_c, pred_h, pred_l])

        true_o = o * (1.0 + y_true[:, 0])
        true_c = c * (1.0 + y_true[:, 1])
        true_h = h * (1.0 + y_true[:, 2])
        true_l = l * (1.0 + y_true[:, 3])

        err = ((pred_o - true_o) ** 2 +
               (pred_c - true_c) ** 2 +
               (pred_h - true_h) ** 2 +
               (pred_l - true_l) ** 2)
        return float(np.mean(err))

    def _fit_best_model(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_test: np.ndarray,
        y_test: np.ndarray,
        sample_weight: np.ndarray
    ) -> XGBRegressor:
        if len(X_test) == 0:
            model = XGBRegressor(**self._model_params)
            model.fit(X_train, y_train, sample_weight=sample_weight)
            return model

        base = dict(self._model_params)
        candidates = [
            {},
            {"max_depth": 4, "learning_rate": 0.04, "min_child_weight": 4, "gamma": 0.1, "n_estimators": 650},
            {"max_depth": 3, "learning_rate": 0.05, "min_child_weight": 8, "gamma": 0.3, "n_estimators": 750},
            {"max_depth": 6, "learning_rate": 0.02, "min_child_weight": 10, "gamma": 0.25, "n_estimators": 1000},
            {"max_depth": 5, "learning_rate": 0.025, "min_child_weight": 6, "gamma": 0.15, "n_estimators": 900},
        ]

        best_model = None
        best_score = float("inf")

        for override in candidates:
            params = dict(base)
            params.update(override)
            model = XGBRegressor(**params)
            model.fit(X_train, y_train, sample_weight=sample_weight)
            pred = model.predict(X_test)
            pred = np.clip(pred, np.array([b[0] for b in self._target_bounds]), np.array([b[1] for b in self._target_bounds]))
            score = self._score_price_sse(pred, y_test, X_test)

            if score < best_score:
                best_score = score
                best_model = model
                self._model_params = params

        if best_model is None:
            raise ValueError("Failed to fit RandomForest/XGBoost model")
        return best_model

    def _fit_output_calibration(self, y_pred: np.ndarray, y_true: np.ndarray, x_test: np.ndarray) -> None:
        if y_pred is None or y_true is None:
            return
        if len(y_pred) == 0 or len(y_true) == 0:
            return

        weights: list[float] = []
        biases: list[float] = []
        grid = np.linspace(0.0, 1.0, 21)

        for i in range(4):
            pred_i = y_pred[:, i]
            true_i = y_true[:, i]

            best_w = 1.0
            best_mse = float("inf")

            for w in grid:
                candidate = pred_i * w
                mse = float(np.mean((candidate - true_i) ** 2))
                if mse < best_mse:
                    best_mse = mse
                    best_w = float(w)

            blended = pred_i * best_w
            bias = float(np.median(true_i - blended))
            weights.append(best_w)
            biases.append(bias)

        self._blend_weights = weights
        self._target_bias = biases

        base = np.zeros_like(y_pred)
        for i in range(4):
            base[:, i] = (y_pred[:, i] * self._blend_weights[i]) + self._target_bias[i]

        best_score = float("inf")
        best_a = 1.0
        best_b = 0.0

        for a in np.linspace(0.6, 1.0, 9):
            for b in np.linspace(-0.45, 0.0, 10):
                self._vol_blend_intercept = float(a)
                self._vol_blend_slope = float(b)
                rw = self._calc_regime_weights_from_matrix(x_test)[:, None]
                candidate = base * rw
                candidate = np.clip(
                    candidate,
                    np.array([bnd[0] for bnd in self._target_bounds]),
                    np.array([bnd[1] for bnd in self._target_bounds])
                )
                score = self._score_price_sse(candidate, y_true, x_test)
                if score < best_score:
                    best_score = score
                    best_a = float(a)
                    best_b = float(b)

        self._vol_blend_intercept = best_a
        self._vol_blend_slope = best_b

    def _generate_features(self, df: pd.DataFrame, ticker_id: Optional[int] = None, include_targets: bool = True):
        data = df.copy()
        if 'datetime' in data.columns:
            data['datetime'] = pd.to_datetime(data['datetime'])
            data.set_index('datetime', inplace=True)

        data['close_lag1'] = data['close'].shift(1)
        data['close_lag2'] = data['close'].shift(2)
        data['close_lag3'] = data['close'].shift(3)

        data['pct_change'] = data['close'].pct_change()

        data['daily_range'] = data['high'] - data['low']
        data['range'] = data['daily_range']
        data['body'] = (data['close'] - data['open']).abs()
        data['oc_return'] = (data['close'] - data['open']) / data['open']
        data['range_ma5'] = data['range'].rolling(5).mean()
        data['range_ratio'] = data['range'] / data['range_ma5']
        data['volatility'] = data['daily_range'].rolling(3).max()
        data['volatility10'] = data['daily_range'].rolling(10).std()

        data['sma5'] = data['close'].rolling(5).mean()
        data['ema5'] = data['close'].ewm(span=5, adjust=False).mean()
        data['ema10'] = data['close'].ewm(span=10, adjust=False).mean()
        data['ema_gap'] = (data['ema5'] - data['ema10']) / data['ema10']
        data['momentum3'] = (data['close'] / data['close_lag3']) - 1.0

        data['vol_ma5'] = data['volume'].rolling(5).mean()
        data['vol_ratio'] = data['volume'] / data['vol_ma5']

        data['bullish'] = (data['close'] > data['open']).astype(int)

        if include_targets:
            data['next_open'] = data['open'].shift(-1)
            data['next_close'] = data['close'].shift(-1)
            data['next_high'] = data['high'].shift(-1)
            data['next_low'] = data['low'].shift(-1)
            data['next_close_return'] = (data['next_close'] - data['close']) / data['close']
            data['next_open_return'] = (data['next_open'] - data['open']) / data['open']
            data['next_high_return'] = (data['next_high'] - data['high']) / data['high']
            data['next_low_return'] = (data['next_low'] - data['low']) / data['low']
        data['trend'] = np.arange(len(data))
        data.replace([np.inf, -np.inf], np.nan, inplace=True)

        required_columns = self._feature_columns(False)
        if include_targets:
            required_columns = required_columns + self._target_columns()
        data.dropna(subset=required_columns, inplace=True)

        if ticker_id is not None:
            data['ticker_id'] = ticker_id

        return data

    def _initNormalization(self, dataset: list[ICandleSeries]):
        for item in dataset:
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
