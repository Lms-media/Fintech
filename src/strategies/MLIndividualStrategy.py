from src.strategies.Strategy import Strategy
from src.data.DataSource import DataSource
from src.data.Candle import Candle
from src.managers.TrainingManagers.IndividualTrainingManager import InvidualTrainingManager
import random
import pandas as  pd
import joblib

class MLIndividualStrategy(Strategy):
    def __init__(self, chunkSize: int, ticker:str, datasource:DataSource , modelPath:str):
        self.lookback_window = chunkSize
        self.ticker = ticker
        self.datasource = datasource
        self.model = self.load_model(modelPath)  

    def predict(self, chunk: list[Candle]) -> float:
        df = InvidualTrainingManager.transform_candles_to_dataframe(chunk)
        if df.empty:
            return None
        feat = InvidualTrainingManager.generate_features(df, ticker_id=0)
        if len(feat) < self.lookback_window:
            return None
        last_seq = feat[['close', 'close_lag1', 'close_lag2', 'close_lag3',
                         'pct_change', 'volatility', 'sma5', 'vol_ratio', 'bullish']].iloc[-self.lookback_window:]
        if 'ticker_id' in feat.columns:
            last_seq = pd.concat([last_seq, feat[['ticker_id']].iloc[-self.lookback_window:]], axis=1)
        X = last_seq.values.reshape(1, -1)
        if self.model is None:
            return None
        predict_price =  self.model.predict(X)[0]
        cur_price = chunk[-1].close
        if cur_price < predict_price:
            return 1
        else:
            return -1 
    def load_model(self, filename):
        try:
            save_data = joblib.load(filename)
            model = save_data.get('model')
            print(f"Модель загружена из {filename}")
            return model
        except Exception as e:
            print(f"Ошибка загрузки: {str(e)}")
            return False
