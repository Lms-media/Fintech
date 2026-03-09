from Entities import INextCandlePrediction
from .RandomForestPredictorValue import RandomForestPredictorValue
from .RandomForestAlgo import RandomForestAlgo
from .RandomForestAdapter import RandomForestPredictorAdapter
from ..ATrainablePredictor import ATrainablePredictor

import pandas as pd
import numpy as np
import joblib
# from sklearn.ensemble import RandomForestRegressor
# from sklearn.metrics import mean_absolute_error, r2_score
# from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from typing import Optional


class RandomForestPredictor(ATrainablePredictor[RandomForestPredictorValue, INextCandlePrediction]):
    def __init__(self, candlesCount: int, **rf_params):
        algo = RandomForestAlgo(candlesCount, **rf_params)
        adapter = RandomForestPredictorAdapter()
        super().__init__(algo, adapter, candlesCount)

        self._sk_model: Optional[XGBRegressor] = algo._model if hasattr(algo, '_model') else None
        self.featured_data = pd.DataFrame()
        self.data = {}
