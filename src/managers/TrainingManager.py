import pandas as pd
from src.data.DataSource import DataSource
from src.strategies.Strategy import Strategy
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from datetime import datetime, timedelta
import joblib
class TrainingManager:
    def __init__(
        self,
        instrumentsData: dict[str, DataSource],
        instruments: dict[str, Strategy],
        chunkSize: int,
        isStarted: bool
    ):
        self.instrumentsData = instrumentsData
        self.instruments = instruments
        self.chunkSize = chunkSize
        self.isStarted = False
    def getChunk(self, number: int):
        pass
         
        