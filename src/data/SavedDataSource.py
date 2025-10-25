from src.data.DataSource import DataSource
from datetime import datetime as dt
from src.utils.chunkSize import getChunkSize
from src.utils.fetchMoex import fetchMoex
from src.data.Candle import Candle
import json
from typing import List


class SavedDataSource(DataSource):

    def __init__(self, interval: int, tickerCode: str, filename: str):
        self.interval = interval
        self.tickerCode = tickerCode
        self.candles = self.candleListFromJson(filename)
        self.size = len(self.candles)

    def candleListFromJson(self, filename: str) -> List[Candle]:
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)

        for item in data:
            item["datetime"] = dt.fromisoformat(item["datetime"])

        return [Candle(**item) for item in data]
