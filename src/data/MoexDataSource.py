from src.data.DataSource import DataSource
from datetime import datetime as dt
from src.utils.chunkSize import getChunkSize
from src.utils.fetchMoex import fetchMoex
from src.data.Candle import Candle


class MoexDataSource(DataSource):

    def __init__(
        self, dateFrom: dt, dateTo: dt, interval: int, tickerCode: str, contentType: str
    ):
        self.interval = interval
        self.tickerCode = tickerCode
        self._candles: list[Candle] = []
        chunkSize = getChunkSize(interval)
        start = int(dateFrom.timestamp())
        end = int(dateTo.timestamp())
        dateFormat = "%Y-%m-%d %H:%M:%S"

        while start < end:
            chunk = fetchMoex(
                self.tickerCode,
                start,
                min(start + chunkSize, end),
                interval,
                contentType,
            )
            for candle in chunk:
                self._candles.append(
                    Candle(
                        candle["open"],
                        candle["close"],
                        candle["high"],
                        candle["low"],
                        candle["volume"],
                        dt.strptime(candle["time"], dateFormat),
                        self.interval,
                    )
                )
            start += chunkSize
        self.size = len(self._candles)
