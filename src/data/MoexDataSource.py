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
        self.candles: list[Candle] = []
        chunkSize = getChunkSize(interval)
        start = int(dateFrom.timestamp())
        initStart = start
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
                self.candles.append(
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
            self.progress_bar(start - initStart, end - initStart)
        self.size = len(self.candles)

    def progress_bar(self, current, total, bar_length=50):
        fraction = current / total

        arrow = int(fraction * bar_length) * "█"
        padding = (bar_length - len(arrow)) * " "
        ending = "\n" if current == total else "\r"

        if int(fraction * 100) > 100:
            print(f"Download data: [{arrow}{padding}] 100%", end=ending, flush=True)
        else:
            print(
                f"Download data: [{arrow}{padding}] {int(fraction * 100)}%",
                end=ending,
                flush=True,
            )
