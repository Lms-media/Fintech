from datetime import datetime
from urllib.parse import urlencode
import requests
from ValueObjects import Candle
from Entities import CandleSeries
from Interfaces import IDataSource, ICandleSeries, IAssetPair, IntervalType

class MoexCurrencyDataSource(IDataSource):
    _assetPair: IAssetPair
    _series: ICandleSeries
    _tickerCode: str
    _fromTimestamp: int
    _toTimestamp: int
    _interval: IntervalType
    _intervalMapping: dict[IntervalType, str] = {
        IntervalType.OneMinute: "1",
        IntervalType.OneHour: "60",
        IntervalType.OneDay: "24",
        IntervalType.OneWeek: "7",
    }

    def __init__(self, assetPair: IAssetPair, tickerCode: str, fromTimestamp: int, toTimestamp: int, interval: IntervalType):
        if interval not in self._intervalMapping:
            supported = list(map(str, list(self._intervalMapping.keys())))
            raise ValueError(f"Only supported interval types: {', '.join(supported)}, but interval type is {interval}")

        self._assetPair = assetPair
        self._series = CandleSeries(assetPair)
        self._tickerCode = tickerCode
        self._fromTimestamp = fromTimestamp
        self._toTimestamp = toTimestamp
        self._interval = interval

    def init(self) -> None:
        currentTimestamp = self._fromTimestamp
        data: list[dict] = list()
        while(currentTimestamp < self._toTimestamp):
            currentData = self._fetchMoex(currentTimestamp, self._toTimestamp)
            if len(currentData) == 0:
                break

            data.extend(currentData)
            lastOpenDate = data[len(data) - 1]['TRADEDATE']
            currentTimestamp = int(datetime.strptime(lastOpenDate, '%Y-%m-%d').timestamp()) + self._interval.value

        self._series = CandleSeries(self._assetPair)

        for item in data:
            assetPair = self._assetPair
            openTimestamp = int(datetime.strptime(item['TRADEDATE'], '%Y-%m-%d').timestamp())
            openPrice = item['OPEN']
            closePrice = item['CLOSE']
            lowPrice = item['LOW']
            highPrice = item['HIGH']
            volume = item['VOLRUR']

            if openPrice == 0 or closePrice == 0 or openPrice is None or closePrice is None:
                continue

            candle = Candle(assetPair, openTimestamp, self._interval, openPrice, closePrice, highPrice, lowPrice, volume)
            self._series.appendRight(candle)

    def _fetchMoex(self, fromTimestamp: int, toTimestamp: int) -> list[dict]:
        baseUrl = f"https://iss.moex.com/iss/history/engines/currency/markets/selt/boards/cets/securities/{self._tickerCode}.json"
        params = {
            'iss.meta': 'off',
            'iss.only': 'history',
            'interval': self._intervalMapping[self._interval],
            'from': datetime.fromtimestamp(fromTimestamp).strftime("%Y-%m-%d"),
            'till': datetime.fromtimestamp(toTimestamp).strftime("%Y-%m-%d"),
        }
        url = f"{baseUrl}?{urlencode(params)}"

        response = requests.get(url)

        if response.status_code != 200:
            raise ValueError(f"Error fetching MOEX: {response.status_code}")

        json: dict = response.json()
        columns: list[str] = json['history']['columns']
        data: list[list]= json['history']['data']
        result = list[dict]()

        for row in data:
            item = dict()
            for i in range(len(columns)):
                column = columns[i]
                value = row[i]

                item[column] = value
            result.append(item)

        return result

    def getSeries(self):
        return self._series
