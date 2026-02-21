from Interfaces import IPredictorAlgo, ICandleSeries, IDataSource, ICandle
from .DirectPredictorValue import DirectPredictorValue
from Entities import PredictionMeta, TrimmedCandleSeries, CandleSeries


class DirectPredictorAlgo(IPredictorAlgo[DirectPredictorValue]):
    
    def __init__(self, dataSource: ICandleSeries):
        self._datasource = dataSource
        super().__init__()

    def calc(self, input: ICandleSeries):
        lastCandle = input.getByIndex(input.getCount() - 1)
        if not lastCandle:
            raise ValueError("Cannot extract last candle from input")
        for i in range(0, self._datasource.getCount() - 1):
            datasourceCandle = self._datasource.getByIndex(i)
            nextDatasourceCandle = self._datasource.getByIndex(i + 1)
            if not datasourceCandle:
                raise ValueError("Cannot extract datasource candle")
            if not nextDatasourceCandle:
                raise ValueError("Cannot extract next datasource candle")
            if lastCandle.getOpenTimestamp() == datasourceCandle.getOpenTimestamp():
                return DirectPredictorValue(
                    PredictionMeta(
                        datasourceCandle.getOpenTimestamp(),
                        input,
                        1.0
                    ), 
                    nextDatasourceCandle
                )
        return DirectPredictorValue(
            PredictionMeta(
                lastCandle.getOpenTimestamp(),
                input,
                1.0
            ), 
            lastCandle
        )
