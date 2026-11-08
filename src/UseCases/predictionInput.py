from Entities import TrimmedCandleSeries
from Entities.CandleSeries.OpenCandleSeries import OpenCandleSeries


def prediction_input(predictor, series, index):
    if predictor.usesCurrentOpen():
        return OpenCandleSeries.atIndex(series, index, predictor.getCandlesCount())
    return TrimmedCandleSeries(series, index - predictor.getCandlesCount(), index)
