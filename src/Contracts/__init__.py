"""Public exports loaded on demand, keeping optional ML/plotting dependencies optional."""
from importlib import import_module

_EXPORTS = {
    'SeriesDataSource': ('.DataSource.SeriesDataSource', 'SeriesDataSource'),
    'APredictor': ('.Predictor.APredictor', 'APredictor'),
    'MockDataSource': ('.DataSource', 'MockDataSource'),
    'LoggedDataSource': ('.DataSource', 'LoggedDataSource'),
    'MoexCurrencyDataSource': ('.DataSource', 'MoexCurrencyDataSource'),
    'DummyPredictor': ('.Predictor', 'DummyPredictor'),
    'LoggedPredictor': ('.Predictor', 'LoggedPredictor'),
    'IndicatorPredictor': ('.Predictor', 'IndicatorPredictor'),
    'SimpleMAPredictorAlgo': ('.Predictor', 'SimpleMAPredictorAlgo'),
    'WeightedMAPredictorAlgo': ('.Predictor', 'WeightedMAPredictorAlgo'),
    'ExponentialMAPredictorAlgo': ('.Predictor', 'ExponentialMAPredictorAlgo'),
    'MACDPredictorAlgo': ('.Predictor', 'MACDPredictorAlgo'),
    'RSIPredictorAlgo': ('.Predictor', 'RSIPredictorAlgo'),
    'BollingerBandsPredictorAlgo': ('.Predictor', 'BollingerBandsPredictorAlgo'),
    'AbsolutePerceptronPredictor': ('.Predictor', 'AbsolutePerceptronPredictor'),
    'RelativeMLPredictor': ('.Predictor', 'RelativeMLPredictor'),
    'ITrainablePredictor': ('.Predictor', 'ITrainablePredictor'),
    'PercentageMLPredictor': ('.Predictor', 'PercentageMLPredictor'),
    'CompositeAvgPredictor': ('.Predictor', 'CompositeAvgPredictor'),
    'CompositeVotingPredictor': ('.Predictor', 'CompositeVotingPredictor'),
    'CompositeMedianPredictor': ('.Predictor', 'CompositeMedianPredictor'),
    'RandomForestPredictor': ('.Predictor', 'RandomForestPredictor'),
    'PercentageDeltaMLPredictor': ('.Predictor', 'PercentageDeltaMLPredictor'),
    'DirectPredictor': ('.Predictor', 'DirectPredictor'),
    'DummyStrategy': ('.Strategy', 'DummyStrategy'),
    'LoggedStrategy': ('.Strategy', 'LoggedStrategy'),
    'DirectStrategy': ('.Strategy', 'DirectStrategy'),
    'HalfInAssessor': ('.Assessor', 'HalfInAssessor'),
    'LoggedAssessor': ('.Assessor', 'LoggedAssessor'),
    'CandleTestAssesor': ('.Assessor', 'CandleTestAssesor'),
    'VolatilityThresholdAssessor': ('.Assessor', 'VolatilityThresholdAssessor'),
    'TrendFilterAssessor': ('.Assessor', 'TrendFilterAssessor'),
    'RSIFilterAssessor': ('.Assessor', 'RSIFilterAssessor'),
    'VolatilityCorridorAssessor': ('.Assessor', 'VolatilityCorridorAssessor'),
    'RSICorridorAssessor': ('.Assessor', 'RSICorridorAssessor'),
    'TripleGuardAssessor': ('.Assessor', 'TripleGuardAssessor'),
    'MockContextProvider': ('.ContextProvider', 'MockContextProvider'),
    'LoggedContextProvider': ('.ContextProvider', 'LoggedContextProvider'),
    'DatasetContextProvider': ('.ContextProvider', 'DatasetContextProvider'),
    'BackgroundPollingExecutor': ('.Executor', 'BackgroundPollingExecutor'),
    'LoggedExecutor': ('.Executor', 'LoggedExecutor'),
    'EWMAClosePredictor': ('.Predictor.OpenClosePredictor', 'EWMAClosePredictor'),
    'RidgeClosePredictor': ('.Predictor.OpenClosePredictor', 'RidgeClosePredictor'),
    'OpenCloseStrategy': ('.Strategy.OpenCloseStrategy', 'OpenCloseStrategy'),
    'CorridorAssessor': ('.Assessor.CorridorAssessor', 'CorridorAssessor'),
    'HistoricalExecutor': ('.Executor.HistoricalExecutor', 'HistoricalExecutor'),
}

__all__ = list(_EXPORTS)


def __getattr__(name):
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module, attribute = _EXPORTS[name]
    value = getattr(import_module(module, __name__), attribute)
    globals()[name] = value
    return value
