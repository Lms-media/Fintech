"""Public exports loaded on demand, keeping optional ML/plotting dependencies optional."""
from importlib import import_module

_EXPORTS = {
    'DummyPredictor': ('.DummyPredictor', 'DummyPredictor'),
    'LoggedPredictor': ('.LoggedPredictor', 'LoggedPredictor'),
    'APredictor': ('.APredictor', 'APredictor'),
    'IndicatorPredictor': ('.IndicatorPredictor', 'IndicatorPredictor'),
    'SimpleMAPredictorAlgo': ('.IndicatorPredictor', 'SimpleMAPredictorAlgo'),
    'WeightedMAPredictorAlgo': ('.IndicatorPredictor', 'WeightedMAPredictorAlgo'),
    'ExponentialMAPredictorAlgo': ('.IndicatorPredictor', 'ExponentialMAPredictorAlgo'),
    'MACDPredictorAlgo': ('.IndicatorPredictor', 'MACDPredictorAlgo'),
    'RSIPredictorAlgo': ('.IndicatorPredictor', 'RSIPredictorAlgo'),
    'BollingerBandsPredictorAlgo': ('.IndicatorPredictor', 'BollingerBandsPredictorAlgo'),
    'AbsolutePerceptronPredictor': ('.AbsolutePerceptronPredictor', 'AbsolutePerceptronPredictor'),
    'RelativeMLPredictor': ('.RelativeMLPredictor', 'RelativeMLPredictor'),
    'PercentageMLPredictor': ('.PercentageMLPredictor', 'PercentageMLPredictor'),
    'CompositeAvgPredictor': ('.CompositePredictor', 'CompositeAvgPredictor'),
    'CompositeVotingPredictor': ('.CompositePredictor', 'CompositeVotingPredictor'),
    'CompositeMedianPredictor': ('.CompositePredictor', 'CompositeMedianPredictor'),
    'ITrainablePredictor': ('.Interfaces', 'ITrainablePredictor'),
    'ITrainablePredictorAlgo': ('.Interfaces', 'ITrainablePredictorAlgo'),
    'RandomForestPredictor': ('.RandomForestPredictor.RandomForestPredictor', 'RandomForestPredictor'),
    'PercentageDeltaMLPredictor': ('.PercentageDeltaMLPredictor', 'PercentageDeltaMLPredictor'),
    'DirectPredictor': ('.DirectPredictor', 'DirectPredictor'),
    'EWMAClosePredictor': ('.OpenClosePredictor', 'EWMAClosePredictor'),
    'RidgeClosePredictor': ('.OpenClosePredictor', 'RidgeClosePredictor'),
}

__all__ = list(_EXPORTS)


def __getattr__(name):
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module, attribute = _EXPORTS[name]
    value = getattr(import_module(module, __name__), attribute)
    globals()[name] = value
    return value

# Preserve class exports when a lightweight child package is imported directly.
for _name in ['APredictor', 'DummyPredictor', 'LoggedPredictor', 'IndicatorPredictor', 'SimpleMAPredictorAlgo', 'WeightedMAPredictorAlgo', 'ExponentialMAPredictorAlgo', 'MACDPredictorAlgo', 'RSIPredictorAlgo', 'BollingerBandsPredictorAlgo', 'CompositeAvgPredictor', 'CompositeVotingPredictor', 'CompositeMedianPredictor', 'ITrainablePredictor', 'ITrainablePredictorAlgo', 'DirectPredictor']:
    globals()[_name] = __getattr__(_name)
