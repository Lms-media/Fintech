from Entities import INextCandlePrediction
from .IndicatorPredictorValue import IndicatorPredictorValue
from .IndicatorPredictorAdapter import IndicatorPredictorAdapter
from .WeightedMAPredictorAlgo import WeightedMAPredictorAlgo
from .SimpleMAPredictorAlgo import SimpleMAPredictorAlgo
from .ExponentialMAPredictorAlgo import ExponentialMAPredictorAlgo
from .MACDPredictorAlgo import MACDPredictorAlgo
from .RSIPredictorAlgo import RSIPredictorAlgo
from .BollingerBandsPredictorAlgo import BollingerBandsPredictorAlgo
from .CompositeAvgIndicatorPredictorAlgo import CompositeAvgIndicatorPredictorAlgo
from ..APredictor import APredictor

class IndicatorPredictor(APredictor[IndicatorPredictorValue, INextCandlePrediction]):

    def __init__(self, candlesCount: int):
        algos = [
            BollingerBandsPredictorAlgo(candlesCount),
            RSIPredictorAlgo(candlesCount),
        ]
        super().__init__(CompositeAvgIndicatorPredictorAlgo(algos), IndicatorPredictorAdapter(), candlesCount)
