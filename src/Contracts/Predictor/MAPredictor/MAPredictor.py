from Entities import INextCandlePrediction
from .MAPredictorValue import MAPredictorValue
from .MAPredictorAdapter import MAPredictorAdapter
from .WeightedMAPredictorAlgo import WeightedMAPredictorAlgo
from .SimpleMAPredictorAlgo import SimpleMAPredictorAlgo
from .ExponentialMAPredictorAlgo import ExponentialMAPredictorAlgo
from ..APredictor import APredictor

class MAPredictor(APredictor[MAPredictorValue, INextCandlePrediction]):

    def __init__(self, candlesCount: int):
        super().__init__(ExponentialMAPredictorAlgo(candlesCount), MAPredictorAdapter(), candlesCount)
