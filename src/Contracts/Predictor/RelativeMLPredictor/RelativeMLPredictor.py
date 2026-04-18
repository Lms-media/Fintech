from Entities import INextCandlePrediction
from .RelativeMLPredictorValue import RelativeMLPredictorValue
from .RelativeMLPredictorAdapter import RelativeMLPredictorAdapter
from .RelativePerceptronPredictorAlgo import RelativePerceptronPredictorAlgo
from .RelativeRNNPredictorAlgo import RelativeRNNPredictorAlgo
from .RelativeLSTMPredictorAlgo import RelativeLSTMPredictorAlgo
from .RelativeMemorizingPredictorAlgo import RelativeMemorizingPredictorAlgo
from ..ATrainablePredictor import ATrainablePredictor
from ..Interfaces import ITrainablePredictor
from Interfaces import ICandleSeries

class RelativeMLPredictor(ATrainablePredictor[RelativeMLPredictorValue, INextCandlePrediction], ITrainablePredictor):
    _dataset: list[ICandleSeries]

    def __init__(self, candlesCount: int):
        super().__init__(RelativePerceptronPredictorAlgo(candlesCount), RelativeMLPredictorAdapter(), candlesCount)
