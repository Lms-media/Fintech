from Entities import INextCandlePrediction
from .RelativePerceptronPredictorValue import RelativePerceptronPredictorValue
from .RelativePerceptronPredictorAdapter import RelativePerceptronPredictorAdapter
from .RelativePerceptronPredictorAlgo import RelativePerceptronPredictorAlgo
from ..ATrainablePredictor import ATrainablePredictor
from ..Interfaces import ITrainablePredictor
from Interfaces import ICandleSeries

class RelativePerceptronPredictor(ATrainablePredictor[RelativePerceptronPredictorValue, INextCandlePrediction], ITrainablePredictor):
    _dataset: list[ICandleSeries]

    def __init__(self, candlesCount: int):
        super().__init__(RelativePerceptronPredictorAlgo(candlesCount), RelativePerceptronPredictorAdapter(), candlesCount)
