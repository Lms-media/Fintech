from Entities import INextCandlePrediction
from .PercentageMLPredictorValue import PercentageMLPredictorValue
from .PercentageMLPredictorAdapter import PercentageMLPredictorAdapter
from .PercentagePerceptronPredictorAlgo import PercentagePerceptronPredictorAlgo
from .PercentageRNNPredictorAlgo import PercentageRNNPredictorAlgo
from .PercentageLSTMPredictorAlgo import PercentageLSTMPredictorAlgo
from ..ATrainablePredictor import ATrainablePredictor
from ..Interfaces import ITrainablePredictor
from Interfaces import ICandleSeries

class PercentageMLPredictor(ATrainablePredictor[PercentageMLPredictorValue, INextCandlePrediction], ITrainablePredictor):
    _dataset: list[ICandleSeries]

    def __init__(self, candlesCount: int):
        super().__init__(PercentageLSTMPredictorAlgo(candlesCount), PercentageMLPredictorAdapter(), candlesCount)
