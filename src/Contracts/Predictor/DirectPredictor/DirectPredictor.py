from Entities import INextCandlePrediction
from .DirectPredictorValue import DirectPredictorValue
from .DirectPredictorAdapter import DirectPredictorAdapter
from .DirectPredictorAlgo import DirectPredictorAlgo
from ..APredictor import APredictor
from Interfaces import IDataSource

class DirectPredictor(APredictor[DirectPredictorValue, INextCandlePrediction]):

    def __init__(self, dataSource: IDataSource):
        super().__init__(DirectPredictorAlgo(dataSource.getSeries()), DirectPredictorAdapter(), 1)
