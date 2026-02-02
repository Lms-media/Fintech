from typing import Generic, TypeVar
from Interfaces import IDataSource, IPredictor, IStrategy, IAssessor, IExecutor, IPrediction, ISignal, IAction
from UseCases import IUseCase

P = TypeVar('P', bound=IPrediction)
S = TypeVar('S', bound=ISignal)
A = TypeVar('A', bound=IAction)

class SingleActionUseCase(IUseCase, Generic[P, S, A]):
    _dataSource: IDataSource
    _predictor: IPredictor[P]
    _strategy: IStrategy[P, S]
    _assessor: IAssessor[S, A]
    _executor: IExecutor

    def __init__(self, dataSource: IDataSource, predictor: IPredictor, strategy: IStrategy, assessor: IAssessor, executor: IExecutor):
        self._dataSource = dataSource
        self._predictor = predictor
        self._strategy = strategy
        self._assessor = assessor
        self._executor = executor

    def execute(self) -> None:
        series = self._dataSource.getSeries()
        prediction = self._predictor.predict(series)
        strategy = self._strategy.getSignal(prediction)
        action = self._assessor.getAction(strategy)

        self._executor.start(action)
