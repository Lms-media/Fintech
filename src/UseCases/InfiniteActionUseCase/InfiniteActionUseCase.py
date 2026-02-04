import time
from typing import Generic, TypeVar
from Interfaces import IPrediction, ISignal, IAction
from UseCases import IUseCase
from Factories import IFactory

P = TypeVar('P', bound=IPrediction)
S = TypeVar('S', bound=ISignal)
A = TypeVar('A', bound=IAction)

class InfiniteActionUseCase(IUseCase, Generic[P, S, A]):
    _factory: IFactory[P, S, A]
    _delay: int

    def __init__(self, factory: IFactory[P, S, A], delay: int):
        self._factory = factory
        self._delay = delay

    def execute(self) -> None:
        while True:
            series = self._factory.getDataSource().getSeries()
            prediction = self._factory.getPredictor().predict(series)
            strategy = self._factory.getStrategy().getSignal(prediction)
            action = self._factory.getAssessor().getAction(strategy)

            self._factory.getExecutor().start(action)

            time.sleep(self._delay)
