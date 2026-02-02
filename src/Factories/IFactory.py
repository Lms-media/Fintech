from abc import ABC, abstractmethod
from typing import TypeVar
from Interfaces import IPrediction, ISignal, IAction, IDataSource, IPredictor, IStrategy, IAssessor, IExecutor, IMarket, IPortfolio, IContextProvider

P = TypeVar('P', bound=IPrediction)
S = TypeVar('S', bound=ISignal)
A = TypeVar('A', bound=IAction)

class IFactory(ABC):

    @abstractmethod
    def getDataSource(self) -> IDataSource:
        pass

    @abstractmethod
    def getPredictor(self) -> IPredictor[P]:
        pass

    @abstractmethod
    def getStrategy(self) -> IStrategy[P, S]:
        pass

    @abstractmethod
    def getAssessor(self) -> IAssessor[S, A]:
        pass

    @abstractmethod
    def getExecutor(self) -> IExecutor:
        pass

    @abstractmethod
    def getMarket(self) -> IMarket:
        pass

    @abstractmethod
    def getPortfolio(self) -> IPortfolio:
        pass

    @abstractmethod
    def getContextProvider(self) -> IContextProvider:
        pass
