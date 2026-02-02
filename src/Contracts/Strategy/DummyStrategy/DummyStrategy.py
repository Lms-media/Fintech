from Interfaces import IStrategy, DirectionType
from Entities import INextCandlePrediction, IDirectionSignal, DirectionSignal

class DummyStrategy(IStrategy[INextCandlePrediction, IDirectionSignal]):
    def getSignal(self, input: INextCandlePrediction) -> IDirectionSignal:
        nextCandle = input.getNextCandle()
        timestamp = nextCandle.getOpenPrice()
        if nextCandle.getOpenPrice() < nextCandle.getClosePrice():
            return DirectionSignal(timestamp, input, 1, DirectionType.Up)
        else:
            return DirectionSignal(timestamp, input, 1, DirectionType.Down)
