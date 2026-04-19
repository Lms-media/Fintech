from enum import Enum

class TaskType(Enum):
    Buy = 1
    Sell = 2
    Idle = 3
    BuyWithLimit = 4
    SellWithLimit = 4
