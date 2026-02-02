from Interfaces import IMarket, ITask

class LogMarket(IMarket):

    def execute(self, task: ITask) -> None:
        print(f"Executing task... Type: {task.getType()}, Asset Pair: {task.getAssetPair()}, Lot Count: {task.getLotCount()}")
