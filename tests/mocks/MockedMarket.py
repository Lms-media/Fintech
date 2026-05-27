from Interfaces import IMarket, ITask

class MockedMarket(IMarket):
    def __init__(self):
        self.executed_tasks = []

    def execute(self, task: ITask) -> None:
        self.executed_tasks.append(task)
