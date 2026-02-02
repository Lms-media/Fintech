from abc import ABC
from Interfaces import IAction, ISignal, ITask, IExecutionContext, ActionStatus, TaskStatus

class AAction(IAction, ABC):
    _signal: ISignal
    _status: ActionStatus
    _tasks: list[ITask]

    def __init__(self, signal, tasks):
        self._status = ActionStatus.Waiting
        self._signal = signal
        self._tasks = list(tasks)

    def getSignal(self):
        return self._signal

    def getStatus(self):
        return self._status

    def getTasks(self):
        return self._tasks

    def start(self):
        if not self._status == ActionStatus.Waiting:
            raise ValueError(f"Action was already started")

    def finish(self):
        if self._status == ActionStatus.Waiting:
            raise ValueError(f"Unable to finish waiting action")

        if self._status == ActionStatus.Finished:
            raise ValueError(f"Action was already finished")

        self._status = ActionStatus.Finished

    def update(self, context: IExecutionContext) -> None:
        for task in self._tasks:
            if task.getStatus == TaskStatus.Finished:
                continue
            trigger = task.getTrigger()
            if trigger.isTriggered(context):
                task.unlock()
