from abc import ABC
import uuid
from Interfaces import IAction, ISignal, ITask, IExecutionContext, ActionStatus, TaskStatus

class AAction(IAction, ABC):
    _id: str
    _signal: ISignal
    _status: ActionStatus
    _tasks: list[ITask]

    def __init__(self, signal, tasks):
        self._status = ActionStatus.Waiting
        self._signal = signal
        self._tasks = list(tasks)
        self._id = uuid.uuid4()

    def getId(self) -> str:
        return self._id

    def getSignal(self) -> ISignal:
        return self._signal

    def getStatus(self) -> ActionStatus:
        return self._status

    def getTasks(self) -> list[ITask]:
        return self._tasks

    def start(self) -> None:
        if not self._status == ActionStatus.Waiting:
            raise ValueError(f"Action was already started")

    def finish(self) -> None:
        if self._status == ActionStatus.Waiting:
            raise ValueError(f"Unable to finish waiting action")

        if self._status == ActionStatus.Finished:
            raise ValueError(f"Action was already finished")

        self._status = ActionStatus.Finished

    def update(self, context: IExecutionContext) -> None:
        for task in self._tasks:
            if task.getStatus() == TaskStatus.Finished:
                continue
            trigger = task.getTrigger()
            if trigger.isTriggered(context):
                task.unlock()

    def __str__(self) -> str:
        lines = list([f"🎬 {self.getId()}; Status: {self.getStatus()};"])
        tasks = self.getTasks()

        for i in range(len(tasks)):
            task = tasks[i]
            lines.append(f"{i + 1}. {str(task)}")

        return '\n'.join(lines)
