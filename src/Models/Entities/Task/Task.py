from Interfaces import ITask, ITaskTrigger, TaskStatus, TaskType

class Task(ITask):
    _status: TaskStatus
    _type: TaskType
    _trigger: ITaskTrigger

    def __init__(self, type: TaskType, trigger: ITaskTrigger):
        self._status = TaskStatus.Locked
        self._type = type
        self._trigger = trigger

    def getStatus(self) -> TaskStatus:
        return self._status

    def getType(self) -> TaskType:
        return self._type

    def getTrigger(self) -> ITaskTrigger:
        return self._trigger

    def unlock(self) -> None:
        if not self._status == TaskStatus.Locked:
            raise ValueError(f"Task is already unlocked")

    def finish(self) -> None:
        if self._status == TaskStatus.Locked:
            raise ValueError(f"Unable to finish locked task")

        if self._status == TaskStatus.Finished:
            raise ValueError(f"Task is already finished")

        self._status == TaskStatus.Finished
