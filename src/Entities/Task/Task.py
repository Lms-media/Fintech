import uuid
from Interfaces import ITask, ITaskTrigger, IAssetPair, TaskStatus, TaskType

class Task(ITask):
    _id: str
    _status: TaskStatus
    _type: TaskType
    _trigger: ITaskTrigger
    _assetPair: IAssetPair
    _lotCount: int
    _timestamp: int

    def __init__(self, type: TaskType, assetPair:IAssetPair, lotCount: int, trigger: ITaskTrigger, timestamp: int):
        if lotCount < 0:
            raise ValueError(f"'lotCount' must be greater than or equal zero, but 'lotCount' is {lotCount}")

        self._status = TaskStatus.Locked
        self._type = type
        self._assetPair = assetPair
        self._lotCount = lotCount
        self._trigger = trigger
        self._id = uuid.uuid4()
        self._timestamp = timestamp

    def getId(self) -> str:
        return self._id

    def getStatus(self) -> TaskStatus:
        return self._status

    def getType(self) -> TaskType:
        return self._type

    def getTrigger(self) -> ITaskTrigger:
        return self._trigger

    def getAssetPair(self) -> IAssetPair:
        return self._assetPair

    def getLotCount(self) -> int:
        return self._lotCount

    def unlock(self) -> None:
        if not self._status == TaskStatus.Locked:
            raise ValueError(f"Task is already unlocked")

        self._status = TaskStatus.Executing

    def finish(self) -> None:
        if self._status == TaskStatus.Locked:
            raise ValueError(f"Unable to finish locked task")

        if self._status == TaskStatus.Finished:
            raise ValueError(f"Task is already finished")

        self._status = TaskStatus.Finished
    
    def getTimestamp(self) -> int:
        return self._timestamp

    def __str__(self) -> str:
        return f"🧩 ({self.getId()}); Status: {self.getStatus()}; Type: {self.getType()}; Trigger: {self.getTrigger()}; Asset Pair: {self.getAssetPair()}; Lot Count: {self.getLotCount()}"
