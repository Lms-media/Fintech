from Interfaces import ITaskTrigger

class ScheduleTaskTrigger(ITaskTrigger):
    _timestamp: int

    def __init__(self, timestamp: int):
        if timestamp < 0:
            raise ValueError(f"'timestamp' must be greater than or equal zero, but 'timestamp' is {timestamp}")

        self._timestamp = timestamp

    def getTimestamp(self) -> int:
        return self._timestamp

    def withTimestamp(self, timestamp: int) -> ScheduleTaskTrigger:
        if timestamp < 0:
            raise ValueError(f"'timestamp' must be greater than or equal zero, but 'timestamp' is {timestamp}")

        return ScheduleTaskTrigger(timestamp)

    def isTriggered(self, context) -> bool:
        return True

    def __eq__(self, other) -> bool:
        if not isinstance(other, ScheduleTaskTrigger):
            return False

        return self._timestamp == other.getTimestamp()

    def __hash__(self) -> int:
        return hash(self._timestamp)

    def __copy__(self) -> ITaskTrigger:
        return ScheduleTaskTrigger(self._timestamp)

    def __str__(self) -> str:
        return f"🚩 Scheduled at {self._timestamp}"
