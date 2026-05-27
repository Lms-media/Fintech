from enum import Enum

class TaskStatus(Enum):
    Locked = 1
    Executing = 2
    Finished = 3
