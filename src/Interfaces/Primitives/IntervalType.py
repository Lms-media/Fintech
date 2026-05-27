from enum import Enum

class IntervalType(Enum):
    OneMinute = 60
    FiveMinutes = 300
    FifteenMinutes = 900
    HalfHour = 1800
    OneHour = 3600
    TwoHours = 7200
    FourHours = 14400
    OneDay = 86400
    OneWeek = 604800
