import copy
import pytest
from ValueObjects import ScheduleTaskTrigger, ExecutionContext


@pytest.fixture
def context():
    return ExecutionContext(100, {})

def test_schedule_task_trigger_stores_timestamp():
    trigger = ScheduleTaskTrigger(100)
    assert trigger.getTimestamp() == 100

def test_schedule_task_trigger_zero_timestamp():
    trigger = ScheduleTaskTrigger(0)
    assert trigger.getTimestamp() == 0

def test_schedule_task_trigger_negative_timestamp():
    with pytest.raises(ValueError):
        ScheduleTaskTrigger(-1)

def test_schedule_task_trigger_with_timestamp():
    trigger = ScheduleTaskTrigger(100)
    newTrigger = trigger.withTimestamp(200)
    assert newTrigger.getTimestamp() == 200
    assert trigger.getTimestamp() == 100

def test_schedule_task_trigger_with_zero_timestamp():
    trigger = ScheduleTaskTrigger(100)
    newTrigger = trigger.withTimestamp(0)
    assert newTrigger.getTimestamp() == 0

def test_schedule_task_trigger_with_negative_timestamp():
    trigger = ScheduleTaskTrigger(100)
    with pytest.raises(ValueError):
        trigger.withTimestamp(-1)

def test_schedule_task_trigger_is_triggered(context):
    trigger = ScheduleTaskTrigger(50)
    assert trigger.isTriggered(context) == True

def test_schedule_task_trigger_is_not_triggered(context):
    trigger = ScheduleTaskTrigger(150)
    assert trigger.isTriggered(context) == False

def test_schedule_task_trigger_is_triggered_equal():
    trigger = ScheduleTaskTrigger(100)
    context = ExecutionContext(100, {})
    assert trigger.isTriggered(context) == False

def test_schedule_task_trigger_compare_other_type():
    trigger = ScheduleTaskTrigger(100)
    assert not trigger == 10

def test_schedule_task_trigger_compare_equivalent():
    trigger = ScheduleTaskTrigger(100)
    otherTrigger = ScheduleTaskTrigger(100)
    assert trigger == otherTrigger


def test_schedule_task_trigger_compare_different_timestamp():
    trigger = ScheduleTaskTrigger(100)
    otherTrigger = ScheduleTaskTrigger(200)
    assert not trigger == otherTrigger


def test_schedule_task_trigger_equal_hashes():
    trigger = ScheduleTaskTrigger(100)
    otherTrigger = ScheduleTaskTrigger(100)
    assert hash(trigger) == hash(otherTrigger)

def test_schedule_task_trigger_not_equal_hashes():
    trigger = ScheduleTaskTrigger(100)
    otherTrigger = ScheduleTaskTrigger(200)
    assert hash(trigger) != hash(otherTrigger)

def test_schedule_task_trigger_copy():
    trigger = ScheduleTaskTrigger(100)
    otherTrigger = copy.copy(trigger)
    assert trigger == otherTrigger
    assert trigger is not otherTrigger

def test_schedule_task_trigger_string():
    trigger = ScheduleTaskTrigger(100)
    assert str(trigger) == "🚩 Scheduled at 100"
