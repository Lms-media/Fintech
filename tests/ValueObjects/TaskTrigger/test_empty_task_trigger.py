import copy
import pytest
from ValueObjects import EmptyTaskTrigger, ExecutionContext

@pytest.fixture
def context():
    return ExecutionContext(100, {})

def test_empty_task_trigger_is_triggered(context):
    trigger = EmptyTaskTrigger()
    assert trigger.isTriggered(context) == True

def test_empty_task_trigger_compare_other_type():
    trigger = EmptyTaskTrigger()
    assert not trigger == 10

def test_empty_task_trigger_compare_other_empty_trigger():
    trigger = EmptyTaskTrigger()
    otherTrigger = EmptyTaskTrigger()
    assert trigger == otherTrigger

def test_empty_task_trigger_equal_hashes():
    trigger = EmptyTaskTrigger()
    otherTrigger = EmptyTaskTrigger()
    assert hash(trigger) == hash(otherTrigger)

def test_empty_task_trigger_not_equal_hashes():
    trigger = EmptyTaskTrigger()
    assert hash(trigger) != hash(10)

def test_empty_task_trigger_copy():
    trigger = EmptyTaskTrigger()
    otherTrigger = copy.copy(trigger)
    assert trigger == otherTrigger
    assert trigger is not otherTrigger

def test_empty_task_trigger_string():
    trigger = EmptyTaskTrigger()
    assert str(trigger) == "🚩 Empty"
