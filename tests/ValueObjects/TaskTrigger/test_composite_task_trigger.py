import copy
import pytest
from ValueObjects import CompositeTaskTrigger, ScheduleTaskTrigger, EmptyTaskTrigger, ExecutionContext

@pytest.fixture
def context():
    return ExecutionContext(100, {})

@pytest.fixture
def empty_trigger():
    return EmptyTaskTrigger()

@pytest.fixture
def schedule_trigger_first():
    return ScheduleTaskTrigger(50)

@pytest.fixture
def schedule_trigger_second():
    return ScheduleTaskTrigger(150)

def test_composite_task_trigger_stores_dependencies(empty_trigger, schedule_trigger_first):
    trigger = CompositeTaskTrigger([empty_trigger, schedule_trigger_first])
    dependencies = trigger.getDependencies()
    assert len(dependencies) == 2
    assert dependencies[0] == empty_trigger
    assert dependencies[1] == schedule_trigger_first

def test_composite_task_trigger_empty_dependencies():
    trigger = CompositeTaskTrigger([])
    dependencies = trigger.getDependencies()
    assert len(dependencies) == 0

def test_composite_task_trigger_with_dependency(empty_trigger, schedule_trigger_first):
    trigger = CompositeTaskTrigger([empty_trigger])
    dependencies = trigger.getDependencies()
    newTrigger = trigger.withDependency(schedule_trigger_first)
    newDependencies = newTrigger.getDependencies()
    assert len(newDependencies) == 2
    assert newDependencies[0] == empty_trigger
    assert newDependencies[1] == schedule_trigger_first
    assert len(dependencies) == 1
    assert dependencies[0] == empty_trigger

def test_composite_task_trigger_without_dependency(empty_trigger, schedule_trigger_first):
    trigger = CompositeTaskTrigger([empty_trigger, schedule_trigger_first])
    dependencies = trigger.getDependencies()
    newTrigger = trigger.withoutDependency(empty_trigger)
    newDependencies = newTrigger.getDependencies()
    assert len(newDependencies) == 1
    assert newDependencies[0] == schedule_trigger_first
    assert len(dependencies) == 2
    assert dependencies[0] == empty_trigger
    assert dependencies[1] == schedule_trigger_first

def test_composite_task_trigger_without_nonexistent_dependency(empty_trigger, schedule_trigger_first, schedule_trigger_second):
    trigger = CompositeTaskTrigger([empty_trigger, schedule_trigger_first])
    with pytest.raises(ValueError):
        trigger.withoutDependency(schedule_trigger_second)

def test_composite_task_trigger_is_triggered_all_true(context, empty_trigger, schedule_trigger_first):
    trigger = CompositeTaskTrigger([empty_trigger, schedule_trigger_first])
    assert trigger.isTriggered(context) == True

def test_composite_task_trigger_is_triggered_one_false(context, empty_trigger, schedule_trigger_second):
    trigger = CompositeTaskTrigger([empty_trigger, schedule_trigger_second])
    assert trigger.isTriggered(context) == False

def test_composite_task_trigger_is_triggered_all_false(context, schedule_trigger_second):
    trigger = CompositeTaskTrigger([schedule_trigger_second, schedule_trigger_second])
    assert trigger.isTriggered(context) == False

def test_composite_task_trigger_is_triggered_empty_list(context):
    trigger = CompositeTaskTrigger([])
    assert trigger.isTriggered(context) == True

def test_composite_task_trigger_compare_other_type():
    trigger = CompositeTaskTrigger([])
    assert not trigger == 10

def test_composite_task_trigger_compare_equivalent(empty_trigger, schedule_trigger_first):
    firstTrigger = CompositeTaskTrigger([empty_trigger, schedule_trigger_first])
    secondTrigger = CompositeTaskTrigger([empty_trigger, schedule_trigger_first])
    assert firstTrigger == secondTrigger

def test_composite_task_trigger_compare_different_dependencies_order(empty_trigger, schedule_trigger_first):
    firstTrigger = CompositeTaskTrigger([empty_trigger, schedule_trigger_first])
    secondTrigger = CompositeTaskTrigger([schedule_trigger_first, empty_trigger])
    assert firstTrigger == secondTrigger  # Order doesn't matter for equality

def test_composite_task_trigger_compare_different_dependencies(empty_trigger, schedule_trigger_first, schedule_trigger_second):
    firstTrigger = CompositeTaskTrigger([empty_trigger, schedule_trigger_first])
    secondTrigger = CompositeTaskTrigger([empty_trigger, schedule_trigger_second])
    assert not firstTrigger == secondTrigger

def test_composite_task_trigger_equal_hashes(empty_trigger, schedule_trigger_first):
    firstTrigger = CompositeTaskTrigger([empty_trigger, schedule_trigger_first])
    secondTrigger = CompositeTaskTrigger([empty_trigger, schedule_trigger_first])
    assert hash(firstTrigger) == hash(secondTrigger)

def test_composite_task_trigger_not_equal_hashes(empty_trigger, schedule_trigger_first, schedule_trigger_second):
    firstTrigger = CompositeTaskTrigger([empty_trigger, schedule_trigger_first])
    secondTrigger = CompositeTaskTrigger([empty_trigger, schedule_trigger_second])
    assert hash(firstTrigger) != hash(secondTrigger)

def test_composite_task_trigger_copy(empty_trigger, schedule_trigger_first):
    trigger = CompositeTaskTrigger([empty_trigger, schedule_trigger_first])
    duplicate = copy.copy(trigger)
    assert trigger == duplicate
    assert trigger is not duplicate

def test_composite_task_trigger_string_empty():
    trigger = CompositeTaskTrigger([])
    assert str(trigger) == "🚩 Depends on:"

def test_composite_task_trigger_string_with_dependencies(empty_trigger, schedule_trigger_first):
    composite = CompositeTaskTrigger([empty_trigger, schedule_trigger_first])
    result = str(composite)
    assert result.startswith("🚩 Depends on:")
    assert "🚩 Empty" in result
    assert "🚩 Scheduled at 50" in result
