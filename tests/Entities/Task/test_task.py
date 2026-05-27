import pytest
from Entities import Task
from ValueObjects import AssetPair, Asset, EmptyTaskTrigger, ScheduleTaskTrigger
from Interfaces import TaskStatus, TaskType

@pytest.fixture()
def empty_trigger():
    return EmptyTaskTrigger()

@pytest.fixture()
def asset_pair():
    return AssetPair(Asset("RUB", 10), Asset("USD", 100))

def test_task_stores_type(empty_trigger, asset_pair):
    task = Task(TaskType.Buy, asset_pair, 10, empty_trigger)
    assert task.getType() == TaskType.Buy

def test_task_stores_asset_pair(empty_trigger, asset_pair):
    task = Task(TaskType.Buy, asset_pair, 10, empty_trigger)
    assert task.getAssetPair() == asset_pair

def test_task_stores_lot_count(empty_trigger, asset_pair):
    task = Task(TaskType.Buy, asset_pair, 10, empty_trigger)
    assert task.getLotCount() == 10

def test_task_stores_trigger(empty_trigger, asset_pair):
    task = Task(TaskType.Buy, asset_pair, 10, empty_trigger)
    assert task.getTrigger() == empty_trigger

def test_task_initial_status_locked(empty_trigger, asset_pair):
    task = Task(TaskType.Buy, asset_pair, 10, empty_trigger)
    assert task.getStatus() == TaskStatus.Locked

def test_task_generates_unique_id(empty_trigger, asset_pair):
    firstTask = Task(TaskType.Buy, asset_pair, 10, empty_trigger)
    secondTask = Task(TaskType.Buy, asset_pair, 10, empty_trigger)
    assert firstTask.getId() != secondTask.getId()

def test_task_zero_lot_count(empty_trigger, asset_pair):
    task = Task(TaskType.Buy, asset_pair, 0, empty_trigger)
    assert task.getLotCount() == 0

def test_task_negative_lot_count(empty_trigger, asset_pair):
    with pytest.raises(ValueError):
        Task(TaskType.Buy, asset_pair, -1, empty_trigger)

def test_task_unlock_from_locked(empty_trigger, asset_pair):
    task = Task(TaskType.Buy, asset_pair, 10, empty_trigger)
    task.unlock()
    assert task.getStatus() == TaskStatus.Executing

def test_task_unlock_already_unlocked(empty_trigger, asset_pair):
    task = Task(TaskType.Buy, asset_pair, 10, empty_trigger)
    task.unlock()
    with pytest.raises(ValueError):
        task.unlock()

def test_task_unlock_from_finished(empty_trigger, asset_pair):
    task = Task(TaskType.Buy, asset_pair, 10, empty_trigger)
    task.unlock()
    task.finish()
    with pytest.raises(ValueError):
        task.unlock()

def test_task_finish_from_executing(empty_trigger, asset_pair):
    task = Task(TaskType.Buy, asset_pair, 10, empty_trigger)
    task.unlock()
    task.finish()
    assert task.getStatus() == TaskStatus.Finished

def test_task_finish_from_locked(empty_trigger, asset_pair):
    task = Task(TaskType.Buy, asset_pair, 10, empty_trigger)
    with pytest.raises(ValueError):
        task.finish()

def test_task_finish_already_finished(empty_trigger, asset_pair):
    task = Task(TaskType.Buy, asset_pair, 10, empty_trigger)
    task.unlock()
    task.finish()
    with pytest.raises(ValueError):
        task.finish()

def test_task_string(empty_trigger, asset_pair):
    task = Task(TaskType.Buy, asset_pair, 10, empty_trigger)
    task_str = str(task)
    assert "🧩" in task_str
    assert task.getId() in task_str
    assert "Status:" in task_str
    assert "Type:" in task_str
    assert "Trigger:" in task_str
    assert "Asset Pair:" in task_str
    assert "Lot Count:" in task_str
