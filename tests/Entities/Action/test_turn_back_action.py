import pytest
from Entities import CandleSeries, NextCandlePrediction, PredictionMeta, DirectionSignal, TurnBackAction
from ValueObjects import Candle, AssetPair, Asset, ExecutionContext, EmptyTaskTrigger, ScheduleTaskTrigger
from Interfaces import IntervalType, DirectionType, TaskType, ActionStatus

@pytest.fixture
def asset_pair():
    return AssetPair(Asset("RUB", 10), Asset("USD", 100))

@pytest.fixture
def candle_series(asset_pair):
    return CandleSeries(asset_pair)

@pytest.fixture
def next_candle(asset_pair):
    return Candle(asset_pair, 160, IntervalType.OneMinute, 105.0, 110.0, 115.0, 100.0, 1100.0)

@pytest.fixture
def meta(candle_series):
    return PredictionMeta(1000, candle_series, 0.9)

@pytest.fixture
def prediction(meta, next_candle):
    return NextCandlePrediction(meta, next_candle)

@pytest.fixture
def signal(prediction):
    return DirectionSignal(1000, prediction, 10.0, DirectionType.Up)

@pytest.fixture
def context(asset_pair):
    return ExecutionContext(500, {asset_pair: 100.0})

@pytest.fixture
def action_first_buy(signal, asset_pair, context):
    return TurnBackAction(signal, asset_pair, 5, True, context, 300)

@pytest.fixture
def action_first_sell(signal, asset_pair, context):
    return TurnBackAction(signal, asset_pair, 5, False, context, 300)

def test_turn_back_action_generates_unique_id(signal, asset_pair, context):
    first = TurnBackAction(signal, asset_pair, 5, True, context, 300)
    second = TurnBackAction(signal, asset_pair, 5, True, context, 300)
    assert first.getId() != second.getId()

def test_turn_back_action_trigger_timestamp(action_first_buy):
    assert action_first_buy.getTriggerTimestamp() == 800

def test_turn_back_action_trigger_timestamp_first_sell(action_first_sell):
    assert action_first_sell.getTriggerTimestamp() == 800

def test_turn_back_action_stores_signal(action_first_buy, signal):
    assert action_first_buy.getSignal() == signal

def test_turn_back_action_initial_status_waiting(action_first_buy):
    assert action_first_buy.getStatus() == ActionStatus.Waiting

def test_turn_back_action_creates_two_tasks(action_first_buy):
    assert len(action_first_buy.getTasks()) == 2

def test_turn_back_action_first_buy_first_task_is_buy(action_first_buy):
    tasks = action_first_buy.getTasks()
    assert tasks[0].getType() == TaskType.Buy

def test_turn_back_action_first_buy_second_task_is_sell(action_first_buy):
    tasks = action_first_buy.getTasks()
    assert tasks[1].getType() == TaskType.Sell

def test_turn_back_action_first_sell_first_task_is_sell(action_first_sell):
    tasks = action_first_sell.getTasks()
    assert tasks[0].getType() == TaskType.Sell

def test_turn_back_action_first_sell_second_task_is_buy(action_first_sell):
    tasks = action_first_sell.getTasks()
    assert tasks[1].getType() == TaskType.Buy

def test_turn_back_action_first_task_has_empty_trigger(action_first_buy):
    tasks = action_first_buy.getTasks()
    assert isinstance(tasks[0].getTrigger(), EmptyTaskTrigger)

def test_turn_back_action_second_task_has_schedule_trigger(action_first_buy):
    tasks = action_first_buy.getTasks()
    assert isinstance(tasks[1].getTrigger(), ScheduleTaskTrigger)

def test_turn_back_action_second_task_trigger_timestamp(action_first_buy):
    tasks = action_first_buy.getTasks()
    trigger = tasks[1].getTrigger()
    assert trigger.getTimestamp() == 800

def test_turn_back_action_tasks_lot_count(action_first_buy):
    tasks = action_first_buy.getTasks()
    assert tasks[0].getLotCount() == 5
    assert tasks[1].getLotCount() == 5

def test_turn_back_action_tasks_asset_pair(action_first_buy, asset_pair):
    tasks = action_first_buy.getTasks()
    assert tasks[0].getAssetPair() == asset_pair
    assert tasks[1].getAssetPair() == asset_pair

def test_turn_back_action_string_contains_id(action_first_buy):
    actionStr = str(action_first_buy)
    assert action_first_buy.getId() in actionStr

def test_turn_back_action_string_contains_emoji(action_first_buy):
    actionStr = str(action_first_buy)
    assert "🎬" in actionStr

def test_turn_back_action_string_contains_status(action_first_buy):
    actionStr = str(action_first_buy)
    assert "Status:" in actionStr
