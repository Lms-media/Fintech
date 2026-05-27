import pytest
from Services import LoggedMarket
from Entities import Task
from ValueObjects import Asset, AssetPair, EmptyTaskTrigger
from Interfaces import TaskType
from tests.mocks import MockedLogger, MockedMarket

@pytest.fixture
def asset_pair():
    return AssetPair(Asset("RUB", 1), Asset("USD", 10))

@pytest.fixture
def task(asset_pair):
    return Task(TaskType.Buy, asset_pair, 1, EmptyTaskTrigger())

@pytest.fixture
def inner_market():
    return MockedMarket()

@pytest.fixture
def logger():
    return MockedLogger()

@pytest.fixture
def market(inner_market, logger):
    return LoggedMarket(inner_market, logger)

def test_logged_market_execute_delegates(market, inner_market, task):
    market.execute(task)
    assert task in inner_market.executed_tasks

def test_logged_market_execute_logs(market, logger, task):
    market.execute(task)
    assert len(logger.messages) == 1
    assert "Executed Task" in logger.messages[0]

def test_logged_market_execute_logs_task_string(market, logger, task):
    market.execute(task)
    assert str(task) in logger.messages[0]

def test_logged_market_execute_multiple(market, inner_market, logger, task):
    market.execute(task)
    market.execute(task)
    assert len(inner_market.executed_tasks) == 2
    assert len(logger.messages) == 2
