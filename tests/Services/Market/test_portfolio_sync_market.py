import pytest
from Services import PortfolioSyncMarket, RuntimePortfolio
from Entities import Task
from ValueObjects import Asset, AssetPair, EmptyTaskTrigger, ExecutionContext
from Interfaces import TaskType
from tests.mocks import MockedContextProvider

@pytest.fixture
def base_asset():
    return Asset("USD", 10)

@pytest.fixture
def quote_asset():
    return Asset("RUB", 1)

@pytest.fixture
def asset_pair(base_asset, quote_asset):
    return AssetPair(base_asset, quote_asset)

@pytest.fixture
def portfolio(base_asset):
    p = RuntimePortfolio(base_asset)
    p.deposit(10000.0)
    return p

@pytest.fixture
def context_provider(asset_pair):
    return MockedContextProvider(asset_pair, 100)


@pytest.fixture
def market(portfolio, context_provider):
    return PortfolioSyncMarket(portfolio, context_provider)

def test_portfolio_sync_market_execute_buy(market, portfolio, quote_asset, asset_pair):
    task = Task(TaskType.Buy, asset_pair, 2, EmptyTaskTrigger())
    market.execute(task)
    assert portfolio.getAssetAmount(quote_asset) == 2.0
    assert portfolio.getBaseAmount() == 9800.0

def test_portfolio_sync_market_execute_sell(market, portfolio, quote_asset, asset_pair):
    buy_task = Task(TaskType.Buy, asset_pair, 1, EmptyTaskTrigger())
    market.execute(buy_task)
    sell_task = Task(TaskType.Sell, asset_pair, 1, EmptyTaskTrigger())
    market.execute(sell_task)
    assert portfolio.getAssetAmount(quote_asset) == 0.0
    assert portfolio.getBaseAmount() == 10000.0
