import pytest
from Services import RuntimePortfolio, LoggedPortfolio
from ValueObjects import Asset, AssetPair, ExecutionContext
from tests.mocks import MockedLogger

@pytest.fixture
def base_asset():
    return Asset("RUB", 1)

@pytest.fixture
def quote_asset():
    return Asset("USD", 10)

@pytest.fixture
def inner_portfolio(base_asset):
    p = RuntimePortfolio(base_asset)
    p.deposit(10000.0)
    return p

@pytest.fixture
def logger():
    return MockedLogger()

@pytest.fixture
def portfolio(inner_portfolio, logger):
    return LoggedPortfolio(inner_portfolio, logger)

def test_logged_portfolio_init_logs_message(inner_portfolio, logger):
    LoggedPortfolio(inner_portfolio, logger)
    assert len(logger.messages) == 1
    assert "Init portfolio" in logger.messages[0]

def test_logged_portfolio_get_base_asset(portfolio, base_asset):
    assert portfolio.getBaseAsset() == base_asset

def test_logged_portfolio_get_base_amount(portfolio):
    assert portfolio.getBaseAmount() == 10000.0

def test_logged_portfolio_get_asset_amount(portfolio, quote_asset):
    assert portfolio.getAssetAmount(quote_asset) == 0.0

def test_logged_portfolio_get_capitalization(portfolio, base_asset, quote_asset):
    assetPair = AssetPair(base_asset, quote_asset)
    context = ExecutionContext(1000, {assetPair: 50.0})
    assert portfolio.getCapitalization(context) == 10000.0

def test_logged_portfolio_buy_asset_logs(portfolio, logger, quote_asset):
    portfolio.buyAsset(quote_asset, 1, 50.0)
    assert any("Buy" in m for m in logger.messages)

def test_logged_portfolio_buy_asset_delegates(portfolio, inner_portfolio, quote_asset):
    portfolio.buyAsset(quote_asset, 1, 50.0)
    assert inner_portfolio.getAssetAmount(quote_asset) == 10.0

def test_logged_portfolio_sell_asset_logs(portfolio, logger, quote_asset):
    portfolio.buyAsset(quote_asset, 1, 50.0)
    portfolio.sellAsset(quote_asset, 1, 60.0)
    assert any("Sell" in m for m in logger.messages)

def test_logged_portfolio_sell_asset_delegates(portfolio, inner_portfolio, quote_asset):
    portfolio.buyAsset(quote_asset, 1, 50.0)
    portfolio.sellAsset(quote_asset, 1, 60.0)
    assert inner_portfolio.getAssetAmount(quote_asset) == 0.0

def test_logged_portfolio_deposit_logs(portfolio, logger):
    portfolio.deposit(500.0)
    assert any("Deposit" in m for m in logger.messages)

def test_logged_portfolio_deposit_delegates(portfolio, inner_portfolio):
    portfolio.deposit(500.0)
    assert inner_portfolio.getBaseAmount() == 10500.0

def test_logged_portfolio_withdraw_logs(portfolio, logger):
    portfolio.withdraw(200.0)
    assert any("Withdraw" in m for m in logger.messages)

def test_logged_portfolio_withdraw_delegates(portfolio, inner_portfolio):
    portfolio.withdraw(200.0)
    assert inner_portfolio.getBaseAmount() == 9800.0
