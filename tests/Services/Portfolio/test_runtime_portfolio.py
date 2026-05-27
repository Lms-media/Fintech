import pytest
from Services import RuntimePortfolio
from ValueObjects import Asset, AssetPair, ExecutionContext

@pytest.fixture
def base_asset():
    return Asset("RUB", 1)

@pytest.fixture
def quote_asset():
    return Asset("USD", 10)

@pytest.fixture
def portfolio(base_asset):
    return RuntimePortfolio(base_asset)

def test_runtime_portfolio_stores_base_asset(portfolio, base_asset):
    assert portfolio.getBaseAsset() == base_asset

def test_runtime_portfolio_initial_base_amount_zero(portfolio):
    assert portfolio.getBaseAmount() == 0.0

def test_runtime_portfolio_initial_asset_amount_zero(portfolio, quote_asset):
    assert portfolio.getAssetAmount(quote_asset) == 0.0

def test_runtime_portfolio_deposit(portfolio):
    portfolio.deposit(1000.0)
    assert portfolio.getBaseAmount() == 1000.0

def test_runtime_portfolio_deposit_multiple(portfolio):
    portfolio.deposit(500.0)
    portfolio.deposit(300.0)
    assert portfolio.getBaseAmount() == 800.0

def test_runtime_portfolio_withdraw(portfolio):
    portfolio.deposit(1000.0)
    portfolio.withdraw(400.0)
    assert portfolio.getBaseAmount() == 600.0

def test_runtime_portfolio_buy_asset_new(portfolio, quote_asset):
    portfolio.deposit(1000.0)
    portfolio.buyAsset(quote_asset, 2, 50.0)
    assert portfolio.getAssetAmount(quote_asset) == 20.0
    assert portfolio.getBaseAmount() == 0.0

def test_runtime_portfolio_buy_asset_existing(portfolio, quote_asset):
    portfolio.deposit(2000.0)
    portfolio.buyAsset(quote_asset, 1, 50.0)
    portfolio.buyAsset(quote_asset, 1, 50.0)
    assert portfolio.getAssetAmount(quote_asset) == 20.0
    assert portfolio.getBaseAmount() == 1000.0

def test_runtime_portfolio_sell_asset_existing(portfolio, quote_asset):
    portfolio.deposit(1000.0)
    portfolio.buyAsset(quote_asset, 2, 50.0)
    portfolio.sellAsset(quote_asset, 1, 60.0)
    assert portfolio.getAssetAmount(quote_asset) == 10.0
    assert portfolio.getBaseAmount() == 600.0

def test_runtime_portfolio_sell_asset_not_existing(portfolio, quote_asset):
    portfolio.sellAsset(quote_asset, 1, 50.0)
    assert portfolio.getAssetAmount(quote_asset) == -10.0
    assert portfolio.getBaseAmount() == 500.0

def test_runtime_portfolio_get_capitalization(portfolio, base_asset, quote_asset):
    portfolio.deposit(500.0)
    portfolio.buyAsset(quote_asset, 1, 50.0)
    assetPair = AssetPair(base_asset, quote_asset)
    context = ExecutionContext(1000, {assetPair: 60.0})
    assert portfolio.getCapitalization(context) == 600.0

def test_runtime_portfolio_get_capitalization_no_price_raises(portfolio, quote_asset):
    portfolio.deposit(500.0)
    portfolio.buyAsset(quote_asset, 1, 50.0)
    context = ExecutionContext(1000, {})
    with pytest.raises(ValueError):
        portfolio.getCapitalization(context)

def test_runtime_portfolio_get_capitalization_empty(portfolio):
    portfolio.deposit(100.0)
    context = ExecutionContext(1000, {})
    assert portfolio.getCapitalization(context) == 100.0
