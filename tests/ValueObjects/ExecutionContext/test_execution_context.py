import copy
import pytest
from ValueObjects import ExecutionContext, AssetPair, Asset

@pytest.fixture
def rub_usd():
    rub = Asset("RUB", 1)
    usd = Asset("USD", 10)
    return AssetPair(rub, usd)

@pytest.fixture
def rub_eur():
    rub = Asset("RUB", 1)
    eur = Asset("EUR", 5)
    return AssetPair(rub, eur)

def test_execution_context_stores_timestamp(rub_usd, rub_eur):
    context = ExecutionContext(100, { rub_usd: 150, rub_eur: 200 })
    assert context.getTimestamp() == 100

def test_execution_context_zero_timestamp(rub_usd):
    context = ExecutionContext(0, { rub_usd: 150 })
    assert context.getTimestamp() == 0

def test_execution_context_negative_timestamp(rub_usd):
    with pytest.raises(ValueError):
        ExecutionContext(-1, { rub_usd: 150 })

def test_execution_context_empty_prices(rub_usd):
    context = ExecutionContext(100, {})
    assert context.getPrice(rub_usd) == None

def test_execution_context_stores_prices(rub_usd, rub_eur):
    context = ExecutionContext(100, { rub_usd: 150, rub_eur: 200 })
    assert context.getPrice(rub_usd) == 150

def test_execution_context_get_non_existing_price(rub_usd, rub_eur):
    context = ExecutionContext(100, { rub_usd: 150 })
    assert context.getPrice(rub_eur) == None

def test_execution_context_with_price(rub_usd, rub_eur):
    context = ExecutionContext(100, { rub_usd: 150 })
    newContext = context.withPrice(rub_eur, 200)
    assert newContext.getPrice(rub_eur) == 200
    assert context.getPrice(rub_eur) == None

def test_execution_context_with_existing_price(rub_usd, rub_eur):
    context = ExecutionContext(100, { rub_usd: 150, rub_eur: 200 })
    newContext = context.withPrice(rub_eur, 300)
    assert newContext.getPrice(rub_eur) == 300
    assert context.getPrice(rub_eur) == 200

def test_execution_context_with_timestamp(rub_usd, rub_eur):
    context = ExecutionContext(100, { rub_usd: 150, rub_eur: 200 })
    newContext = context.withTimestamp(200)
    assert newContext.getTimestamp() == 200
    assert context.getTimestamp() == 100

def test_executiion_context_with_zero_timestamp(rub_usd, rub_eur):
    context = ExecutionContext(100, { rub_usd: 150, rub_eur: 200 })
    newContext = context.withTimestamp(0)
    assert newContext.getTimestamp() == 0

def test_execution_context_with_negative_timestamp(rub_usd, rub_eur):
    context = ExecutionContext(100, { rub_usd: 150, rub_eur: 200 })
    with pytest.raises(ValueError):
        context.withTimestamp(-1)

def test_execution_context_compare_other_type(rub_usd, rub_eur):
    context = ExecutionContext(100, { rub_usd: 150, rub_eur: 200 })
    assert context != 100

def test_execution_context_compare_equivalent(rub_usd, rub_eur):
    context = ExecutionContext(100, { rub_usd: 150, rub_eur: 200 })
    otherContext = ExecutionContext(100, { rub_usd: 150, rub_eur: 200 })
    assert context == otherContext

def test_execution_context_compare_different_timestamps(rub_usd, rub_eur):
    context = ExecutionContext(100, { rub_usd: 150, rub_eur: 200 })
    otherContext = ExecutionContext(200, { rub_usd: 150, rub_eur: 200 })
    assert context != otherContext

def test_execution_context_compare_different_prices(rub_usd, rub_eur):
    context = ExecutionContext(100, { rub_usd: 150, rub_eur: 200 })
    otherContext = ExecutionContext(100, { rub_usd: 150, rub_eur: 300 })
    assert context != otherContext

def test_execution_context_equal_hashes(rub_usd, rub_eur):
    context = ExecutionContext(100, { rub_usd: 150, rub_eur: 200 })
    otherContext = ExecutionContext(100, { rub_usd: 150, rub_eur: 200 })
    assert hash(context) == hash(otherContext)

def test_execution_context_not_equal_hashes(rub_usd, rub_eur):
    context = ExecutionContext(100, { rub_usd: 150, rub_eur: 200 })
    otherContext = ExecutionContext(200, { rub_usd: 150, rub_eur: 200 })
    assert hash(context) != hash(otherContext)

def test_execution_context_copy(rub_usd, rub_eur):
    context = ExecutionContext(100, { rub_usd: 150, rub_eur: 200 })
    otherContext = copy.copy(context)
    assert context == otherContext
    assert context is not otherContext

def test_execution_context_string(rub_usd, rub_eur):
    context = ExecutionContext(100, { rub_usd: 150, rub_eur: 200 })
    assert str(context) == f"ℹ️ Timestamp: 100\n{str(rub_usd)} - 150.0000\n{str(rub_eur)} - 200.0000"
