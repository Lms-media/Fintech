import copy
import pytest
from ValueObjects import Asset

def test_asset_stores_ticker_code():
    asset = Asset("RUB", 10)
    assert asset.getTickerCode() == "RUB"


def test_asset_stores_lot_size():
    asset = Asset("RUB", 10)
    assert asset.getLotSize() == 10

def test_asset_empty_ticker_code():
    with pytest.raises(ValueError):
        Asset("", 10)

def test_asset_zero_lot_size():
    with pytest.raises(ValueError):
        Asset("RUB", 0)

def test_asset_negative_lot_size():
    with pytest.raises(ValueError):
        Asset("RUB", -10)

def test_asset_with_ticker_code():
    asset = Asset("RUB", 10)
    newAsset = asset.withTickerCode("USD")
    assert newAsset.getTickerCode() == "USD"
    assert asset.getTickerCode() == "RUB"

def test_asset_with_ticker_code_empty():
    asset = Asset("RUB", 10)
    with pytest.raises(ValueError):
        asset.withTickerCode("")

def test_asset_with_lot_size():
    asset = Asset("RUB", 10)
    newAsset = asset.withLotSize(20)
    assert newAsset.getLotSize() == 20
    assert asset.getLotSize() == 10

def test_asset_with_lot_size_zero():
    asset = Asset("RUB", 10)
    with pytest.raises(ValueError):
        asset.withLotSize(0)

def test_asset_with_lot_size_negative():
    asset = Asset("RUB", 10)
    with pytest.raises(ValueError):
        asset.withLotSize(-10)

def test_asset_compare_other_type():
    assert not Asset("RUB", 10) == 10

def test_asset_compare_equivalent_asset():
    assert Asset("RUB", 10) == Asset("RUB", 10)

def test_asset_compare_different_ticker_code():
    assert not Asset("RUB", 10) == Asset("USD", 10)

def test_asset_compare_different_lot_size():
    assert not Asset("RUB", 10) == Asset("RUB", 20)

def test_asset_equal_hashes():
    assert hash(Asset("RUB", 10)) == hash(Asset("RUB", 10))

def test_asset_not_equal_hashes():
    assert hash(Asset("RUB", 10)) != hash(Asset("USD", 10))

def test_asset_copy():
    original = Asset("RUB", 10)
    duplicate = copy.copy(original)

    assert duplicate == original
    assert duplicate is not original

def test_asset_string():
    assert str(Asset("RUB", 10)) == "💵 RUB (10)"
