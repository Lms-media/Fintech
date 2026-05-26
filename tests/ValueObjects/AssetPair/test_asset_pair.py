import copy
import pytest
from ValueObjects import Asset, AssetPair

@pytest.fixture
def rub():
    return Asset("RUB", 1)

@pytest.fixture
def usd():
    return Asset("USD", 10)

@pytest.fixture
def eur():
    return Asset("EUR", 5)

def test_asset_pair_stores_base_asset(rub, usd):
    pair = AssetPair(rub, usd)
    assert pair.getBaseAsset() == rub

def test_asset_pair_stores_quote_asset(rub, usd):
    pair = AssetPair(rub, usd)
    assert pair.getQuoteAsset() == usd

def test_asset_pair_equal_assets(rub):
    with pytest.raises(ValueError):
        AssetPair(rub, rub)

def test_asset_pair_with_base_asset(rub, usd, eur):
    pair = AssetPair(rub, usd)
    newPair = pair.withBaseAsset(eur)
    assert newPair.getBaseAsset() == eur
    assert pair.getBaseAsset() == rub

def test_asset_pair_with_base_equal_asset(rub, usd):
    pair = AssetPair(rub, usd)
    with pytest.raises(ValueError):
        pair.withBaseAsset(usd)

def test_asset_pair_with_quote_asset(rub, usd, eur):
    pair = AssetPair(rub, usd)
    newPair = pair.withQuoteAsset(eur)
    assert newPair.getQuoteAsset() == eur
    assert pair.getQuoteAsset() == usd

def test_asset_pair_with_quote_equal_asset(rub, usd):
    pair = AssetPair(rub, usd)
    with pytest.raises(ValueError):
        pair.withQuoteAsset(rub)

def test_asset_pair_compare_other_type(rub, usd):
    assert not AssetPair(rub, usd) == 10

def test_asset_pair_compare_equivalent_asset(rub, usd):
    assert AssetPair(rub, usd) == AssetPair(rub, usd)

def test_asset_pair_compare_different_base_asset(rub, usd, eur):
    assert not AssetPair(rub, usd) == AssetPair(eur, usd)

def test_asset_pair_compare_different_quote_asset(rub, usd, eur):
    assert not AssetPair(rub, usd) == AssetPair(rub, eur)

def test_asset_pair_equal_hashes(rub, usd):
    assert hash(AssetPair(rub, usd)) == hash(AssetPair(rub, usd))

def test_asset_pair_not_equal_hashes(rub, usd, eur):
    assert hash(AssetPair(rub, usd)) != hash(AssetPair(eur, usd))

def test_asset_pair_copy(rub, usd):
    original = AssetPair(rub, usd)
    duplicate = copy.copy(original)

    assert duplicate == original
    assert duplicate is not original

def test_asset_pair_string(rub, usd):
    assert str(AssetPair(rub, usd)) == f"{str(rub)} - {str(usd)}"
