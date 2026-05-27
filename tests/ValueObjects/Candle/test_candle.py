import copy
import pytest
from ValueObjects import Asset, AssetPair, Candle
from Interfaces import IntervalType

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

def test_candle_stores_asset_pair(rub_usd):
    candle = Candle(rub_usd, 0, IntervalType.OneDay, 10, 12, 15, 5, 7)
    assert candle.getAssetPair() == rub_usd

def test_candle_stores_timestamp(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 10, 12, 15, 5, 7)
    assert candle.getOpenTimestamp() == 180

def test_candle_stores_interval_type(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 10, 12, 15, 5, 7)
    assert candle.getInterval() == IntervalType.OneDay

def test_candle_stores_open_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 10, 12, 15, 5, 7)
    assert candle.getOpenPrice() == 10

def test_candle_stores_close_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 10, 12, 15, 5, 7)
    assert candle.getClosePrice() == 12

def test_candle_stores_high_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 10, 12, 15, 5, 7)
    assert candle.getHighPrice() == 15

def test_candle_stores_low_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 10, 12, 15, 5, 7)
    assert candle.getLowPrice() == 5

def test_candle_stores_volume(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 10, 12, 15, 5, 7)
    assert candle.getVolume() == 7

def test_candle_low_price_equals_high_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 11, 11, 11, 11, 7)
    assert candle.getLowPrice() == candle.getClosePrice()

def test_candle_low_price_greater_than_high_price(rub_usd):
    with pytest.raises(ValueError):
        Candle(rub_usd, 180, IntervalType.OneDay, 10, 12, 11, 11.5, 7)

def test_candle_zero_open_timestamp(rub_usd):
    candle = Candle(rub_usd, 0, IntervalType.OneDay, 10, 12, 15, 5, 7)
    assert candle.getOpenTimestamp() == 0

def test_candle_negative_open_timestamp(rub_usd):
    with pytest.raises(ValueError):
        Candle(rub_usd, -10, IntervalType.OneDay, 10, 12, 15, 5, 7)

def test_candle_zero_low_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 10, 12, 15, 0, 7)
    assert candle.getLowPrice() == 0

def test_candle_negative_low_price(rub_usd):
    with pytest.raises(ValueError):
        Candle(rub_usd, 180, IntervalType.OneDay, 10, 12, 15, -15, 7)

def test_candle_zero_volume(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 0, 0)
    assert candle.getVolume() == 0

def test_candle_negative_volume(rub_usd):
    with pytest.raises(ValueError):
        Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 0, -7)

def test_candle_high_price_equal_open_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 10, 5, 10, 0, 7)
    assert candle.getHighPrice() == 10

def test_candle_high_price_less_than_open_price(rub_usd):
    with pytest.raises(ValueError):
        Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 1, 0, 7)

def test_candle_high_price_equal_close_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 10, 0, 7)
    assert candle.getHighPrice() == 10

def test_candle_high_price_less_than_close_price(rub_usd):
    with pytest.raises(ValueError):
        Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 7, 1, 7)

def test_candle_low_price_equal_open_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 5, 7)
    assert candle.getLowPrice() == 5

def test_candle_low_price_greater_than_open_price(rub_usd):
    with pytest.raises(ValueError):
        Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 7, 1)

def test_candle_low_price_equal_close_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 10, 5, 15, 5, 7)
    assert candle.getLowPrice() == 5

def test_candle_low_price_greater_than_close_price(rub_usd):
    with pytest.raises(ValueError):
        Candle(rub_usd, 180, IntervalType.OneDay, 10, 5, 15, 7, 7)

def test_candle_with_asset_pair(rub_usd, rub_eur):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 0, 7)
    newCandle = candle.withAssetPair(rub_eur)
    assert newCandle.getAssetPair() == rub_eur
    assert candle.getAssetPair() == rub_usd

def test_candle_with_open_timestamp(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 0, 7)
    newCandle = candle.withOpenTimestamp(100)
    assert newCandle.getOpenTimestamp() == 100
    assert candle.getOpenTimestamp() == 180

def test_candle_with_zero_open_timestamp(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 0, 7)
    newCandle = candle.withOpenTimestamp(0)
    assert newCandle.getOpenTimestamp() == 0

def test_candle_with_negative_open_timestamp(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 0, 7)
    with pytest.raises(ValueError):
        candle.withOpenTimestamp(-10)

def test_candle_with_interval(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 0, 7)
    newCandle = candle.withInterval(IntervalType.OneHour)
    assert newCandle.getInterval() == IntervalType.OneHour
    assert candle.getInterval() == IntervalType.OneDay

def test_candle_with_open_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 0, 7)
    newCandle = candle.withOpenPrice(10)
    assert newCandle.getOpenPrice() == 10
    assert candle.getOpenPrice() == 5

def test_candle_with_zero_open_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 0, 7)
    newCandle = candle.withOpenPrice(0)
    assert newCandle.getOpenPrice() == 0

def test_candle_with_negative_open_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 0, 7)
    with pytest.raises(ValueError):
        candle.withOpenPrice(-10)

def test_candle_with_open_price_equal_high_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 0, 7)
    newCandle = candle.withOpenPrice(15)
    assert newCandle.getOpenPrice() == 15

def test_candle_with_open_price_greater_than_high_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 0, 7)
    with pytest.raises(ValueError):
        candle.withOpenPrice(20)

def test_candle_with_open_price_equal_low_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7)
    newCandle = candle.withOpenPrice(3)
    assert newCandle.getOpenPrice() == 3

def test_candle_with_open_price_less_than_low_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7)
    with pytest.raises(ValueError):
        candle.withOpenPrice(0)

def test_candle_with_close_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 0, 7)
    newCandle = candle.withClosePrice(12)
    assert newCandle.getClosePrice() == 12
    assert candle.getClosePrice() == 10

def test_candle_with_zero_close_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 0, 7)
    newCandle = candle.withClosePrice(0)
    assert newCandle.getClosePrice() == 0

def test_candle_with_negative_close_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 0, 7)
    with pytest.raises(ValueError):
        candle.withClosePrice(-10)

def test_candle_with_close_price_equal_high_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 0, 7)
    newCandle = candle.withClosePrice(15)
    assert newCandle.getClosePrice() == 15

def test_candle_with_close_price_greater_than_high_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 0, 7)
    with pytest.raises(ValueError):
        candle.withClosePrice(20)

def test_candle_with_close_price_equal_low_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7)
    newCandle = candle.withClosePrice(3)
    assert newCandle.getClosePrice() == 3

def test_candle_with_close_price_less_than_low_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7)
    with pytest.raises(ValueError):
        candle.withClosePrice(0)

def test_candle_with_high_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 0, 7)
    newCandle = candle.withHighPrice(20)
    assert newCandle.getHighPrice() == 20
    assert candle.getHighPrice() == 15

def test_candle_with_zero_high_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 0, 0, 15, 0, 7)
    newCandle = candle.withHighPrice(0)
    assert newCandle.getHighPrice() == 0

def test_candle_with_negative_high_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 0, 7)
    with pytest.raises(ValueError):
        candle.withHighPrice(-10)

def test_candle_with_high_price_equal_open_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 10, 5, 15, 0, 7)
    newCandle = candle.withHighPrice(10)
    assert newCandle.getHighPrice() == 10

def test_candle_with_high_price_less_than_open_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 10, 5, 15, 0, 7)
    with pytest.raises(ValueError):
        candle.withHighPrice(7)

def test_candle_with_high_price_equal_close_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7)
    newCandle = candle.withHighPrice(10)
    assert newCandle.getHighPrice() == 10

def test_candle_with_high_price_less_than_close_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7)
    with pytest.raises(ValueError):
        candle.withHighPrice(7)

def test_candle_with_low_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7)
    newCandle = candle.withLowPrice(2)
    assert newCandle.getLowPrice() == 2
    assert candle.getLowPrice() == 3

def test_candle_with_zero_low_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7)
    newCandle = candle.withLowPrice(0)
    assert newCandle.getLowPrice() == 0

def test_candle_with_negative_low_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7)
    with pytest.raises(ValueError):
        candle.withLowPrice(-10)

def test_candle_with_low_price_equal_open_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7)
    newCandle = candle.withLowPrice(5)
    assert newCandle.getLowPrice() == 5

def test_candle_with_low_price_less_than_open_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7)
    with pytest.raises(ValueError):
        candle.withLowPrice(7)

def test_candle_with_low_price_equal_close_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 10, 5, 15, 3, 7)
    newCandle = candle.withLowPrice(5)
    assert newCandle.getLowPrice() == 5

def test_candle_with_low_price_less_than_close_price(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 10, 5, 15, 3, 7)
    with pytest.raises(ValueError):
        candle.withLowPrice(7)

def test_candle_with_volume(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7)
    newCandle = candle.withVolume(100)
    assert newCandle.getVolume() == 100

def test_candle_with_zero_volume(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7)
    newCandle = candle.withVolume(0)
    assert newCandle.getVolume() == 0

def test_candle_with_negative_volume(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7)
    with pytest.raises(ValueError):
        candle.withVolume(-100)

def test_candle_compare_other_type(rub_usd):
    assert not Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7) == 10

def test_candle_compare_other_candle(rub_usd):
    assert Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7) == Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7)

def test_candle_compare_different_asset_pair(rub_usd, rub_eur):
    assert not Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7) == Candle(rub_eur, 180, IntervalType.OneDay, 5, 10, 15, 3, 7)

def test_candle_compare_different_open_timestamp(rub_usd):
    assert not Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7) == Candle(rub_usd, 181, IntervalType.OneDay, 5, 10, 15, 3, 7)

def test_candle_compare_different_interval(rub_usd):
    assert not Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7) == Candle(rub_usd, 180, IntervalType.TwoHours, 5, 10, 15, 3, 7)

def test_candle_compare_different_open_price(rub_usd):
    assert not Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7) == Candle(rub_usd, 180, IntervalType.OneDay, 6, 10, 15, 3, 7)

def test_candle_compare_different_close_price(rub_usd):
    assert not Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7) == Candle(rub_usd, 180, IntervalType.OneDay, 5, 11, 15, 3, 7)

def test_candle_compare_different_high_price(rub_usd):
    assert not Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7) == Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 16, 3, 7)

def test_candle_compare_different_low_price(rub_usd):
    assert not Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7) == Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 14, 3, 7)

def test_candle_compare_different_volume(rub_usd):
    assert not Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7) == Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 8)

def test_candle_equal_hashes(rub_usd):
    assert hash(Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7)) == hash(Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7))

def test_candle_not_equal_hashes(rub_usd):
    assert hash(Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7)) != hash(Candle(rub_usd, 181, IntervalType.OneDay, 5, 10, 15, 3, 7))

def test_candle_copy(rub_usd):
    original = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7)
    duplicate = copy.copy(original)

    assert duplicate == original
    assert duplicate is not original

def test_candle_string(rub_usd):
    candle = Candle(rub_usd, 180, IntervalType.OneDay, 5, 10, 15, 3, 7)
    assert str(candle) == f"📊 {{{str(rub_usd)}, timestamp=180, interval=OneDay, O=5, C=10, H=15, L=3, V=7}}"
