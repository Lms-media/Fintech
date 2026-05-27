import pytest
from Entities import CandleSeries
from ValueObjects import Candle, AssetPair, Asset
from Interfaces import IntervalType

@pytest.fixture
def asset_pair():
    return AssetPair(Asset("RUB", 10), Asset("USD", 100))

@pytest.fixture
def first_candle(asset_pair):
    return Candle(asset_pair, 100, IntervalType.OneMinute, 100.0, 105.0, 110.0, 95.0, 1000.0)

@pytest.fixture
def second_candle(asset_pair):
    return Candle(asset_pair, 160, IntervalType.OneMinute, 105.0, 110.0, 115.0, 100.0, 1100.0)

@pytest.fixture
def third_candle(asset_pair):
    return Candle(asset_pair, 220, IntervalType.OneMinute, 110.0, 115.0, 120.0, 105.0, 1200.0)

def test_candle_series_stores_asset_pair(asset_pair):
    series = CandleSeries(asset_pair)
    assert series.getAssetPair() == asset_pair

def test_candle_series_generates_unique_id(asset_pair):
    firstSeries = CandleSeries(asset_pair)
    secondSeries = CandleSeries(asset_pair)
    assert firstSeries.getId() != secondSeries.getId()

def test_candle_series_initial_count_zero(asset_pair):
    series = CandleSeries(asset_pair)
    assert series.getCount() == 0

def test_candle_series_get_by_index_empty(asset_pair):
    series = CandleSeries(asset_pair)
    assert series.getByIndex(0) is None

def test_candle_series_get_by_index_negative(asset_pair):
    series = CandleSeries(asset_pair)
    assert series.getByIndex(-1) is None

def test_candle_series_get_by_timestamp_empty(asset_pair):
    series = CandleSeries(asset_pair)
    assert series.getByTimestamp(100) is None

def test_candle_series_append_right_first_candle(asset_pair, first_candle):
    candleSeries = CandleSeries(asset_pair)
    candleSeries.appendRight(first_candle)
    assert candleSeries.getCount() == 1
    assert candleSeries.getByIndex(0) == first_candle

def test_candle_series_append_right_multiple_candles(asset_pair, first_candle, second_candle):
    candleSeries = CandleSeries(asset_pair)
    candleSeries.appendRight(first_candle)
    candleSeries.appendRight(second_candle)
    assert candleSeries.getCount() == 2
    assert candleSeries.getByIndex(0) == first_candle
    assert candleSeries.getByIndex(1) == second_candle

def test_candle_series_append_right_other_asset_pair(asset_pair):
    candleSeries = CandleSeries(asset_pair)
    otherAssetPair = AssetPair(Asset("EUR", 10), Asset("USD", 100))
    candle = Candle(otherAssetPair, 100, IntervalType.OneMinute, 100.0, 105.0, 110.0, 95.0, 1000.0)
    with pytest.raises(ValueError):
        candleSeries.appendRight(candle)

def test_candle_series_append_right_not_adjoining(asset_pair, first_candle):
    candleSeries = CandleSeries(asset_pair)
    candleSeries.appendRight(first_candle)
    # Candle at timestamp 50 starts before first_candle ends (100+60=160), so it's not adjoining
    overlappingCandle = Candle(asset_pair, 50, IntervalType.OneMinute, 100.0, 105.0, 110.0, 95.0, 1000.0)
    with pytest.raises(ValueError):
        candleSeries.appendRight(overlappingCandle)

def test_candle_series_append_left_first_candle(asset_pair, third_candle):
    candleSeries = CandleSeries(asset_pair)
    candleSeries.appendLeft(third_candle)
    assert candleSeries.getCount() == 1
    assert candleSeries.getByIndex(0) == third_candle

def test_candle_series_append_left_multiple_candles(asset_pair, third_candle):
    candleSeries = CandleSeries(asset_pair)
    candleSeries.appendLeft(third_candle)   # series: [220]
    # 161 + 60 = 221 > 220 ✓
    leftCandle = Candle(third_candle.getAssetPair(), 161, IntervalType.OneMinute, 105.0, 110.0, 115.0, 100.0, 1100.0)
    candleSeries.appendLeft(leftCandle)     # series: [161, 220]
    assert candleSeries.getCount() == 2
    assert candleSeries.getByIndex(0) == leftCandle
    assert candleSeries.getByIndex(1) == third_candle

def test_candle_series_append_left_other_asset_pair(asset_pair):
    candleSeries = CandleSeries(asset_pair)
    otherAssetPair = AssetPair(Asset("EUR", 10), Asset("USD", 100))
    candle = Candle(otherAssetPair, 100, IntervalType.OneMinute, 100.0, 105.0, 110.0, 95.0, 1000.0)
    with pytest.raises(ValueError):
        candleSeries.appendLeft(candle)

def test_candle_series_append_left_not_adjoining(asset_pair, first_candle, third_candle):
    candleSeries = CandleSeries(asset_pair)
    candleSeries.appendLeft(third_candle)
    # first_candle end = 100 + 60 = 160, NOT > 220 → raises
    with pytest.raises(ValueError):
        candleSeries.appendLeft(first_candle)

def test_candle_series_pop_left_empty(asset_pair):
    candleSeries = CandleSeries(asset_pair)
    assert candleSeries.popLeft() is None

def test_candle_series_pop_left_single_candle(asset_pair, first_candle):
    candleSeries = CandleSeries(asset_pair)
    candleSeries.appendRight(first_candle)
    popped = candleSeries.popLeft()
    assert popped == first_candle
    assert candleSeries.getCount() == 0

def test_candle_series_pop_left_multiple_candles(asset_pair, first_candle, second_candle):
    candleSeries = CandleSeries(asset_pair)
    candleSeries.appendRight(first_candle)
    candleSeries.appendRight(second_candle)
    popped = candleSeries.popLeft()
    assert popped == first_candle
    assert candleSeries.getCount() == 1
    assert candleSeries.getByIndex(0) == second_candle

def test_candle_series_pop_right_empty(asset_pair):
    candleSeries = CandleSeries(asset_pair)
    assert candleSeries.popRight() is None

def test_candle_series_pop_right_single_candle(asset_pair, first_candle):
    candleSeries = CandleSeries(asset_pair)
    candleSeries.appendRight(first_candle)
    popped = candleSeries.popRight()
    assert popped == first_candle
    assert candleSeries.getCount() == 0

def test_candle_series_pop_right_multiple_candles(asset_pair, first_candle, second_candle):
    candleSeries = CandleSeries(asset_pair)
    candleSeries.appendRight(first_candle)
    candleSeries.appendRight(second_candle)
    popped = candleSeries.popRight()
    assert popped == second_candle
    assert candleSeries.getCount() == 1
    assert candleSeries.getByIndex(0) == first_candle

def test_candle_series_get_by_index_valid(asset_pair, first_candle, second_candle):
    candleSeries = CandleSeries(asset_pair)
    candleSeries.appendRight(first_candle)
    candleSeries.appendRight(second_candle)
    assert candleSeries.getByIndex(0) == first_candle
    assert candleSeries.getByIndex(1) == second_candle

def test_candle_series_get_by_index_out_of_range(asset_pair, first_candle):
    candleSeries = CandleSeries(asset_pair)
    candleSeries.appendRight(first_candle)
    assert candleSeries.getByIndex(1) is None
    assert candleSeries.getByIndex(10) is None

def test_candle_series_get_by_timestamp_exact_match(asset_pair, first_candle, second_candle):
    candleSeries = CandleSeries(asset_pair)
    candleSeries.appendRight(first_candle)
    candleSeries.appendRight(second_candle)
    assert candleSeries.getByTimestamp(100) == first_candle
    assert candleSeries.getByTimestamp(160) == second_candle

def test_candle_series_get_by_timestamp_within_range(asset_pair, first_candle, second_candle):
    candleSeries = CandleSeries(asset_pair)
    candleSeries.appendRight(first_candle)
    candleSeries.appendRight(second_candle)
    # Timestamp 130 is within first_candle's range (100-160)
    assert candleSeries.getByTimestamp(130) == first_candle
    # Timestamp 190 is within second_candle's range (160-220)
    assert candleSeries.getByTimestamp(190) == second_candle

def test_candle_series_get_by_timestamp_binary_search_five_candles(asset_pair):
    # 5-candle series exercises all binary search branches:
    # - move left pointer right (line 118): timestamp in upper half
    # - move right pointer left (line 120): timestamp in lower half, mid overshoots
    # Candles: 100, 160, 220, 280, 340 (each adjacent, interval=60)
    assetPair = AssetPair(Asset("RUB", 10), Asset("USD", 100))
    c1 = Candle(assetPair, 100, IntervalType.OneMinute, 1.0, 1.0, 1.0, 1.0, 1.0)
    c2 = Candle(assetPair, 160, IntervalType.OneMinute, 1.0, 1.0, 1.0, 1.0, 1.0)
    c3 = Candle(assetPair, 220, IntervalType.OneMinute, 1.0, 1.0, 1.0, 1.0, 1.0)
    c4 = Candle(assetPair, 280, IntervalType.OneMinute, 1.0, 1.0, 1.0, 1.0, 1.0)
    c5 = Candle(assetPair, 340, IntervalType.OneMinute, 1.0, 1.0, 1.0, 1.0, 1.0)
    candleSeries = CandleSeries(assetPair)
    candleSeries.appendRight(c1)
    candleSeries.appendRight(c2)
    candleSeries.appendRight(c3)
    candleSeries.appendRight(c4)
    candleSeries.appendRight(c5)
    # timestamp=290: mid=2 (220), next[3]=280 <= 290 → left=3 (line 118)
    #                mid=3 (280), next[4]=340 > 290 → return 3 (line 116)
    assert candleSeries.getByTimestamp(290) == c4
    # timestamp=130: mid=2 (220), 220 > 130 → right=1 (line 120)
    #                mid=0 (100), next[1]=160 > 130 → return 0 (line 116)
    assert candleSeries.getByTimestamp(130) == c1

def test_candle_series_get_by_timestamp_before_first(asset_pair, first_candle):
    candleSeries = CandleSeries(asset_pair)
    candleSeries.appendRight(first_candle)
    assert candleSeries.getByTimestamp(50) is None

def test_candle_series_get_by_timestamp_after_last(asset_pair, first_candle):
    candleSeries = CandleSeries(asset_pair)
    candleSeries.appendRight(first_candle)
    assert candleSeries.getByTimestamp(200) == first_candle

def test_candle_series_string_empty(asset_pair):
    candleSeries = CandleSeries(asset_pair)
    seriesStr = str(candleSeries)
    assert "📋" in seriesStr
    assert candleSeries.getId() in seriesStr

def test_candle_series_string_with_candles(asset_pair, first_candle, second_candle):
    candleSeries = CandleSeries(asset_pair)
    candleSeries.appendRight(first_candle)
    candleSeries.appendRight(second_candle)
    seriesStr = str(candleSeries)
    assert "📋" in seriesStr
    assert "1." in seriesStr
    assert "2." in seriesStr
    assert "100" in seriesStr
    assert "160" in seriesStr
