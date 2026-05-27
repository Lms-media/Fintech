import pytest
from Entities import CandleSeries, TrimmedCandleSeries
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

@pytest.fixture
def base_series(asset_pair, first_candle, second_candle, third_candle):
    series = CandleSeries(asset_pair)
    series.appendRight(first_candle)
    series.appendRight(second_candle)
    series.appendRight(third_candle)
    return series

def test_trimmed_candle_series_generates_unique_id(base_series):
    first = TrimmedCandleSeries(base_series, 0, 2)
    second = TrimmedCandleSeries(base_series, 0, 2)
    assert first.getId() != second.getId()

def test_trimmed_candle_series_get_count(base_series):
    trimmed = TrimmedCandleSeries(base_series, 0, 2)
    assert trimmed.getCount() == 2

def test_trimmed_candle_series_get_count_single(base_series):
    trimmed = TrimmedCandleSeries(base_series, 1, 2)
    assert trimmed.getCount() == 1

def test_trimmed_candle_series_get_count_empty(base_series):
    trimmed = TrimmedCandleSeries(base_series, 1, 1)
    assert trimmed.getCount() == 0

def test_trimmed_candle_series_get_asset_pair(asset_pair, base_series):
    trimmed = TrimmedCandleSeries(base_series, 0, 2)
    assert trimmed.getAssetPair() == asset_pair

def test_trimmed_candle_series_get_by_index_first(base_series, first_candle):
    trimmed = TrimmedCandleSeries(base_series, 0, 2)
    assert trimmed.getByIndex(0) == first_candle

def test_trimmed_candle_series_get_by_index_second(base_series, second_candle):
    trimmed = TrimmedCandleSeries(base_series, 0, 2)
    assert trimmed.getByIndex(1) == second_candle

def test_trimmed_candle_series_get_by_index_with_offset(base_series, second_candle, third_candle):
    trimmed = TrimmedCandleSeries(base_series, 1, 3)
    assert trimmed.getByIndex(0) == second_candle
    assert trimmed.getByIndex(1) == third_candle

def test_trimmed_candle_series_get_by_index_out_of_range(base_series):
    trimmed = TrimmedCandleSeries(base_series, 0, 2)
    assert trimmed.getByIndex(2) is None
    assert trimmed.getByIndex(10) is None

def test_trimmed_candle_series_get_by_index_negative(base_series):
    trimmed = TrimmedCandleSeries(base_series, 0, 2)
    assert trimmed.getByIndex(-1) is None

def test_trimmed_candle_series_get_by_timestamp_within_range(base_series, first_candle):
    trimmed = TrimmedCandleSeries(base_series, 0, 1)
    assert trimmed.getByTimestamp(100) == first_candle
    assert trimmed.getByTimestamp(130) == first_candle

def test_trimmed_candle_series_get_by_timestamp_exact_from(base_series, first_candle):
    trimmed = TrimmedCandleSeries(base_series, 0, 1)
    assert trimmed.getByTimestamp(100) == first_candle

def test_trimmed_candle_series_get_by_timestamp_before_range(base_series):
    trimmed = TrimmedCandleSeries(base_series, 0, 1)
    assert trimmed.getByTimestamp(50) is None

def test_trimmed_candle_series_get_by_timestamp_after_range(base_series):
    trimmed = TrimmedCandleSeries(base_series, 0, 1)
    assert trimmed.getByTimestamp(220) is None
    assert trimmed.getByTimestamp(300) is None

def test_trimmed_candle_series_get_by_timestamp_no_from_candle(asset_pair):
    emptySeries = CandleSeries(asset_pair)
    trimmed = TrimmedCandleSeries(emptySeries, 0, 1)
    assert trimmed.getByTimestamp(100) is None

def test_trimmed_candle_series_get_by_timestamp_no_to_candle(base_series):
    trimmed = TrimmedCandleSeries(base_series, 0, 3)
    assert trimmed.getByTimestamp(100) is None

def test_trimmed_candle_series_string_empty(base_series):
    trimmed = TrimmedCandleSeries(base_series, 1, 1)
    seriesStr = str(trimmed)
    assert "📋" in seriesStr
    assert trimmed.getId() in seriesStr

def test_trimmed_candle_series_string_with_candles(base_series, second_candle, third_candle):
    trimmed = TrimmedCandleSeries(base_series, 1, 3)
    seriesStr = str(trimmed)
    assert "📋" in seriesStr
    assert "1." in seriesStr
    assert "2." in seriesStr
    assert "160" in seriesStr
    assert "220" in seriesStr
