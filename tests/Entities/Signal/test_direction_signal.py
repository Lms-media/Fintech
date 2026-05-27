import pytest
from Entities import CandleSeries, NextCandlePrediction, PredictionMeta, DirectionSignal
from ValueObjects import Candle, AssetPair, Asset
from Interfaces import IntervalType, DirectionType

@pytest.fixture
def asset_pair():
    return AssetPair(Asset("RUB", 10), Asset("USD", 100))

@pytest.fixture
def candle_series(asset_pair):
    return CandleSeries(asset_pair)

@pytest.fixture
def next_candle(asset_pair):
    return Candle(asset_pair, 160, IntervalType.OneMinute, 105.0, 110.0, 115.0, 100.0, 1100.0)

@pytest.fixture
def meta(candle_series):
    return PredictionMeta(1000, candle_series, 0.9)

@pytest.fixture
def prediction(meta, next_candle):
    return NextCandlePrediction(meta, next_candle)

@pytest.fixture
def signal_up(prediction):
    return DirectionSignal(1000, prediction, 10.0, DirectionType.Up)

def test_direction_signal_generates_unique_id(prediction):
    first = DirectionSignal(1000, prediction, 10.0, DirectionType.Up)
    second = DirectionSignal(1000, prediction, 10.0, DirectionType.Up)
    assert first.getId() != second.getId()

def test_direction_signal_stores_timestamp(signal_up):
    assert signal_up.getTimestamp() == 1000

def test_direction_signal_stores_zero_timestamp(prediction):
    signal = DirectionSignal(0, prediction, 10.0, DirectionType.Up)
    assert signal.getTimestamp() == 0

def test_direction_signal_negative_timestamp_raises(prediction):
    with pytest.raises(ValueError):
        DirectionSignal(-1, prediction, 10.0, DirectionType.Up)

def test_direction_signal_stores_prediction(signal_up, prediction):
    assert signal_up.getPrediction() == prediction

def test_direction_signal_stores_volume(signal_up):
    assert signal_up.getVolume() == 10.0

def test_direction_signal_stores_direction_up(signal_up):
    assert signal_up.getDirection() == DirectionType.Up

def test_direction_signal_stores_direction_down(prediction):
    signal = DirectionSignal(1000, prediction, 10.0, DirectionType.Down)
    assert signal.getDirection() == DirectionType.Down

def test_direction_signal_string_contains_id(signal_up):
    signalStr = str(signal_up)
    assert signal_up.getId() in signalStr

def test_direction_signal_string_contains_emoji(signal_up):
    signalStr = str(signal_up)
    assert "🪧" in signalStr

def test_direction_signal_string_contains_timestamp(signal_up):
    signalStr = str(signal_up)
    assert "1000" in signalStr

def test_direction_signal_string_contains_volume(signal_up):
    signalStr = str(signal_up)
    assert "10.0" in signalStr

def test_direction_signal_string_contains_direction(signal_up):
    signalStr = str(signal_up)
    assert "Up" in signalStr
