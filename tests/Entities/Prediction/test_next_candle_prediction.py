import pytest
from Entities import CandleSeries, NextCandlePrediction, PredictionMeta
from ValueObjects import Candle, AssetPair, Asset
from Interfaces import IntervalType

@pytest.fixture
def asset_pair():
    return AssetPair(Asset("RUB", 10), Asset("USD", 100))

@pytest.fixture
def candle_series(asset_pair):
    return CandleSeries(asset_pair)

@pytest.fixture
def candle(asset_pair):
    return Candle(asset_pair, 100, IntervalType.OneMinute, 100.0, 105.0, 110.0, 95.0, 1000.0)

@pytest.fixture
def meta(candle_series):
    return PredictionMeta(1000, candle_series, 0.9)

@pytest.fixture
def next_candle(asset_pair):
    return Candle(asset_pair, 160, IntervalType.OneMinute, 105.0, 110.0, 115.0, 100.0, 1100.0)

@pytest.fixture
def prediction(meta, next_candle):
    return NextCandlePrediction(meta, next_candle)

def test_next_candle_prediction_generates_unique_id(meta, next_candle):
    first = NextCandlePrediction(meta, next_candle)
    second = NextCandlePrediction(meta, next_candle)
    assert first.getId() != second.getId()

def test_next_candle_prediction_stores_meta(prediction, meta):
    assert prediction.getMeta() == meta

def test_next_candle_prediction_stores_next_candle(prediction, next_candle):
    assert prediction.getNextCandle() == next_candle

def test_next_candle_prediction_string_contains_id(prediction):
    predStr = str(prediction)
    assert prediction.getId() in predStr

def test_next_candle_prediction_string_contains_emoji(prediction):
    predStr = str(prediction)
    assert "🔮" in predStr

def test_next_candle_prediction_string_contains_next_candle(prediction, next_candle):
    predStr = str(prediction)
    assert str(next_candle) in predStr

def test_next_candle_prediction_string_contains_meta(prediction, meta):
    predStr = str(prediction)
    assert str(meta) in predStr
