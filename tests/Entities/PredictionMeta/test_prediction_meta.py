import pytest
from Entities import CandleSeries, PredictionMeta
from ValueObjects import AssetPair, Asset


@pytest.fixture
def asset_pair():
    return AssetPair(Asset("RUB", 10), Asset("USD", 100))


@pytest.fixture
def candle_series(asset_pair):
    return CandleSeries(asset_pair)


def test_prediction_meta_generates_unique_id(candle_series):
    first = PredictionMeta(1000, candle_series, 0.9)
    second = PredictionMeta(1000, candle_series, 0.9)
    assert first.getId() != second.getId()

def test_prediction_meta_stores_timestamp(candle_series):
    meta = PredictionMeta(1000, candle_series, 0.9)
    assert meta.getTimestamp() == 1000

def test_prediction_meta_stores_zero_timestamp(candle_series):
    meta = PredictionMeta(0, candle_series, 0.9)
    assert meta.getTimestamp() == 0

def test_prediction_meta_stores_candle_series(candle_series):
    meta = PredictionMeta(1000, candle_series, 0.9)
    assert meta.getCandleSeries() == candle_series

def test_prediction_meta_stores_confidence(candle_series):
    meta = PredictionMeta(1000, candle_series, 0.9)
    assert meta.getConfidence() == 0.9

def test_prediction_meta_stores_zero_confidence(candle_series):
    meta = PredictionMeta(1000, candle_series, 0.0)
    assert meta.getConfidence() == 0.0

def test_prediction_meta_stores_full_confidence(candle_series):
    meta = PredictionMeta(1000, candle_series, 1.0)
    assert meta.getConfidence() == 1.0

def test_prediction_meta_negative_timestamp_raises(candle_series):
    with pytest.raises(ValueError):
        PredictionMeta(-1, candle_series, 0.9)

def test_prediction_meta_negative_confidence_raises(candle_series):
    with pytest.raises(ValueError):
        PredictionMeta(1000, candle_series, -0.1)

def test_prediction_meta_confidence_above_one_raises(candle_series):
    with pytest.raises(ValueError):
        PredictionMeta(1000, candle_series, 1.1)

def test_prediction_meta_string_contains_id(candle_series):
    meta = PredictionMeta(1000, candle_series, 0.9)
    metaStr = str(meta)
    assert meta.getId() in metaStr

def test_prediction_meta_string_contains_emoji(candle_series):
    meta = PredictionMeta(1000, candle_series, 0.9)
    metaStr = str(meta)
    assert "📝" in metaStr

def test_prediction_meta_string_contains_timestamp(candle_series):
    meta = PredictionMeta(1000, candle_series, 0.9)
    metaStr = str(meta)
    assert "1000" in metaStr

def test_prediction_meta_string_contains_confidence(candle_series):
    meta = PredictionMeta(1000, candle_series, 0.9)
    metaStr = str(meta)
    assert "0.9" in metaStr
