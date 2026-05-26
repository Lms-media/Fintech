import copy
import pytest
from ValueObjects import Range

def test_range_stores_from_timestamp():
    asset = Range(10, 20)
    assert asset.getFromTimestamp() == 10

def test_range_stores_to_timestamp():
    asset = Range(10, 20)
    assert asset.getToTimestamp() == 20

def test_range_to_less_than_from():
    with pytest.raises(ValueError):
        Range(20, 10)

def test_range_to_equal_from():
    range = Range(10, 10)
    assert range.getFromTimestamp() == 10
    assert range.getToTimestamp() == 10

def test_range_zero_from_timestamp():
    range = Range(0, 10)
    assert range.getFromTimestamp() == 0

def test_range_negative_from_timestamp():
    with pytest.raises(ValueError):
        Range(-10, 10)

def test_range_zero_to_timestamp():
    range = Range(0, 0)
    assert range.getToTimestamp() == 0

def test_range_negative_to_timestamp():
    with pytest.raises(ValueError):
        Range(10, -10)

def test_range_duration():
    range = Range(10, 30)
    assert range.getDuration() == 20

def test_range_with_from_timestamp():
    range = Range(10, 20)
    newRange = range.withFromTimestamp(5)
    assert newRange.getFromTimestamp() == 5
    assert range.getFromTimestamp() == 10

def test_range_with_from_timestamp_greater_than_to():
    range = Range(10, 20)
    with pytest.raises(ValueError):
        range.withFromTimestamp(30)

def test_range_with_zero_from_timestamp():
    range = Range(10, 20)
    newRange = range.withFromTimestamp(0)
    assert newRange.getFromTimestamp() == 0
    assert range.getFromTimestamp() == 10

def test_range_with_negative_from_timestamp():
    range = Range(10, 20)
    with pytest.raises(ValueError):
        range.withFromTimestamp(-10)

def test_range_with_to_timestamp():
    range = Range(10, 20)
    newRange = range.withToTimestamp(30)
    assert newRange.getToTimestamp() == 30
    assert range.getToTimestamp() == 20

def test_range_with_to_timestamp_less_than_from():
    range = Range(10, 20)
    with pytest.raises(ValueError):
        range.withToTimestamp(5)

def test_range_with_zero_to_timestamp():
    range = Range(0, 20)
    newRange = range.withToTimestamp(0)
    assert newRange.getToTimestamp() == 0
    assert range.getToTimestamp() == 20

def test_range_includes_internal_timestamp():
    range = Range(10, 20)
    assert range.includes(15)

def test_range_includes_from_timestamp():
    range = Range(10, 20)
    assert range.includes(10)

def test_range_includes_to_timestamp():
    range = Range(10, 20)
    assert range.includes(20)

def test_range_not_includes_external_timestamp():
    range = Range(10, 20)
    assert not range.includes(30)

def test_range_compare_other_type():
    assert not Range(10, 20) == 10

def test_range_compare_equivalent_range():
    assert Range(10, 20) == Range(10, 20)

def test_range_compare_different_from_timestamp():
    assert Range(10, 40) != Range(30, 40)

def test_range_compare_different_to_timestamp():
    assert Range(10, 20) != Range(10, 30)

def test_range_equal_hashes():
    assert hash(Range(10, 20)) == hash(Range(10, 20))

def test_range_not_equal_hashes():
    assert hash(Range(10, 20)) != hash(Range(30, 40))

def test_range_copy():
    original = Range(10, 20)
    duplicate = copy.copy(original)
    assert duplicate == original
    assert duplicate is not original

def test_range_string_representation():
    assert str(Range(10, 20)) == "[10, 20]"
