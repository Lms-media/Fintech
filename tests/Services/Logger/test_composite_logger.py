import pytest
from Services import CompositeLogger
from tests.mocks import MockedLogger

@pytest.fixture
def first_logger():
    return MockedLogger()

@pytest.fixture
def second_logger():
    return MockedLogger()

def test_composite_logger_init_calls_all(first_logger, second_logger):
    logger = CompositeLogger([first_logger, second_logger])
    logger.init()
    assert first_logger.inited
    assert second_logger.inited

def test_composite_logger_log_calls_all(first_logger, second_logger):
    logger = CompositeLogger([first_logger, second_logger])
    logger.log("hello")
    assert first_logger.messages == ["hello"]
    assert second_logger.messages == ["hello"]

def test_composite_logger_log_multiple(first_logger, second_logger):
    logger = CompositeLogger([first_logger, second_logger])
    logger.log("first")
    logger.log("second")
    assert first_logger.messages == ["first", "second"]
    assert second_logger.messages == ["first", "second"]

def test_composite_logger_empty_list():
    logger = CompositeLogger([])
    logger.init()
    logger.log("x")
