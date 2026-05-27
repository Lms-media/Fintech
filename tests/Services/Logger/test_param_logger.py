import pytest
from Services import ParamLogger
from tests.mocks import MockedLogger

@pytest.fixture
def inner():
    return MockedLogger()

def test_param_logger_init_delegates(inner):
    logger = ParamLogger(inner, 0.0, 1.0)
    logger.init()
    assert inner.inited

def test_param_logger_log_formats_chunk(inner):
    logger = ParamLogger(inner, 0.0, 1.0)
    logger.log("42.0")
    assert inner.messages[0] == "x:0.0;y:42.0"

def test_param_logger_log_increments_value(inner):
    logger = ParamLogger(inner, 0.0, 1.0)
    logger.log("10.0")
    logger.log("20.0")
    assert inner.messages[0] == "x:0.0;y:10.0"
    assert inner.messages[1] == "x:1.0;y:20.0"

def test_param_logger_log_custom_start_and_step(inner):
    logger = ParamLogger(inner, 5.0, 2.5)
    logger.log("1.0")
    logger.log("2.0")
    assert inner.messages[0] == "x:5.0;y:1.0"
    assert inner.messages[1] == "x:7.5;y:2.0"
