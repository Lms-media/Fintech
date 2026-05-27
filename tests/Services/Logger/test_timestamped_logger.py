import pytest
from Services import TimestampedLogger
from Interfaces import ILogger


class CapturingLogger(ILogger):
    def __init__(self):
        self.inited = False
        self.messages = []

    def init(self):
        self.inited = True

    def log(self, chunk: str):
        self.messages.append(chunk)


@pytest.fixture
def inner():
    return CapturingLogger()


def test_timestamped_logger_init_delegates(inner):
    logger = TimestampedLogger(inner)
    logger.init()
    assert inner.inited

def test_timestamped_logger_log_prepends_timestamp(inner):
    logger = TimestampedLogger(inner)
    logger.log("hello")
    assert len(inner.messages) == 1
    assert "hello" in inner.messages[0]
    assert "[" in inner.messages[0]  # timestamp bracket

def test_timestamped_logger_log_multiple(inner):
    logger = TimestampedLogger(inner)
    logger.log("first")
    logger.log("second")
    assert len(inner.messages) == 2
    assert "first" in inner.messages[0]
    assert "second" in inner.messages[1]
