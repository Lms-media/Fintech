import os
import pytest
from Services import FileLogger

@pytest.fixture
def tmp_log_file(tmp_path):
    return str(tmp_path / "test.log")

def test_file_logger_init_creates_file(tmp_log_file):
    logger = FileLogger(tmp_log_file)
    logger.init()
    assert os.path.exists(tmp_log_file)

def test_file_logger_init_clears_existing_file(tmp_log_file):
    with open(tmp_log_file, 'w') as f:
        f.write("old content")
    logger = FileLogger(tmp_log_file)
    logger.init()
    with open(tmp_log_file, 'r') as f:
        content = f.read()
    assert content == ""

def test_file_logger_log_writes_to_file(tmp_log_file):
    logger = FileLogger(tmp_log_file)
    logger.init()
    logger.log("hello")
    with open(tmp_log_file, 'r', encoding='utf-8') as f:
        content = f.read()
    assert "hello" in content

def test_file_logger_log_appends_multiple(tmp_log_file):
    logger = FileLogger(tmp_log_file)
    logger.init()
    logger.log("first")
    logger.log("second")
    with open(tmp_log_file, 'r', encoding='utf-8') as f:
        content = f.read()
    assert "first" in content
    assert "second" in content

def test_file_logger_log_multiline_chunk(tmp_log_file):
    logger = FileLogger(tmp_log_file)
    logger.init()
    logger.log("line1\nline2")
    with open(tmp_log_file, 'r', encoding='utf-8') as f:
        content = f.read()
    assert "line1" in content
    assert "line2" in content
