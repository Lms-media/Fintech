import os
import pytest
from Services import GraphLogger2D

@pytest.fixture
def tmp_img_file(tmp_path):
    return str(tmp_path / "graph.png")

def test_graph_logger_2d_init_creates_file(tmp_img_file):
    logger = GraphLogger2D(tmp_img_file)
    logger.init()
    assert os.path.exists(tmp_img_file)

def test_graph_logger_2d_init_clears_existing_file(tmp_img_file):
    with open(tmp_img_file, 'w') as f:
        f.write("old content")
    logger = GraphLogger2D(tmp_img_file)
    logger.init()
    assert os.path.exists(tmp_img_file)

def test_graph_logger_2d_log_valid_chunk(tmp_img_file):
    logger = GraphLogger2D(tmp_img_file)
    logger.init()
    logger.log("x:1.0;y:2.0")

def test_graph_logger_2d_log_appends_new_series(tmp_img_file):
    logger = GraphLogger2D(tmp_img_file)
    logger.init()
    logger.log("x:1.0;y:2.0")
    logger.log("x:2.0;y:3.0")

def test_graph_logger_2d_log_multiple_series(tmp_img_file):
    logger = GraphLogger2D(tmp_img_file)
    logger.init()
    logger.log("x:1.0;series1:10.0")
    logger.log("x:2.0;series2:20.0")

def test_graph_logger_2d_log_invalid_no_semicolon_raises(tmp_img_file):
    logger = GraphLogger2D(tmp_img_file)
    logger.init()
    with pytest.raises(ValueError):
        logger.log("x:1.0y:2.0")

def test_graph_logger_2d_log_invalid_too_many_parts_raises(tmp_img_file):
    logger = GraphLogger2D(tmp_img_file)
    logger.init()
    with pytest.raises(ValueError):
        logger.log("x:1.0;y:2.0;z:3.0")

def test_graph_logger_2d_log_invalid_x_part_raises(tmp_img_file):
    logger = GraphLogger2D(tmp_img_file)
    logger.init()
    with pytest.raises(ValueError):
        logger.log("1.0;y:2.0")

def test_graph_logger_2d_log_invalid_x_name_raises(tmp_img_file):
    logger = GraphLogger2D(tmp_img_file)
    logger.init()
    with pytest.raises(ValueError):
        logger.log("z:1.0;y:2.0")
