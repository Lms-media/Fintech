from Services import PrintLogger

def test_print_logger_init_does_not_raise():
    logger = PrintLogger()
    logger.init()

def test_print_logger_log_prints(capsys):
    logger = PrintLogger()
    logger.log("hello")
    captured = capsys.readouterr()
    assert "hello" in captured.out

def test_print_logger_log_multiple(capsys):
    logger = PrintLogger()
    logger.log("first")
    logger.log("second")
    captured = capsys.readouterr()
    assert "first" in captured.out
    assert "second" in captured.out
