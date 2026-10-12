from lamindb_setup import logger


def test_logger():
    logger.set_verbosity(2)
    assert logger._verbosity == 2
