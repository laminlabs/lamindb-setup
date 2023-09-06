import logging


def silence_loggers():
    from botocore import session

    # this gets logged 6 times
    session.set_stream_logger(name="botocore.credentials", level=logging.WARNING)
    session.set_stream_logger(name="botocore.hooks", level=logging.WARNING)
    session.set_stream_logger(name="botocore.utils", level=logging.WARNING)
    session.set_stream_logger(name="botocore.auth", level=logging.WARNING)
    session.set_stream_logger(name="botocore.endpoint", level=logging.WARNING)
    try:
        import aibotocore  # noqa

        # the 7th logging message of credentials came from aiobotocore
        session.set_stream_logger(name="aiobotocore.credentials", level=logging.WARNING)
    except ImportError:
        pass
