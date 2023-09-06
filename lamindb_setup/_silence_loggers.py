import logging


def silence_loggers():
    import botocore

    # this gets logged 6 times
    botocore.session.set_stream_logger(
        name="botocore.credentials", level=logging.WARNING
    )
    botocore.session.set_stream_logger(name="botocore.hooks", level=logging.WARNING)
    botocore.session.set_stream_logger(name="botocore.utils", level=logging.WARNING)
    botocore.session.set_stream_logger(name="botocore.auth", level=logging.WARNING)
    botocore.session.set_stream_logger(name="botocore.endpoint", level=logging.WARNING)
    try:
        import aibotocore  # noqa

        # the 7th logging message of credentials came from aiobotocore
        botocore.session.set_stream_logger(
            name="aiobotocore.credentials", level=logging.WARNING
        )
    except ImportError:
        pass
