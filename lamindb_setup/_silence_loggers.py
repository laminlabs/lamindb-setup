from __future__ import annotations

import logging

silenced = False


# from https://github.com/boto/boto3/blob/8c6e641bed8130a9d8cb4d97b4acbe7aa0d0657a/boto3/__init__.py#L37
def set_stream_logger(name, level):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.StreamHandler()
    handler.setLevel(level)
    logger.addHandler(handler)


def silence_loggers():
    global silenced

    if not silenced:
        # this gets logged 6 times
        set_stream_logger(name="botocore.credentials", level=logging.WARNING)
        set_stream_logger(name="botocore.hooks", level=logging.WARNING)
        set_stream_logger(name="botocore.utils", level=logging.WARNING)
        set_stream_logger(name="botocore.auth", level=logging.WARNING)
        set_stream_logger(name="botocore.endpoint", level=logging.WARNING)
        set_stream_logger(name="httpx", level=logging.WARNING)
        try:
            import aiobotocore

            # the 7th logging message of credentials came from aiobotocore
            set_stream_logger(name="aiobotocore.credentials", level=logging.WARNING)
        except ImportError:
            pass
        try:
            # google also aggressively logs authentication related warnings
            # in cases where users access public data
            set_stream_logger(name="google.auth._default", level=logging.ERROR)
            set_stream_logger(
                name="google.auth.compute_engine._metadata", level=logging.ERROR
            )
        except Exception:
            pass
    silenced = True
