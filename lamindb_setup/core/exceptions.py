from typing import Optional


class DefaultMessageException(Exception):
    default_message: Optional[str] = None

    def __init__(self, message: Optional[str] = None):
        if message is None:
            message = self.default_message
        super().__init__(message)
