from __future__ import annotations

from typing import Optional


class DefaultMessageException(Exception):
    default_message: str | None = None

    def __init__(self, message: str | None = None):
        if message is None:
            message = self.default_message
        super().__init__(message)
