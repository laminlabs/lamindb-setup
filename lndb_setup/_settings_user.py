from dataclasses import dataclass
from typing import Union


@dataclass
class UserSettings:
    """User Settings written during setup."""

    user_email: str = None  # type: ignore
    """User email."""
    user_secret: Union[str, None] = None
    """User login secret. Auto-generated."""
    user_id: Union[str, None] = None
    """User ID. Auto-generated."""
