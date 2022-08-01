from dataclasses import dataclass
from typing import Union


class user_description:
    email = """User email."""
    password = """User login password. Auto-generated if not provided."""
    id = """User ID. Auto-generated."""
    handle = "Unique handle. Like a Twitter handle or a GitHub username."


@dataclass
class UserSettings:
    """User Settings written during setup."""

    email: str = None  # type: ignore
    """User email."""
    password: Union[str, None] = None
    """User login password. Auto-generated if not provided."""
    id: Union[str, None] = None
    """User ID. Auto-generated."""
    handle: Union[str, None] = None
    "Unique handle. Like a Twitter handle or a GitHub username."
