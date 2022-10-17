from dataclasses import dataclass
from typing import Union


class user_description:
    email = """User email."""
    password = """User password."""
    id = """User ID."""
    handle = "Unique handle."
    name = "Full name."


@dataclass
class UserSettings:
    """User data. All synched from cloud."""

    email: str = None  # type: ignore
    """User email."""
    password: Union[str, None] = None
    """User password."""
    access_token: Union[str, None] = None
    """User access token."""
    id: Union[str, None] = None
    """User ID."""
    handle: Union[str, None] = None
    "Unique handle."
    name: Union[str, None] = None
    "Full name."
