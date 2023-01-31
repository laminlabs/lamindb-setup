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

    def __repr__(self):
        """Rich string representation."""
        representation = f"Current user: {self.handle}"
        attrs = ["handle", "email", "id"]
        for attr in attrs:
            value = getattr(self, attr)
            representation += f"\n- {attr}: {value}"
        return representation
