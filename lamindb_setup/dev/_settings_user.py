from dataclasses import dataclass
from typing import Union
from uuid import UUID


class user_description:
    email = """User email."""
    password = """API key or legacy password."""
    uid = """Universal user ID."""
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
    uid: str = "null"
    """Universal user ID."""
    uuid: Union[UUID, None] = None
    """Lamin's internal universal user ID."""
    handle: Union[str, None] = None
    """Unique handle."""
    name: Union[str, None] = None
    """Full name."""

    def __repr__(self):
        """Rich string representation."""
        representation = f"Current user: {self.handle}"
        attrs = ["handle", "email", "uid"]
        for attr in attrs:
            value = getattr(self, attr)
            representation += f"\n- {attr}: {value}"
        return representation

    @property
    def id(self):
        from lnschema_core.users import current_user_id

        return current_user_id()
