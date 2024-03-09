from dataclasses import dataclass
from typing import Optional
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

    handle: str = "anonymous"
    """Unique handle."""
    email: str = None  # type: ignore
    """User email."""
    password: Optional[str] = None
    """API key or legacy password."""
    access_token: Optional[str] = None
    """User access token."""
    uid: str = "null"
    """Universal user ID."""
    uuid: Optional[UUID] = None
    """Lamin's internal user ID."""
    name: Optional[str] = None
    """Full name."""

    def __repr__(self) -> str:
        """Rich string representation."""
        representation = f"Current user: {self.handle}"
        attrs = ["handle", "email", "uid"]
        for attr in attrs:
            value = getattr(self, attr)
            representation += f"\n- {attr}: {value}"
        return representation

    @property
    def id(self):
        """Integer id valid in current intance."""
        from lnschema_core.users import current_user_id

        return current_user_id()
