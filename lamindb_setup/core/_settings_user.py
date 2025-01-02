from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
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
    email: str | None = None
    """User email."""
    api_key: str | None = None
    """API key."""
    password: str | None = None
    """legacy API key or legacy password."""
    access_token: str | None = None
    """User access token."""
    uid: str = "null"
    """Universal user ID."""
    _uuid: UUID | None = None
    """Lamin's internal user ID."""
    name: str | None = None
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
        from lamindb.base.users import current_user_id

        # there is no cache needed here because current_user_id()
        # has its own cache
        return current_user_id()
