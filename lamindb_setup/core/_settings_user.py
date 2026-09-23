from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from uuid import UUID


@dataclass
class AccessToken:
    """JWT access token."""

    access_token: str
    """JWT access token."""

    def __post_init__(self):
        self.expiration: float | None = self._get_expiration(self.access_token)

    def needs_refresh(self) -> bool:
        return self.expiration is not None and time.time() >= self.expiration

    @staticmethod
    def _get_expiration(access_token: str) -> float | None:
        from jwt import decode

        # refresh 15 minutes early
        # an unreadable token is left for call_with_fallback_auth
        try:
            exp = decode(
                access_token, options={"verify_signature": False, "verify_exp": False}
            )["exp"]
            return exp - 900
        except Exception:
            return None


class UserSettings:
    """User data. All synched from cloud."""

    handle: str
    """Unique handle."""
    email: str | None
    """User email."""
    api_key: str | None
    """API key."""
    password: str | None
    """Legacy password."""
    access_token: str | None
    """User access token."""
    uid: str
    """Universal user ID."""
    _uuid: UUID | None
    """Lamin's internal user ID."""
    name: str | None
    """Full name."""

    def __init__(
        self,
        handle: str = "anonymous",
        email: str | None = None,
        api_key: str | None = None,
        password: str | None = None,
        access_token: str | None = None,
        uid: str = "null",
        _uuid: UUID | None = None,
        name: str | None = None,
    ):
        self.handle = handle
        self.email = email
        self.api_key = api_key
        self.password = password
        self.uid = uid
        self._uuid = _uuid
        self.name = name

        self._access_token: AccessToken | None = None
        # passes through the setter
        self.access_token = access_token

    @property  # type: ignore[no-redef]
    def access_token(self) -> str | None:
        """User access token."""
        if (_access_token := self._access_token) is not None:
            return _access_token.access_token
        return None

    @access_token.setter
    def access_token(self, value: str | None):
        token: AccessToken | None = AccessToken(value) if value is not None else None
        self._access_token = token

    def access_token_needs_refresh(self) -> bool:
        return (
            _access_token := self._access_token
        ) is not None and _access_token.needs_refresh()

    @property
    def id(self):
        """Integer id valid in current instance."""
        from lamindb.base.users import current_user_id

        # there is no cache needed here because current_user_id()
        # has its own cache
        return current_user_id()

    def asdict(self) -> dict:
        """Convert to dictionary.

        This does not refresh the access token.
        """
        return {
            "handle": self.handle,
            "email": self.email,
            "api_key": self.api_key,
            "password": self.password,
            "access_token": self.access_token,
            "uid": self.uid,
            "_uuid": self._uuid,
            "name": self.name,
        }

    def __repr__(self) -> str:
        """Rich string representation."""
        representation = "Current user:"
        attrs = ["handle", "uid"]
        for attr in attrs:
            value = getattr(self, attr)
            representation += f"\n - {attr}: {value}"
        return representation
