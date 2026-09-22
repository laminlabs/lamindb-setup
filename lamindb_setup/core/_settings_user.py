from __future__ import annotations

import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from uuid import UUID


class AccessToken:
    def __init__(self, access_token: str, user_settings: UserSettings):
        self._access_token: str = access_token
        self._expiration: float = self._get_expiration(access_token)
        # needed to refresh access token when it expires
        self._user_settings: UserSettings = user_settings

    @staticmethod
    def _get_expiration(access_token: str) -> float:
        from jwt import decode

        # buffer time of 1 hour
        return decode(access_token, options={"verify_signature": False})["exp"] - 3600

    def _refresh_token(self):
        if time.time() >= self._expiration:
            from ._hub_client import get_access_token

            new_access_token = get_access_token(
                self._user_settings.email,
                self._user_settings.password,
                self._user_settings.api_key,
            )
            if new_access_token is not None:
                self._access_token = new_access_token
                self._expiration = self._get_expiration(new_access_token)

    @property
    def access_token(self) -> str:
        self._refresh_token()
        return self._access_token


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
        # passes through the setter
        self.access_token = access_token

    @property  # type: ignore[no-redef]
    def access_token(self) -> str | None:
        """User access token."""
        refreshable_access_token = self._refreshable_access_token
        if refreshable_access_token is not None:
            return refreshable_access_token.access_token
        return None

    @access_token.setter
    def access_token(self, value: str | None):
        token: AccessToken | None = (
            AccessToken(value, self) if value is not None else None
        )
        self._refreshable_access_token = token

    def __repr__(self) -> str:
        """Rich string representation."""
        representation = "Current user:"
        attrs = ["handle", "uid"]
        for attr in attrs:
            value = getattr(self, attr)
            representation += f"\n - {attr}: {value}"
        return representation

    @property
    def id(self):
        """Integer id valid in current instance."""
        from lamindb.base.users import current_user_id

        # there is no cache needed here because current_user_id()
        # has its own cache
        return current_user_id()
