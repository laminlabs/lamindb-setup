from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from uuid import UUID


class AccessToken:
    """JWT access token that refreshes itself before expiration."""

    def __init__(self, access_token: str, user_settings: UserSettings):
        self._access_token: str = access_token
        # None if the token is undecodable
        self._expiration: float | None = self._get_expiration(access_token)
        # needed to refresh access token when it expires
        self._user_settings: UserSettings = user_settings
        self._lock = threading.Lock()

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

    def _refresh_token(self):
        with self._lock:
            if self._expiration is None or time.time() < self._expiration:
                return
            from ._hub_client import get_access_token

            new_access_token = get_access_token(
                self._user_settings.email,
                self._user_settings.password,
                self._user_settings.api_key,
            )
            if new_access_token is None:
                return
            self._access_token = new_access_token
            self._expiration = self._get_expiration(new_access_token)

            from ._settings_save import save_user_settings

            save_user_settings(self._user_settings)

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
        # also sets self._refreshable_access_token
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

    @property
    def _access_token(self) -> str | None:
        """Stored access token, without refreshing."""
        refreshable_access_token = self._refreshable_access_token
        if refreshable_access_token is not None:
            return refreshable_access_token._access_token
        return None

    @property
    def id(self):
        """Integer id valid in current instance."""
        from lamindb.base.users import current_user_id

        # there is no cache needed here because current_user_id()
        # has its own cache
        return current_user_id()

    def to_dict(self) -> dict:
        """Convert to dictionary.

        This does not refresh the access token.
        """
        return {
            "handle": self.handle,
            "email": self.email,
            "api_key": self.api_key,
            "password": self.password,
            "access_token": self._access_token,
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
