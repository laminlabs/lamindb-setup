from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from lamindb_setup.core._hub_client import (
    _warn_if_api_key_expiring,
    call_with_fallback_auth,
    get_access_token,
)
from lamindb_setup.errors import ApiKeyError, ApiKeyExpired, ApiKeyNotFound
from supabase_functions.errors import FunctionsHttpError

NOW = datetime(2026, 8, 25, 17, 23, 14, 401936, tzinfo=timezone.utc)


def _supabase_expires_at(days: int) -> str:
    dt = NOW + timedelta(days=days)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f") + "+00"


@pytest.mark.parametrize(
    ("expires_at", "expected"),
    [
        (_supabase_expires_at(0), "API key expires in 0 days"),
        (_supabase_expires_at(1), "API key expires in 1 day"),
        (_supabase_expires_at(7), "API key expires in 7 days"),
        (_supabase_expires_at(8), None),
        (_supabase_expires_at(-1), None),
        ("2026-08-11 17:23:14.401936+00", None),
        ("not-a-date", None),
    ],
)
def test_warn_if_api_key_expiring(expires_at, expected):
    with (
        patch(
            "lamindb_setup.core._hub_client.datetime", wraps=datetime
        ) as mock_datetime,
        patch("lamindb_setup.core._hub_client.logger.warning") as warning,
    ):
        mock_datetime.now.return_value = NOW
        _warn_if_api_key_expiring(expires_at)
    if expected is None:
        warning.assert_not_called()
    else:
        warning.assert_called_once_with(expected)


@pytest.mark.parametrize(
    ("hub_message", "error_type"),
    [
        ("API key has expired.", ApiKeyExpired),
        ("API key not found.", ApiKeyNotFound),
    ],
)
def test_get_access_token_raises_api_key_error(hub_message, error_type):
    hub = MagicMock()
    hub.functions.invoke.side_effect = FunctionsHttpError(hub_message)
    with (
        patch("lamindb_setup.core._hub_client.connect_hub", return_value=hub),
        pytest.raises(error_type) as exc_info,
    ):
        get_access_token(api_key="bad-key")
    assert isinstance(exc_info.value, ApiKeyError)
    assert exc_info.value.__cause__ is None
    hub.auth.sign_out.assert_called_once()


def test_get_access_token_does_not_treat_jwt_expired_as_api_key_error():
    hub = MagicMock()
    hub.functions.invoke.side_effect = FunctionsHttpError("JWT expired")
    with (
        patch("lamindb_setup.core._hub_client.connect_hub", return_value=hub),
        pytest.raises(FunctionsHttpError, match="JWT expired"),
    ):
        get_access_token(api_key="some-key")


@pytest.mark.parametrize("error_type", [ApiKeyExpired, ApiKeyNotFound])
def test_call_with_fallback_auth_stops_retries_on_api_key_error(error_type):
    api_key_error = error_type()
    calls: list[dict] = []

    def fake_connect(**kwargs):
        calls.append(kwargs)
        if kwargs.get("renew_token"):
            raise api_key_error
        return MagicMock()

    def fail_with_jwt(*, client):
        raise FunctionsHttpError("JWT expired")

    with (
        patch(
            "lamindb_setup.core._hub_client.connect_hub_with_auth",
            side_effect=fake_connect,
        ),
        pytest.raises(error_type) as exc_info,
    ):
        call_with_fallback_auth(fail_with_jwt)

    assert exc_info.value is api_key_error
    assert exc_info.value.__cause__ is None
    assert len(calls) == 2
    assert calls[0]["renew_token"] is False
    assert calls[1]["renew_token"] is True
    assert calls[1]["fallback_env"] is False
