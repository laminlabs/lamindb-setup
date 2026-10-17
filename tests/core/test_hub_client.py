from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from lamindb_setup.core._hub_client import (
    _warn_if_api_key_expiring,
    call_with_fallback_auth,
    get_access_token,
    request_with_auth,
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

    settings = MagicMock()
    settings.user.access_token_needs_refresh.return_value = False
    with (
        patch(
            "lamindb_setup.core._hub_client.connect_hub_with_auth",
            side_effect=fake_connect,
        ),
        patch("lamindb_setup.settings", settings),
        pytest.raises(error_type) as exc_info,
    ):
        call_with_fallback_auth(fail_with_jwt)

    assert exc_info.value is api_key_error
    assert exc_info.value.__cause__ is None
    assert len(calls) == 2
    assert calls[0]["renew_token"] is False
    assert calls[1]["renew_token"] is True
    assert calls[1]["fallback_env"] is False


@pytest.mark.parametrize("error_type", [ApiKeyExpired, ApiKeyNotFound])
def test_call_with_fallback_auth_renews_before_call_when_token_needs_refresh(
    error_type,
):
    api_key_error = error_type()
    calls: list[dict] = []

    def fake_connect(**kwargs):
        calls.append(kwargs)
        if kwargs.get("renew_token"):
            raise api_key_error
        return MagicMock()

    settings = MagicMock()
    settings.user.access_token_needs_refresh.return_value = True
    with (
        patch(
            "lamindb_setup.core._hub_client.connect_hub_with_auth",
            side_effect=fake_connect,
        ),
        patch("lamindb_setup.settings", settings),
        pytest.raises(error_type) as exc_info,
    ):
        call_with_fallback_auth(lambda *, client: None)

    assert exc_info.value is api_key_error
    assert len(calls) == 1
    assert calls[0]["renew_token"] is True
    assert calls[0]["fallback_env"] is False


def test_request_with_auth_refreshes_on_primary_before_request():
    response = MagicMock()
    client = MagicMock()
    client.get.return_value = response
    settings = MagicMock()
    settings.user.access_token_needs_refresh.return_value = True

    with (
        patch("lamindb_setup.core._hub_client.httpx_client") as httpx_client,
        patch(
            "lamindb_setup.core._hub_client._renew_save_token", return_value="new-token"
        ) as renew,
        patch("lamindb_setup.settings", settings),
    ):
        httpx_client.return_value.__enter__.return_value = client
        result = request_with_auth("https://example.test", "get", "old-token", True)

    assert result is response
    renew.assert_called_once_with(fallback_env=False)
    client.get.assert_called_once_with(
        "https://example.test",
        headers={"Authorization": "Bearer new-token"},
        timeout=12,
    )


def test_request_with_auth_uses_fallback_after_primary_mint_fails():
    response = MagicMock()
    client = MagicMock()
    client.get.return_value = response
    settings = MagicMock()
    settings.user.access_token_needs_refresh.return_value = True

    def renew(*, fallback_env):
        if not fallback_env:
            raise RuntimeError("primary down")
        return "fallback-token"

    with (
        patch("lamindb_setup.core._hub_client.httpx_client") as httpx_client,
        patch("lamindb_setup.core._hub_client._renew_save_token", side_effect=renew),
        patch("lamindb_setup.settings", settings),
    ):
        httpx_client.return_value.__enter__.return_value = client
        result = request_with_auth("https://example.test", "get", "old-token", True)

    assert result is response
    client.get.assert_called_once_with(
        "https://example.test",
        headers={"Authorization": "Bearer fallback-token"},
        timeout=12,
    )


def test_request_with_auth_keeps_token_when_both_mints_fail():
    response = MagicMock()
    client = MagicMock()
    client.get.return_value = response
    settings = MagicMock()
    settings.user.access_token_needs_refresh.return_value = True

    with (
        patch("lamindb_setup.core._hub_client.httpx_client") as httpx_client,
        patch(
            "lamindb_setup.core._hub_client._renew_save_token",
            side_effect=RuntimeError("down"),
        ),
        patch("lamindb_setup.settings", settings),
    ):
        httpx_client.return_value.__enter__.return_value = client
        result = request_with_auth("https://example.test", "get", "old-token", True)

    assert result is response
    client.get.assert_called_once_with(
        "https://example.test",
        headers={"Authorization": "Bearer old-token"},
        timeout=12,
    )


def test_request_with_auth_does_not_try_fallback_for_api_key_error():
    settings = MagicMock()
    settings.user.access_token_needs_refresh.return_value = True

    with (
        patch("lamindb_setup.core._hub_client.httpx_client") as httpx_client,
        patch(
            "lamindb_setup.core._hub_client._renew_save_token",
            side_effect=ApiKeyExpired(),
        ) as renew,
        patch("lamindb_setup.settings", settings),
        pytest.raises(ApiKeyExpired),
    ):
        request_with_auth("https://example.test", "get", "old-token", True)

    renew.assert_called_once_with(fallback_env=False)
    httpx_client.assert_not_called()


def test_request_with_auth_refreshes_unreadable_token():
    from lamindb_setup.core._settings_user import UserSettings

    user = UserSettings(handle="tester", access_token="header.payload.signature")
    response = MagicMock()
    client = MagicMock()
    client.get.return_value = response
    settings = MagicMock()
    settings.user = user

    with (
        patch("lamindb_setup.core._hub_client.httpx_client") as httpx_client,
        patch(
            "lamindb_setup.core._hub_client._renew_save_token", return_value="new-token"
        ) as renew,
        patch("lamindb_setup.settings", settings),
    ):
        httpx_client.return_value.__enter__.return_value = client
        result = request_with_auth(
            "https://example.test", "get", user.access_token, True
        )

    assert user.access_token_needs_refresh()
    assert result is response
    renew.assert_called_once_with(fallback_env=False)
    client.get.assert_called_once_with(
        "https://example.test",
        headers={"Authorization": "Bearer new-token"},
        timeout=12,
    )


def test_request_with_auth_skips_refresh_when_token_is_current():
    response = MagicMock()
    client = MagicMock()
    client.post.return_value = response
    settings = MagicMock()
    settings.user.access_token_needs_refresh.return_value = False

    with (
        patch("lamindb_setup.core._hub_client.httpx_client") as httpx_client,
        patch("lamindb_setup.core._hub_client._renew_save_token") as renew,
        patch("lamindb_setup.settings", settings),
    ):
        httpx_client.return_value.__enter__.return_value = client
        result = request_with_auth(
            "https://example.test",
            "post",
            "current-token",
            True,
            json={"path": "s3://bucket/key"},
        )

    assert result is response
    renew.assert_not_called()
    client.post.assert_called_once_with(
        "https://example.test",
        headers={"Authorization": "Bearer current-token"},
        timeout=12,
        json={"path": "s3://bucket/key"},
    )
