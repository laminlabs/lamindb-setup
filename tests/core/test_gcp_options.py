from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from lamindb_setup.core._gcp_options import (
    GCPOptionsManager,
    _gcp_options_manager_dict,
    get_user_gcp_options_manager,
    reset_user_gcp_options_cache,
)
from lamindb_setup.core._hub_core import (
    _access_gcp_endpoint,
    _access_gcp_route,
    access_gcp,
)
from lamindb_setup.core._settings import settings
from lamindb_setup.core._settings_storage import StorageSettings
from lamindb_setup.core.upath import UPath, create_path

_API_URL = "https://aws.us-east-1.lamin.ai/api"
_MANAGED_ROOT = "gs://managed-bucket/prefix/"


@pytest.fixture
def managed_gcp(monkeypatch):
    manager = GCPOptionsManager()
    state = {"token": "ya29.first", "fail": False}
    calls: list[tuple[str, str | None]] = []

    def fake_access_gcp(path: str, access_token: str | None = None):
        calls.append((path, access_token))
        if state["fail"] or not path.startswith(_MANAGED_ROOT):
            return {"credentials": {}, "accessibility": {}}
        return {
            "credentials": {
                "token": state["token"],
                "expiry_time": "2099-01-01T00:00:00Z",
            },
            "accessibility": {
                "storage_root": "gs://managed-bucket/prefix",
                "is_managed": True,
                "extra_parameters": {"project": "lamin-proj"},
            },
        }

    monkeypatch.setattr("lamindb_setup.core._hub_core.access_gcp", fake_access_gcp)
    return SimpleNamespace(manager=manager, calls=calls, state=state)


@pytest.fixture
def hub_user(monkeypatch):
    """Point settings at a fake user without loading credentials from disk."""

    def _set(handle: str = "testuser", access_token: str | None = "user-token"):
        monkeypatch.setattr(
            settings,
            "_user_settings",
            SimpleNamespace(handle=handle, access_token=access_token),
        )
        # None skips the env-change reload inside settings.user
        monkeypatch.setattr(settings, "_user_settings_env", None)

    return _set


def _past_expiry() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=10)


def test_enrich_path_caches_credentials_for_a_managed_root(managed_gcp):
    pytest.importorskip("google.auth")
    import google.auth.credentials

    path = managed_gcp.manager.enrich_path(UPath("gs://managed-bucket/prefix/file.txt"))
    token = path.storage_options["token"]

    assert isinstance(token, google.auth.credentials.Credentials)
    assert token.token == "ya29.first"
    assert token.valid
    assert token.expiry.tzinfo is None
    assert path.storage_options["project"] == "lamin-proj"
    assert path.storage_options["use_listings_cache"] is True
    assert path.storage_options["version_aware"] is False
    assert managed_gcp.calls == [("gs://managed-bucket/prefix/file.txt/", None)]
    assert managed_gcp.manager._credentials_cache[_MANAGED_ROOT] is token

    again = managed_gcp.manager.enrich_path(
        UPath("gcs://managed-bucket/prefix/other.txt")
    )
    assert again.storage_options["token"] is token
    assert managed_gcp.calls == [("gs://managed-bucket/prefix/file.txt/", None)]

    public = managed_gcp.manager.enrich_path(UPath("gs://public-bucket/a/file.txt"))
    assert "token" not in public.storage_options
    managed_gcp.manager.enrich_path(UPath("gs://public-bucket/b/file.txt"))
    assert managed_gcp.calls == [
        ("gs://managed-bucket/prefix/file.txt/", None),
        ("gs://public-bucket/a/file.txt/", None),
    ]
    assert managed_gcp.manager._credentials_cache["gs://public-bucket/"] is None

    # a longer cached root wins over the bucket
    managed_gcp.manager._credentials_cache["gs://managed-bucket/"] = None
    nested = managed_gcp.manager.enrich_path(
        UPath("gs://managed-bucket/prefix/nested/file.txt")
    )
    assert nested.storage_options["token"] is token
    assert len(managed_gcp.calls) == 2


def test_enrich_path_leaves_unmanaged_paths_and_custom_endpoints(managed_gcp):
    owned = managed_gcp.manager.enrich_path(
        UPath(
            "gs://public-bucket/file.txt",
            token="caller-token",
            use_listings_cache=False,
            version_aware=True,
        )
    )
    assert owned.storage_options["token"] == "caller-token"
    assert owned.storage_options["use_listings_cache"] is False
    assert owned.storage_options["version_aware"] is True

    replaced = managed_gcp.manager.enrich_path(
        UPath("gs://managed-bucket/prefix/file.txt", token="caller-token")
    )
    assert replaced.storage_options["token"] != "caller-token"

    before = len(managed_gcp.calls)
    custom = managed_gcp.manager.enrich_path(
        UPath(
            "gs://managed-bucket/prefix/file.txt",
            endpoint_url="http://localhost:4443",
        )
    )
    assert custom.storage_options["endpoint_url"] == "http://localhost:4443"
    assert "token" not in custom.storage_options
    assert len(managed_gcp.calls) == before


def test_explicit_access_token_bypasses_the_cache(managed_gcp):
    first = managed_gcp.manager.enrich_path(
        UPath("gs://managed-bucket/prefix/file.txt"), access_token="explicit"
    )
    second = managed_gcp.manager.enrich_path(
        UPath("gs://managed-bucket/prefix/file.txt"), access_token="explicit"
    )

    assert first.storage_options["token"] is not second.storage_options["token"]
    assert managed_gcp.calls == [
        ("gs://managed-bucket/prefix/file.txt/", "explicit"),
        ("gs://managed-bucket/prefix/file.txt/", "explicit"),
    ]
    assert _MANAGED_ROOT not in managed_gcp.manager._credentials_cache


def test_create_path_and_storage_root_reuse_cached_credentials(
    managed_gcp, monkeypatch
):
    monkeypatch.setattr(
        "lamindb_setup.core.upath.get_user_gcp_options_manager",
        lambda: managed_gcp.manager,
    )
    monkeypatch.setattr(
        "lamindb_setup.core._gcp_options.get_user_gcp_options_manager",
        lambda: managed_gcp.manager,
    )

    path = create_path("gs://managed-bucket/prefix/file.txt")
    storage = StorageSettings("gs://managed-bucket/prefix")
    root = storage.root
    again = storage.root

    assert path.storage_options["token"].token == "ya29.first"
    assert root.storage_options["token"] is path.storage_options["token"]
    assert again.storage_options["token"] is root.storage_options["token"]
    # the storage root is the cached managed root, so it is not fetched again
    assert managed_gcp.calls == [("gs://managed-bucket/prefix/file.txt/", None)]


def test_credentials_refresh_through_gcsfs(managed_gcp):
    pytest.importorskip("google.auth")
    pytest.importorskip("gcsfs")
    from gcsfs.retry import HttpError
    from google.auth.exceptions import RefreshError

    path = managed_gcp.manager.enrich_path(UPath("gs://managed-bucket/prefix/file.txt"))
    token = path.storage_options["token"]
    filesystem = path.fs
    assert filesystem.credentials.credentials is token

    managed_gcp.state["token"] = "ya29.second"
    token.expiry = _past_expiry()
    token.refresh(None)
    assert token.token == "ya29.second"
    assert token.valid
    assert managed_gcp.calls[-1] == (_MANAGED_ROOT, None)

    managed_gcp.state["token"] = "ya29.third"
    token.expiry = _past_expiry()
    filesystem.credentials.maybe_refresh()
    assert token.token == "ya29.third"

    managed_gcp.state["fail"] = True
    token.expiry = _past_expiry()
    with pytest.raises(RefreshError):
        token.refresh(None)
    assert token.token == "ya29.third"
    with pytest.raises(HttpError) as exc_info:
        filesystem.credentials.maybe_refresh()
    assert exc_info.value.code == 401
    assert token.token == "ya29.third"


def test_access_gcp_maps_the_endpoint_response(hub_user):
    hub_user()
    payload = {
        "Credentials": {
            "access_token": "ya29.abc",
            "expiry": "2099-01-01T00:00:00Z",
            "token_type": "Bearer",
        },
        "StorageAccessibility": {
            "storageRoot": "gs://managed-bucket/prefix",
            "role": "read",
            "isPublic": False,
            "isManaged": True,
            "extraParameters": {"project": "lamin-proj"},
        },
    }
    with (
        patch(
            "lamindb_setup.core._hub_core.call_with_fallback_auth",
            return_value=_API_URL,
        ) as route,
        patch(
            "lamindb_setup.core._hub_core.call_with_fallback",
        ) as anonymous_route,
        patch(
            "lamindb_setup.core._hub_core._access_gcp_endpoint",
            return_value=payload,
        ) as endpoint,
    ):
        info = access_gcp("gs://managed-bucket/prefix/file.txt")

    anonymous_route.assert_not_called()
    assert route.call_args.args[0] is _access_gcp_route
    assert route.call_args.kwargs == {
        "path": "gs://managed-bucket/prefix/file.txt",
        "access_token": None,
    }
    endpoint.assert_called_once_with(
        _API_URL, "gs://managed-bucket/prefix/file.txt", access_token=None
    )
    assert info["credentials"] == {
        "token": "ya29.abc",
        "expiry_time": "2099-01-01T00:00:00Z",
    }
    assert info["accessibility"] == {
        "storage_root": "gs://managed-bucket/prefix",
        "is_managed": True,
        "extra_parameters": {"project": "lamin-proj"},
    }


def test_access_gcp_skips_the_endpoint_without_an_api_url(hub_user):
    hub_user()
    with (
        patch(
            "lamindb_setup.core._hub_core.call_with_fallback_auth",
            return_value=None,
        ),
        patch("lamindb_setup.core._hub_core._access_gcp_endpoint") as endpoint,
    ):
        info = access_gcp("gs://public-bucket/file.txt")

    endpoint.assert_not_called()
    assert info == {"credentials": {}, "accessibility": {}}


def test_access_gcp_uses_the_unauthenticated_client_for_anonymous_callers(hub_user):
    hub_user("anonymous", None)
    with (
        patch(
            "lamindb_setup.core._hub_core.call_with_fallback",
            return_value=None,
        ) as route,
        patch(
            "lamindb_setup.core._hub_core.call_with_fallback_auth",
        ) as authenticated_route,
    ):
        access_gcp("gs://public-bucket/file.txt")

    authenticated_route.assert_not_called()
    assert route.call_args.kwargs == {"path": "gs://public-bucket/file.txt"}

    hub_user("anonymous", None)
    with (
        patch(
            "lamindb_setup.core._hub_core.call_with_fallback_auth",
            return_value=None,
        ) as authenticated_route,
        patch("lamindb_setup.core._hub_core.call_with_fallback") as route,
    ):
        access_gcp("gs://public-bucket/file.txt", access_token="explicit")

    route.assert_not_called()
    assert authenticated_route.call_args.kwargs["access_token"] == "explicit"


def test_access_gcp_swallows_endpoint_errors(hub_user):
    hub_user()
    with (
        patch(
            "lamindb_setup.core._hub_core.call_with_fallback_auth",
            return_value=_API_URL,
        ),
        patch(
            "lamindb_setup.core._hub_core._access_gcp_endpoint",
            side_effect=PermissionError("denied"),
        ),
        patch("lamindb_setup.core._hub_core.logger.warning") as warning,
    ):
        info = access_gcp("gs://managed-bucket/prefix/file.txt")

    assert info == {"credentials": {}, "accessibility": {}}
    warning.assert_called_once()
    assert "gs://managed-bucket/prefix/file.txt" in warning.call_args.args[0]


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        ([], None),
        (None, None),
        ([{"api_url": None}], None),
        ([{"api_url": ""}], None),
        ([{"api_url": _API_URL, "assume_role_arn": None}], _API_URL),
    ],
)
def test_access_gcp_route_returns_only_the_api_url(data, expected):
    client = MagicMock()
    client.rpc.return_value.execute.return_value.data = data

    assert _access_gcp_route(
        path="gs://managed-bucket/prefix/file.txt", client=client
    ) == (expected)
    client.rpc.assert_called_once_with(
        "get_storage_api_info_from_path",
        {"_path": "gs://managed-bucket/prefix/file.txt"},
        get=True,
    )


def test_access_gcp_endpoint_requests_a_downscoped_token(hub_user):
    hub_user()
    response = MagicMock(status_code=200)
    response.json.return_value = {"Credentials": {}}

    with (
        patch.dict("os.environ", {"LAMIN_ENV": "prod"}),
        patch(
            "lamindb_setup.core._hub_core.request_with_auth",
            return_value=response,
        ) as request,
    ):
        assert _access_gcp_endpoint(_API_URL, "gs://managed-bucket/prefix/") == {
            "Credentials": {}
        }

    request.assert_called_once_with(
        f"{_API_URL}/gcs/credentials",
        "post",
        "user-token",
        True,
        json={"path": "gs://managed-bucket/prefix/", "duration_seconds": 3600},
    )

    with (
        patch.dict("os.environ", {"LAMIN_ENV": "local"}),
        patch(
            "lamindb_setup.core._hub_core.request_with_auth",
            return_value=response,
        ) as request,
    ):
        _access_gcp_endpoint(
            _API_URL, "gs://managed-bucket/prefix/", access_token="explicit"
        )

    request.assert_called_once_with(
        "/gcs/credentials",
        "post",
        "explicit",
        False,
        json={"path": "gs://managed-bucket/prefix/", "duration_seconds": 3600},
    )

    hub_user("anonymous", None)
    response.status_code = 403
    response.text = "denied"
    with (
        patch.dict("os.environ", {"LAMIN_ENV": "prod"}),
        patch(
            "lamindb_setup.core._hub_core.request_with_auth",
            return_value=response,
        ) as request,
        pytest.raises(PermissionError, match="403 denied"),
    ):
        _access_gcp_endpoint(_API_URL, "gs://managed-bucket/prefix/")

    request.assert_called_once_with(
        f"{_API_URL}/gcs/credentials",
        "post",
        None,
        False,
        json={"path": "gs://managed-bucket/prefix/", "duration_seconds": 3600},
    )


def test_user_manager_is_cached_until_reset(hub_user, monkeypatch):
    hub_user("gcp-test-alice")
    try:
        first = get_user_gcp_options_manager()
        assert get_user_gcp_options_manager() is first
        hub_user("gcp-test-bob")
        assert get_user_gcp_options_manager() is not first
    finally:
        _gcp_options_manager_dict.pop("gcp-test-alice", None)
        _gcp_options_manager_dict.pop("gcp-test-bob", None)

    _gcp_options_manager_dict["gcp-test-alice"] = GCPOptionsManager()
    monkeypatch.setattr(
        settings, "_user_settings", SimpleNamespace(handle="gcp-test-alice")
    )
    reset_user_gcp_options_cache()
    assert "gcp-test-alice" not in _gcp_options_manager_dict

    _gcp_options_manager_dict["gcp-test-alice"] = GCPOptionsManager()
    monkeypatch.setattr(settings, "_user_settings", None)
    reset_user_gcp_options_cache()
    assert "gcp-test-alice" in _gcp_options_manager_dict
    _gcp_options_manager_dict.pop("gcp-test-alice", None)
