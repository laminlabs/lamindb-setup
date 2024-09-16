from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import lamindb_setup as ln_setup
import pytest
from lamindb_setup.core._hub_client import connect_hub_with_auth
from lamindb_setup.core._hub_core import create_api_key
from supafunc.errors import FunctionsHttpError


def test_login():
    ln_setup.login("testuser1")
    assert ln_setup.settings.user.email == "testuser1@lamin.ai"
    assert ln_setup.settings.user.uid == "DzTjkKse"
    assert ln_setup.settings.user.handle == "testuser1"

    import jwt

    access_token_payload = jwt.decode(
        ln_setup.settings.user.access_token,
        algorithms="HS256",
        options={"verify_signature": False},
    )
    assert access_token_payload["email"] == "testuser1@lamin.ai"


def test_login_api_key():
    ln_setup.login("testuser1")
    save_password = ln_setup.settings.user.password
    # obtain API key
    expires_at = datetime.now(tz=timezone.utc) + timedelta(days=1)
    api_key = create_api_key(
        {
            "expires_at": expires_at.strftime("%Y-%m-%d"),
            "description": "test_login_api_key",
        }
    )

    ln_setup.logout()
    assert ln_setup.settings.user.handle == "anonymous"

    with pytest.raises(FunctionsHttpError):
        ln_setup.login(api_key="invalid-key")

    with pytest.raises(ValueError):
        ln_setup.login(user=None, api_key=None)

    os.environ["LAMIN_API_KEY"] = api_key
    ln_setup.login()
    assert ln_setup.settings.user.handle == "testuser1"
    assert ln_setup.settings.user.api_key == api_key
    assert ln_setup.settings.user.password is None

    ln_setup.logout()

    ln_setup.login(api_key=api_key)
    assert ln_setup.settings.user.handle == "testuser1"
    assert ln_setup.settings.user.api_key == api_key
    assert ln_setup.settings.user.password is None

    ln_setup.logout()

    # load from handle env
    ln_setup.login("testuser1")
    assert ln_setup.settings.user.handle == "testuser1"
    assert ln_setup.settings.user.api_key == api_key
    assert ln_setup.settings.user.password is None

    # clean up
    # here checks also refreshing access token with api_key
    hub = connect_hub_with_auth(renew_token=True)
    hub.table("api_key").delete().eq("description", "test_login_api_key").execute()
    hub.auth.sign_out({"scope": "local"})

    # login back with email to populate all fields
    ln_setup.login("testuser1@lamin.ai", key=save_password)
    assert ln_setup.settings.user.handle == "testuser1"
    assert ln_setup.settings.user.api_key is None
    assert ln_setup.settings.user.password == save_password
