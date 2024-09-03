from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import lamindb_setup as ln_setup
from lamindb_setup.core._hub_client import connect_hub_with_auth


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
    # obtain API key
    hub = connect_hub_with_auth()
    expires_at = (datetime.now(tz=timezone.utc) + timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
    response = hub.functions.invoke(
        "create-api-key",
        invoke_options={
            "body": {"expires_at": expires_at, "description": "test_login_api_key"}
        },
    )
    api_key = json.loads(response)["apiKey"]
    hub.auth.sign_out({"scope": "local"})

    ln_setup.logout()
    assert ln_setup.settings.user.handle == "anonymous"

    ln_setup.login(user=None, api_key=api_key)
    assert ln_setup.settings.user.handle == "testuser1"

    # clean up
    hub = connect_hub_with_auth()  # uses jwt from api_key
    hub.table("api_key").delete().eq("description", "test_login_api_key").execute()
    hub.auth.sign_out({"scope": "local"})

    # login back with email to populate all fields
    ln_setup.login("testuser1@lamin.ai")
