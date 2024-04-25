from __future__ import annotations

import lamindb_setup as ln_setup


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
