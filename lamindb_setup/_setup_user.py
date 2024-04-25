from __future__ import annotations

from typing import Optional, Union

from lamin_utils import logger

from ._check_setup import _check_instance_setup
from ._init_instance import register_user
from .core._settings import settings
from .core._settings_load import load_or_create_user_settings, load_user_settings
from .core._settings_save import save_user_settings
from .core._settings_store import (
    current_user_settings_file,
    user_settings_file_email,
    user_settings_file_handle,
)


def load_user(email: str | None = None, handle: str | None = None) -> str | None:
    if email is not None:
        settings_file = user_settings_file_email(email)
    if handle is not None:
        settings_file = user_settings_file_handle(handle)
    if settings_file.exists():
        user_settings = load_user_settings(settings_file)
        save_user_settings(user_settings)  # needed to save to current_user.env
        assert user_settings.email is not None
    else:
        user_settings = load_or_create_user_settings()
        if email is None:
            raise RuntimeError(
                "Use your email for your first login in a compute environment. "
                "After that, you can use your handle."
            )
        user_settings.email = email
        user_settings.handle = handle
        save_user_settings(user_settings)

    from .core._settings import settings

    settings._user_settings = None  # this is to refresh a settings instance

    return None


def login(
    user: str,
    *,
    key: str | None = None,
    password: str | None = None,  # for backward compat
) -> None:
    """Log in user.

    Args:
        user: handle or email
        key: API key or legacy passward
        password: Backward compat, will be removed
    """
    if "@" in user:
        email, handle = user, None
    else:
        email, handle = None, user
    load_user(email, handle)

    user_settings = load_or_create_user_settings()

    if password is not None:
        logger.warning(
            "please use --key instead of --password, "
            "passwords are deprecated and replaced with API keys"
        )
        key = password

    if key is not None:
        # within UserSettings, we still call it "password" for a while
        user_settings.password = key

    if user_settings.email is None:
        raise RuntimeError("No stored user email, please call: lamin login {user}")

    if user_settings.password is None:
        raise RuntimeError(
            "No stored API key, please call: lamin login <your-email> --key <API-key>"
        )

    from .core._hub_core import sign_in_hub

    response = sign_in_hub(
        user_settings.email, user_settings.password, user_settings.handle
    )
    if isinstance(response, Exception):
        raise response
    else:
        user_uuid, user_id, user_handle, user_name, access_token = response
    if handle is None:
        logger.success(f"logged in with handle {user_handle} (uid: {user_id})")
    else:
        logger.success(f"logged in with email {user_settings.email} (uid: {user_id})")
    user_settings.uid = user_id
    user_settings.handle = user_handle
    user_settings.name = user_name
    user_settings._uuid = user_uuid
    user_settings.access_token = access_token
    save_user_settings(user_settings)

    if settings._instance_exists and _check_instance_setup():
        register_user(user_settings)

    settings._user_settings = None
    return None


def logout():
    if current_user_settings_file().exists():
        current_user_settings_file().unlink()
        logger.success("logged out")
    else:
        logger.important("already logged out")
