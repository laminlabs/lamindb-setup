from __future__ import annotations

import os
from typing import TYPE_CHECKING

from lamin_utils import logger

from ._check_setup import _check_instance_setup
from ._init_instance import register_user
from .core._settings import settings
from .core._settings_load import load_user_settings
from .core._settings_save import save_user_settings
from .core._settings_store import (
    current_user_settings_file,
    user_settings_file_email,
    user_settings_file_handle,
)
from .core._settings_user import UserSettings


def load_user(email: str | None = None, handle: str | None = None) -> UserSettings:
    if email is not None:
        settings_file = user_settings_file_email(email)
    if handle is not None:
        settings_file = user_settings_file_handle(handle)
    if settings_file.exists():
        user_settings = load_user_settings(settings_file)
        save_user_settings(user_settings)  # needed to save to current_user.env
        assert user_settings.email is not None or user_settings.api_key is not None
    else:
        if email is None:
            raise SystemExit(
                "✗ Use your email for your first login in a compute environment. "
                "After that, you can use your handle."
            )
        user_settings = UserSettings(handle=handle, email=email, uid="null")  # type: ignore

    from .core._settings import settings

    settings._user_settings = None  # this is to refresh a settings instance

    return user_settings


def login(
    user: str | None = None, *, api_key: str | None = None, **kwargs
) -> UserSettings:
    # note that the docstring needs to be synced with lamin login
    """Log into LaminHub.

    `login()` prompts for your API key unless you set it via the `LAMIN_API_KEY` environment variable or pass it as an argument.

    You can create your API key in your account settings on LaminHub (top right corner).

    Args:
        user: User handle.
        api_key: API key.

    See Also:
        Login via the CLI command `lamin login`, see `here <https://docs.lamin.ai/cli#login>`__.

    Examples:

        Logging in the first time::

            import lamindb as ln

            ln.setup.login()  # prompts for API key

        After authenticating multiple users, you can switch between user profiles::

            ln.setup.login("myhandle")  # pass your user handle
    """
    if user is None:
        if api_key is None:
            if "LAMIN_API_KEY" in os.environ:
                api_key = os.environ["LAMIN_API_KEY"]
            else:
                api_key = input("Your API key: ")
    elif api_key is not None:
        raise ValueError("Please provide either 'user' or 'api_key', not both.")

    for kwarg in kwargs:
        if kwarg != "key":
            raise TypeError(f"login() got unexpected keyword argument '{kwarg}'")
    key: str | None = kwargs.get("key", None)  # legacy API key aka password
    if key is not None:
        logger.warning(
            "the legacy API key is deprecated and will likely be removed in a future version"
        )

    if api_key is None:
        if "@" in user:  # type: ignore
            email, handle = user, None
        else:
            email, handle = None, user
        user_settings = load_user(email, handle)

        if key is not None:
            user_settings.password = key

        if user_settings.password is None:
            api_key = user_settings.api_key
            if api_key is None:
                raise SystemExit(
                    "✗ No stored API key, please call: "
                    "`lamin login` or `lamin login <your-email> --key <API-key>`"
                )
        elif user_settings.email is None:
            raise SystemExit(f"✗ No stored user email, please call: lamin login {user}")
    else:
        user_settings = UserSettings(handle="temporary", uid="null")

    from .core._hub_core import sign_in_hub, sign_in_hub_api_key

    if api_key is None:
        response = sign_in_hub(
            user_settings.email,  # type: ignore
            user_settings.password,  # type: ignore
            user_settings.handle,
        )
    else:
        response = sign_in_hub_api_key(api_key)
        user_settings.password = None

    if isinstance(response, Exception):
        raise response
    elif isinstance(response, str):
        raise SystemExit(f"✗ Unsuccessful login: {response}.")
    else:
        user_uuid, user_id, user_handle, user_name, access_token = response

    if api_key is not None:
        logger.success(f"logged in {user_handle}")
    else:  # legacy flow
        logger.success(f"logged in with email {user_settings.email}")

    user_settings.uid = user_id
    user_settings.handle = user_handle
    user_settings.name = user_name
    user_settings._uuid = user_uuid
    user_settings.access_token = access_token
    user_settings.api_key = api_key
    save_user_settings(user_settings)

    if settings._instance_exists and _check_instance_setup():
        register_user(user_settings)

    settings._user_settings = None
    return user_settings


def logout():
    """Log out the current user.

    This deletes the `~/.lamin/current_user.env` so that you'll no longer be automatically authenticated in the environment.

    It keeps a copy as `~/.lamin/user--<user_handle>.env` so you can easily switch between user profiles.

    See Also:
        Logout via the CLI command `lamin logout`, see `here <https://docs.lamin.ai/cli#logout>`__.
    """
    if current_user_settings_file().exists():
        current_user_settings_file().unlink()
        settings._user_settings = None
        logger.success("logged out")
    else:
        logger.important("already logged out")
    if os.environ.get("LAMIN_API_KEY") is not None:
        logger.warning(
            "LAMIN_API_KEY is still set in your environment and will automatically log you in"
        )
