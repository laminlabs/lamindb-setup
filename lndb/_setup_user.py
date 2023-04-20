from typing import Union

from lamin_logger import logger
from lnhub_rest.core.account._signup_signin import sign_in_hub, sign_up_hub

from ._settings import settings
from .dev._db import upsert
from .dev._settings_load import load_or_create_user_settings, load_user_settings
from .dev._settings_save import save_user_settings
from .dev._settings_store import user_settings_file_email, user_settings_file_handle


def signup(email: str) -> Union[str, None]:
    """Sign up user."""
    response = sign_up_hub(email)
    if response == "handle-exists":  # handle already exists
        logger.error("The handle already exists. Please choose a different one.")
        return "handle-exists"
    if response == "user-exists":  # user already exists
        logger.error("User already exists! Please login instead: `lndb login`.")
        return "user-exists"
    user_settings = load_or_create_user_settings()
    user_settings.email = email
    save_user_settings(user_settings)
    user_settings.password = response
    save_user_settings(user_settings)
    return None  # user needs to confirm email now


def load_user(email: str = None, handle: str = None) -> Union[str, None]:
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

    from ._settings import settings

    settings._user_settings = None  # this is to refresh a settings instance

    return None


def login(
    user: str,
    *,
    password: Union[str, None] = None,
) -> Union[str, None]:
    """Log in user."""
    if "@" in user:
        email, handle = user, None
    else:
        email, handle = None, user
    load_user(email, handle)

    user_settings = load_or_create_user_settings()

    if password:
        user_settings.password = password

    if user_settings.email is None:
        raise RuntimeError("No stored user email, please call: lndb login {user}")

    if user_settings.password is None:
        raise RuntimeError(
            "No stored user password, please call: lndb login <your-email>"
            " --password <your-password>"
        )

    response = sign_in_hub(
        user_settings.email, user_settings.password, user_settings.handle
    )
    if response == "could-not-login":
        return response
    elif response == "complete-signup":
        return response
    else:
        user_id, user_handle, user_name, access_token = response
    if handle is None:
        logger.success(f"Logged in with handle {user_handle} and id {user_id}")
    else:
        logger.success(f"Logged in with email {user_settings.email} and id {user_id}")
    user_settings.id = user_id
    user_settings.handle = user_handle
    user_settings.name = user_name
    user_settings.access_token = access_token
    save_user_settings(user_settings)

    settings._user_settings = None

    # register login of user in instance db
    if settings._instance_exists:
        if not settings.instance._is_db_reachable():
            logger.info("Consider closing the instance: `lamin close`")
            return None
        upsert.user(
            settings.user.email,
            settings.user.id,
            settings.user.handle,
            settings.user.name,
        )

    return None
