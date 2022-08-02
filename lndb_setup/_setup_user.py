from typing import Union

from lamin_logger import logger

from ._db import insert_if_not_exists
from ._hub import sign_in_hub, sign_up_hub
from ._settings_load import load_or_create_user_settings, load_user_settings
from ._settings_save import save_user_settings
from ._settings_store import settings_dir


def signup(email: str):
    """Sign up user."""
    user_settings = load_or_create_user_settings()
    user_settings.email = email
    save_user_settings(user_settings)
    password = sign_up_hub(email)
    if password == "handle-exists":  # handle already exists
        logger.error("The handle already exists. Please choose a different one.")
        return "handle-exists"
    if password is None:  # user already exists
        logger.error("User already exists! Please login instead: `lndb login`.")
        return "user-exists"
    user_settings.password = password
    save_user_settings(user_settings)
    return None  # user needs to confirm email now


def load_user(email: str = None, handle: str = None):
    if email is not None:
        settings_file = settings_dir / f"user-{email}.env"
    if handle is not None:
        settings_file = settings_dir / f"user-{handle}.env"
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


def login(
    user: str,
    *,
    password: Union[str, None] = None,
):
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
            "No stored user password, please call: lndb login --email <your-email>"
            " --email <your-password>"
        )

    response = sign_in_hub(
        user_settings.email, user_settings.password, user_settings.handle
    )
    if response == "could-not-login":
        return response
    elif response == "complete-signup":
        return response
    else:
        user_id, user_handle = response
    if handle is None:
        logger.info(f"Your user handle is '{user_handle}'.")
    user_settings.id = user_id
    user_settings.handle = user_handle
    save_user_settings(user_settings)

    from ._settings import settings

    settings._user_settings = None

    # log in user into instance db
    if settings.instance.name is not None:
        # the above if condition is not safe enough
        # users might delete a database but still keeping the
        # current_instance.env settings file around
        # hence, the if condition will pass despite the database
        # having actually been deleted
        # so, let's do another check
        if settings.instance._dbconfig == "sqlite":
            # let's check whether the sqlite file is actually available
            if not settings.instance._sqlite_file.exists():
                # if the file doesn't exist, there is no need to
                # log in the user
                # hence, simply end log in here
                return None
            # there is a remaining case where there could be a file that
            # has no tables in it, we're ignoring this for now
        insert_if_not_exists.user(
            settings.user.email, settings.user.id, settings.user.handle
        )
