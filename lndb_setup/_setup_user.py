from typing import Union

from lamin_logger import logger

from ._hub import sign_in_hub, sign_up_hub
from ._settings_load import load_or_create_user_settings, load_user_settings
from ._settings_save import save_user_settings
from ._settings_store import settings_dir


def sign_up_user(email):
    """Sign up user."""
    user_settings = load_or_create_user_settings()
    user_settings.email = email
    save_user_settings(user_settings)
    password = sign_up_hub(email)
    if password is None:  # user already exists
        logger.error("User already exists! Please login instead: `lndb login`.")
        return "user-exists"
    user_settings.password = password
    save_user_settings(user_settings)
    return None  # user needs to confirm email now


def load_user(email: str):
    settings_file = settings_dir / f"{email}.env"
    if settings_file.exists():
        user_settings = load_user_settings(settings_file)
        assert user_settings.email is not None
    else:
        user_settings = load_or_create_user_settings()
        user_settings.email = email
    save_user_settings(user_settings)

    from ._settings import settings

    settings._user_settings = None


def log_in_user(
    *,
    email: Union[str, None] = None,
    password: Union[str, None] = None,
):
    """Log in user."""
    if email:
        load_user(email)

    user_settings = load_or_create_user_settings()

    if password:
        user_settings.password = password

    if user_settings.email is None:
        raise RuntimeError(
            "No stored user email, please call: lndb login --email <your-email>"
        )

    if user_settings.password is None:
        raise RuntimeError(
            "No stored user password, please call: lndb login --email <your-email>"
            " --email <your-password>"
        )

    user_id = sign_in_hub(user_settings.email, user_settings.password)
    user_settings.id = user_id
    save_user_settings(user_settings)

    from ._settings import settings

    settings._user_settings = None
