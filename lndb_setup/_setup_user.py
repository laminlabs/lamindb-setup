from typing import Union

from lamin_logger import logger

from ._hub import sign_in_hub, sign_up_hub
from ._settings_load import load_or_create_user_settings, load_user_settings
from ._settings_save import save_user_settings
from ._settings_store import settings_dir


def sign_up_user(email):
    """Sign up user."""
    user_settings = load_or_create_user_settings()
    user_settings.user_email = email
    save_user_settings(user_settings)
    secret = sign_up_hub(email)
    if secret is None:  # user already exists
        logger.error("User already exists! Please login instead: `lndb login`.")
        return "user-exists"
    user_settings.user_secret = secret
    save_user_settings(user_settings)
    return None  # user needs to confirm email now


def load_user(user_email: str):
    settings_file = settings_dir / f"{user_email}.env"
    if settings_file.exists():
        user_settings = load_user_settings(settings_file)
        assert user_settings.user_email is not None
    else:
        user_settings = load_or_create_user_settings()
        user_settings.user_email = user_email
    save_user_settings(user_settings)

    from ._settings import settings

    settings._user_settings = None


def log_in_user(
    *,
    email: Union[str, None] = None,
    secret: Union[str, None] = None,
):
    """Log in user."""
    if email:
        load_user(email)

    user_settings = load_or_create_user_settings()

    if secret:
        user_settings.user_secret = secret

    if user_settings.user_email is None:
        raise RuntimeError(
            "No stored user email, please call: lndb login --email <your-email>"
        )

    if user_settings.user_secret is None:
        raise RuntimeError(
            "No stored user secret, please call: lndb login --email <your-email>"
            " --email <your-secret>"
        )

    user_id = sign_in_hub(user_settings.user_email, user_settings.user_secret)
    user_settings.user_id = user_id
    save_user_settings(user_settings)

    from ._settings import settings

    settings._user_settings = None
