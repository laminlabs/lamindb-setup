from typing import Union
from urllib.request import urlretrieve

from lamin_logger import logger
from lamindb_schema import id
from supabase import create_client

from ._settings_load import load_or_create_user_settings
from ._settings_store import Connector, current_user_settings_file


def connect_hub():
    connector_file, _ = urlretrieve(
        "https://lamin-site-assets.s3.amazonaws.com/connector.env"
    )
    connector = Connector(_env_file=connector_file)
    return create_client(connector.url, connector.key)


def sign_up_hub(email) -> Union[str, None]:
    hub = connect_hub()
    password = id.id_secret()  # generate new password
    user = hub.auth.sign_up(email=email, password=password)
    # if user already exists a fake user object without identity is returned
    if user.identities:
        # if user had called sign-up before, but not confirmed their email
        # the user has an identity with a wrong ID
        # we can check for it by comparing time stamps
        # see design note uL8Sjht0y4qg
        diff = user.confirmation_sent_at - user.identities[0].last_sign_in_at
        if (
            diff.total_seconds() > 0.1
        ):  # the first time, this is on the order of microseconds
            raise RuntimeError(
                "It seems you already signed up with this email. Please click on the"
                " link in the confirmation email that you should have received from"
                " lamin.ai."
            )
        logger.info(
            "Please *confirm* the sign-up email. After that, login with `lndb login"
            f" {email}`!\n\n"
            f"Generated password: {password}.\n"
            f"Email & password persist in: {current_user_settings_file}.\n"  # noqa
            "Going forward, credentials are auto-loaded. "  # noqa
            "In case of loss, you can always recover your password via email."
        )
        return password
    else:
        return None


def sign_in_hub(email, password, handle=None):
    hub = connect_hub()
    try:
        session = hub.auth.sign_in(email=email, password=password)
    except Exception:  # this is bad, but I don't find APIError right now
        logger.error("Could not login. Probably your password is wrong.")
        return "could-not-login"
    data = hub.table("usermeta").select("*").eq("id", session.user.id.hex).execute()
    if len(data.data) > 0:  # user is completely registered
        user_id = data.data[0]["lnid"]
        user_handle = data.data[0]["handle"]
        if handle is not None and handle != user_handle:
            logger.warning(
                f"Passed handle {handle} does not match your account handle"
                f" {user_handle}! Using account handle {user_handle}."
            )
    else:  # user did not complete signup as usermeta has no matching row
        logger.error("Complete signup on your account page.")
        return "complete-signup"
    hub.auth.sign_out()
    return user_id, user_handle


def create_instance(instance_name):
    hub = connect_hub()
    user_settings = load_or_create_user_settings()
    session = hub.auth.sign_in(
        email=user_settings.email, password=user_settings.password
    )
    hub.postgrest.auth(session.access_token)
    # data = hub.table("user_instance").select("instance_id").eq("user_id", session.user.id.hex).execute()  # noqa
    instance_id = id.id_instance()
    data = (
        hub.table("instance")
        .insert({"lnid": instance_id, "handle": instance_id, "name": instance_name})
        .execute()
    )
    assert len(data.data) > 0
    data = (
        hub.table("user_instance")
        .insert(
            {
                "user_id": session.user.id.hex,
                "instance_id": data.data[0]["id"],
                "is_owner": True,
            }
        )  # noqa
        .execute()
    )
    assert len(data.data) > 0
    hub.auth.sign_out()
    return instance_id
