import os
import uuid
from urllib.request import urlretrieve

from lamin_logger import logger
from supabase import create_client
from supabase.client import Client

from ._settings_instance import InstanceSettings
from ._settings_load import load_or_create_user_settings
from ._settings_store import Connector, settings_dir


def get_connectore_file_url():
    if "LAMIN_ENV" in os.environ:
        if os.environ["LAMIN_ENV"] == "dev":
            return "https://lamin-site-assets.s3.amazonaws.com/connector_dev.env"
        elif os.environ["LAMIN_ENV"] == "test":
            return "https://lamin-site-assets.s3.amazonaws.com/connector_test.env"
        elif os.environ["LAMIN_ENV"] == "staging":
            return "https://lamin-site-assets.s3.amazonaws.com/connector_staging.env"
    return "https://lamin-site-assets.s3.amazonaws.com/connector.env"


def get_lamin_site_base_url():
    if "LAMIN_ENV" in os.environ:
        if os.environ["LAMIN_ENV"] == "dev":
            return "http://localhost:3000"
        elif os.environ["LAMIN_ENV"] == "test":
            return "http://localhost:3000"
        elif os.environ["LAMIN_ENV"] == "staging":
            return "https://staging.lamin.ai"
    return "https://lamin.ai"


def connect_hub():
    file_url = get_connectore_file_url()
    connector_file, _ = urlretrieve(file_url)
    connector = Connector(_env_file=connector_file)
    return create_client(connector.url, connector.key)


def connect_hub_with_auth():
    hub = connect_hub()
    user_settings = load_or_create_user_settings()
    session = hub.auth.sign_in(
        email=user_settings.email, password=user_settings.password
    )
    hub.postgrest.auth(session.access_token)
    return hub


def sign_up_hub(email) -> str:
    from lnschema_core import id

    hub = connect_hub()
    password = id.secret()  # generate new password
    user = hub.auth.sign_up(
        email=email,
        password=password,
        redirect_to=f"{get_lamin_site_base_url()}/signup",
    )
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
        usettings_file = settings_dir / f"user-{email}.env"
        logger.info(
            "Please *confirm* the sign-up email. After that, login with `lndb login"
            f" {email}`!\n\n"
            f"Generated password: {password}.\n"
            f"Email & password persist in: {usettings_file}.\n"  # noqa
            "Going forward, credentials are auto-loaded. "  # noqa
            "In case of loss, recover your password via email: https://lamin.ai."
        )
        return password
    else:
        return "user-exists"


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
        user_name = data.data[0]["name"]
        if handle is not None and handle != user_handle:
            logger.warning(
                f"Passed handle {handle} does not match your account handle"
                f" {user_handle}! Using account handle {user_handle}."
            )
    else:  # user did not complete signup as usermeta has no matching row
        logger.error("Complete signup on your account page.")
        return "complete-signup"
    hub.auth.sign_out()
    return user_id, user_handle, user_name, session.access_token


# can currently not type-annotate with lnschema_core.Storage as we cannot
# import it statically at time of lndb_setup import
def push_instance_if_not_exists(isettings: InstanceSettings, storage_db_entry):
    hub = connect_hub_with_auth()

    owner = get_user_by_handle(hub, isettings.owner)

    response = hub.table("storage").select("*").eq("id", storage_db_entry.id).execute()
    if len(response.data) == 0:
        storage_fields = {
            "id": storage_db_entry.id,
            "root": str(storage_db_entry.root),
            "region": storage_db_entry.region,
            "type": storage_db_entry.type,
        }
        data = hub.table("storage").insert(storage_fields).execute().data
        assert len(data) == 1

    instance = get_instance(hub, isettings.name, owner["id"])
    if instance is None:
        instance_fields = {
            "id": str(uuid.uuid4()),
            "name": isettings.name,
            "owner_id": owner["id"],
            "storage_id": storage_db_entry.id,
            "dbconfig": isettings._dbconfig,
            "cache_dir": str(isettings.cache_dir),
            "sqlite_file": str(isettings._sqlite_file),
            "sqlite_file_local": str(isettings._sqlite_file_local),
            "db": isettings.db,
        }
        data = hub.table("instance").insert(instance_fields).execute().data
        assert len(data) == 1
        instance_id = data[0]["id"]
    else:
        instance_id = instance["id"]

    response = (
        hub.table("user_instance")
        .select("*")
        .eq("user_id", owner["id"])
        .eq("instance_id", instance_id)
        .execute()
    )
    if len(response.data) == 0:
        user_instance_fields = {
            "user_id": owner["id"],
            "instance_id": instance_id,
        }
        data = hub.table("user_instance").insert(user_instance_fields).execute().data
        assert len(data) == 1

    hub.auth.sign_out()


def get_instance(hub: Client, name: str, owner_id: str):
    response = (
        hub.table("instance")
        .select("*")
        .eq("name", name)
        .eq("owner_id", owner_id)
        .execute()
    )

    if len(response.data) == 0:
        return None
    else:
        assert len(response.data) == 1

    instance = response.data[0]

    return instance


def get_user_by_id(hub: Client, user_id: str):
    response = hub.table("usermeta").select("*").eq("id", user_id).execute()

    if len(response.data) == 0:
        return None
    else:
        assert len(response.data) == 1

    usermeta = response.data[0]

    return usermeta


def get_user_by_handle(hub: Client, handle: str):
    response = hub.table("usermeta").select("*").eq("handle", handle).execute()

    if len(response.data) == 0:
        return None
    else:
        assert len(response.data) == 1

    usermeta = response.data[0]

    return usermeta
