import os
from typing import Optional, Tuple, Union
from uuid import UUID, uuid4

from lamin_utils import logger
from postgrest.exceptions import APIError

from supabase import Client

from ._hub_client import connect_hub, connect_hub_with_auth
from ._hub_crud import (
    sb_insert_collaborator,
    sb_insert_db_user,
    sb_insert_instance,
    sb_insert_storage,
    sb_select_account_by_handle,
    sb_select_db_user_by_instance,
    sb_select_instance_by_name,
    sb_select_storage,
    sb_select_storage_by_root,
)
from ._hub_utils import (
    LaminDsn,
    LaminDsnModel,
    base62,
    get_storage_region,
    get_storage_type,
    secret,
    validate_db_arg,
    validate_schema_arg,
    validate_storage_root_arg,
    validate_unique_sqlite,
)
from ._settings_store import user_settings_file_email


def add_storage(root: str, account_id: UUID, hub: Client) -> UUID:
    # unlike storage keys, storage roots are always stored without the
    # trailing slash in the SQL database
    root = root.rstrip("/")
    validate_storage_root_arg(root)
    # check if storage exists already
    storage = sb_select_storage_by_root(root, hub)
    if storage is not None:
        logger.warning("storage exists already")
        return UUID(storage["id"])
    storage_region = get_storage_region(root)
    storage_type = get_storage_type(root)
    storage = sb_insert_storage(
        {
            "id": uuid4().hex,
            "lnid": base62(8),
            "created_by": account_id.hex,
            "root": root,
            "region": storage_region,
            "type": storage_type,
        },
        hub,
    )
    return UUID(storage["id"])


def init_instance(
    *,
    name: str,  # instance name
    storage: str,  # storage location on cloud
    db: Optional[str] = None,  # str has to be postgresdsn (use pydantic in the future)
    schema: Optional[str] = None,  # comma-separated list of schema names
) -> Union[str, UUID]:
    hub = connect_hub_with_auth()
    from .._settings import settings

    usettings = settings.user
    try:
        # validate input arguments
        schema_str = validate_schema_arg(schema)
        # storage is validated in add_storage
        validate_db_arg(db)

        # get storage and add if not yet there
        storage_id = add_storage(
            storage,
            account_id=usettings.uuid,
            hub=hub,
        )
        instance = sb_select_instance_by_name(usettings.uuid, name, hub)
        if instance is not None:
            return UUID(instance["id"])

        instance_id = uuid4()
        db_user_id = None
        if db is None:
            validate_unique_sqlite(storage_id=storage_id, name=name, hub=hub)
            sb_insert_instance(
                {
                    "id": instance_id.hex,
                    "account_id": usettings.uuid.hex,
                    "name": name,
                    "storage_id": storage_id.hex,
                    "schema_str": schema_str,
                    "public": False,
                },
                hub,
            )
        else:
            db_dsn = LaminDsnModel(db=db)
            db_user_id = uuid4().hex
            sb_insert_instance(
                {
                    "id": instance_id.hex,
                    "account_id": usettings.uuid.hex,
                    "name": name,
                    "storage_id": storage_id.hex,
                    "db": db,
                    "db_scheme": db_dsn.db.scheme,
                    "db_host": db_dsn.db.host,
                    "db_port": db_dsn.db.port,
                    "db_database": db_dsn.db.database,
                    "schema_str": schema_str,
                    "public": False,
                },
                hub,
            )
            sb_insert_db_user(
                {
                    "id": db_user_id,
                    "instance_id": instance_id.hex,
                    "db_user_name": db_dsn.db.user,
                    "db_user_password": db_dsn.db.password,
                },
                hub,
            )
        sb_insert_collaborator(
            {
                "instance_id": instance_id.hex,
                "account_id": usettings.uuid.hex,
                "db_user_id": db_user_id,
                "role": "admin",
            },
            hub,
        )
        return instance_id
    except APIError as api_error:
        uq_instance_db_error = (
            'duplicate key value violates unique constraint "uq_instance_db"'
        )
        if api_error.message == uq_instance_db_error:
            return "error-db-already-exists"
        raise api_error
    except Exception as e:
        raise e
    finally:
        hub.auth.sign_out()


def load_instance(
    *,
    owner: str,  # owner handle
    name: str,  # instance name
    _email: Optional[str] = None,
    _password: Optional[str] = None,
    _access_token: Optional[str] = None,
) -> Union[Tuple[dict, dict], str]:
    hub = connect_hub_with_auth(
        email=_email, password=_password, access_token=_access_token
    )
    try:
        # get account
        account = sb_select_account_by_handle(owner, hub)
        if account is None:
            return "account-not-exists"

        instance = sb_select_instance_by_name(account["id"], name, hub)
        if instance is None:
            return "instance-not-reachable"

        if not (instance["db"] is None or instance["db"].startswith("sqlite://")):
            # get db_account
            db_user = sb_select_db_user_by_instance(instance["id"], hub)
            if db_user is None:
                return "db-user-not-reachable"

            # construct dsn from instance and db_account fields
            db_dsn = LaminDsn.build(
                scheme=instance["db_scheme"],
                user=db_user["db_user_name"],
                password=db_user["db_user_password"],
                host=instance["db_host"],
                port=str(instance["db_port"]),
                database=instance["db_database"],
            )

            # override the db string with the constructed dsn
            instance["db"] = db_dsn

        # get default storage
        storage = sb_select_storage(instance["storage_id"], hub)
        if storage is None:
            return "storage-does-not-exist-on-hub"

        return instance, storage
    except Exception:
        return "loading-instance-failed"
    finally:
        hub.auth.sign_out()


def get_lamin_site_base_url():
    if "LAMIN_ENV" in os.environ:
        if os.environ["LAMIN_ENV"] == "local":
            return "http://localhost:3000"
        elif os.environ["LAMIN_ENV"] == "staging":
            return "https://staging.lamin.ai"
    return "https://lamin.ai"


def sign_up_hub(email) -> Union[str, Tuple[str, str, str]]:
    hub = connect_hub()
    password = secret()  # generate new password
    sign_up_kwargs = {"email": email, "password": password}
    if os.getenv("LAMIN_ENV") != "local":
        sign_up_kwargs["options"] = {
            "redirect_to": f"{get_lamin_site_base_url()}/signup"
        }
    try:
        # the only case we know when this errors is when the user already exists
        auth_response = hub.auth.sign_up(sign_up_kwargs)
    except Exception as e:
        logger.error(e)
        return "user-exists"
    user = auth_response.user
    # if user already exists a fake user object without identity is returned
    if auth_response.user.identities:
        # if user had called sign-up before, but not confirmed their email
        # the user has an identity with a wrong ID
        # we can check for it by comparing time stamps
        # see design note uL8Sjht0y4qg
        if user.confirmation_sent_at is not None:  # is None in local client
            diff = user.confirmation_sent_at - user.identities[0].last_sign_in_at
            if (
                diff.total_seconds() > 0.1
            ):  # the first time, this is on the order of microseconds
                raise RuntimeError(
                    "It seems you already signed up with this email. Please click on"
                    " the link in the confirmation email that you should have received"
                    " from lamin.ai."
                )
        logger.info(
            "Please *confirm* the sign-up email. After that, login with `lamin login"
            f" {email}`!\n\n"
            f"Generated password: {password}\n"
            f"Email & password are cached: {user_settings_file_email(email)}\n"  # noqa
            "Going forward, credentials are auto-loaded! "  # noqa
            "In case of loss, recover your password via email: https://lamin.ai"
        )
        return (
            password,
            auth_response.session.user.id,
            auth_response.session.access_token,
        )
    else:
        logger.error("user already exists! please login instead: `lamin login`")
        return "user-exists"


def sign_in_hub(
    email, password, handle=None
) -> Union[str, Tuple[UUID, str, str, str, str]]:
    hub = connect_hub()
    try:
        auth_response = hub.auth.sign_in_with_password(
            {
                "email": email,
                "password": password,
                "options": {"redirect_to": f"{get_lamin_site_base_url()}/signup"},
            }
        )
    except Exception as exception:  # this is bad, but I don't find APIError right now
        logger.error(exception)
        logger.error(
            "could not login. probably your password is wrong or you didn't complete"
            " signup."
        )
        return "could-not-login"
    data = hub.table("account").select("*").eq("id", auth_response.user.id).execute()
    if len(data.data) > 0:  # user is completely registered
        user_uuid = UUID(data.data[0]["id"])
        user_id = data.data[0]["lnid"]
        user_handle = data.data[0]["handle"]
        user_name = data.data[0]["name"]
        if handle is not None and handle != user_handle:
            logger.warning(
                f"using account handle {user_handle} (cached handle was {handle})"
            )
    else:  # user did not complete signup as usermeta has no matching row
        logger.error("complete signup on your account page.")
        return "complete-signup"
    hub.auth.sign_out()
    return (
        user_uuid,
        user_id,
        user_handle,
        user_name,
        auth_response.session.access_token,
    )
