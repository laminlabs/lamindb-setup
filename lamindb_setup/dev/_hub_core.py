import os
from typing import Optional, Tuple, Union
from uuid import UUID, uuid4
from lamin_utils import logger
from supabase import Client

from ._hub_client import (
    connect_hub,
    call_with_fallback_auth,
    call_with_fallback,
)
from ._hub_crud import (
    select_instance_by_owner_name,
    sb_insert_collaborator,
    sb_insert_instance,
    sb_insert_db_user,
    sb_update_db_user,
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
    return call_with_fallback_auth(
        _init_instance, name=name, storage=storage, db=db, schema=schema
    )


def _init_instance(
    *,
    name: str,  # instance name
    storage: str,  # storage location on cloud
    db: Optional[str] = None,  # str has to be postgresdsn (use pydantic in the future)
    schema: Optional[str] = None,  # comma-separated list of schema names
    client: Client,
) -> Union[str, UUID]:
    from .._settings import settings

    usettings = settings.user
    # validate input arguments
    schema_str = validate_schema_arg(schema)

    # get storage and add if not yet there
    storage_id = add_storage(
        storage,
        account_id=usettings.uuid,
        hub=client,
    )
    instance = sb_select_instance_by_name(usettings.uuid, name, client)
    if instance is not None:
        return UUID(instance["id"])

    # for internal use when creating instances through CICD
    instance_id_str = os.getenv("LAMINDB_INSTANCE_ID_INIT")
    if instance_id_str is None:
        instance_id = uuid4()
    else:
        instance_id = UUID(instance_id_str)
    # sqlite
    if db is None:
        validate_unique_sqlite(storage_id=storage_id, name=name, client=client)
        sb_insert_instance(
            {
                "id": instance_id.hex,
                "account_id": usettings.uuid.hex,
                "name": name,
                "storage_id": storage_id.hex,
                "schema_str": schema_str,
                "public": False,
            },
            client,
        )
    # postgres
    else:
        db_dsn = LaminDsnModel(db=db)
        sb_insert_instance(
            {
                "id": instance_id.hex,
                "account_id": usettings.uuid.hex,
                "name": name,
                "storage_id": storage_id.hex,
                "db_scheme": db_dsn.db.scheme,
                "db_host": db_dsn.db.host,
                "db_port": db_dsn.db.port,
                "db_database": db_dsn.db.database,
                "schema_str": schema_str,
                "public": False,
            },
            client,
        )
    sb_insert_collaborator(
        {
            "instance_id": instance_id.hex,
            "account_id": usettings.uuid.hex,
            "role": "admin",
        },
        client,
    )
    return instance_id


def set_db_user(
    *,
    db: str,
    instance_id: Optional[UUID] = None,
) -> None:
    return call_with_fallback_auth(_set_db_user, db=db, instance_id=instance_id)


def _set_db_user(
    *,
    db: str,
    instance_id: Optional[UUID] = None,
    client: Client,
) -> None:
    if instance_id is None:
        from .._settings import settings

        instance_id = settings.instance.id
    db_dsn = LaminDsnModel(db=db)
    db_user = sb_select_db_user_by_instance(instance_id.hex, client)
    if db_user is None:
        sb_insert_db_user(
            {
                "id": uuid4().hex,
                "instance_id": instance_id.hex,
                "db_user_name": db_dsn.db.user,
                "db_user_password": db_dsn.db.password,
            },
            client,
        )
    else:
        sb_update_db_user(
            db_user["id"],
            {
                "instance_id": instance_id.hex,
                "db_user_name": db_dsn.db.user,
                "db_user_password": db_dsn.db.password,
            },
            client,
        )


def load_instance(
    *,
    owner: str,  # account_handle
    name: str,  # instance_name
) -> Union[Tuple[dict, dict], str]:
    return call_with_fallback_auth(_load_instance, owner=owner, name=name)


def _load_instance(
    *,
    owner: str,  # account_handle
    name: str,  # instance_name
    client: Client,
) -> Union[Tuple[dict, dict], str]:
    instance_account = select_instance_by_owner_name(owner, name, client)
    if instance_account is None:
        account = sb_select_account_by_handle(owner, client)
        if account is None:
            return "account-not-exists"
        instance = sb_select_instance_by_name(account["id"], name, client)
        if instance is None:
            return "instance-not-reachable"
    else:
        account = instance_account.pop("account")
        instance = instance_account
    # check if is postgres instance
    # this used to be a check for `instance["db"] is not None` in earlier versions
    # removed this on 2022-10-25 and can remove from the hub probably for lamindb 1.0
    if instance["db_scheme"] is not None:
        # get db_user
        db_user = sb_select_db_user_by_instance(instance["id"], client)
        if db_user is None:
            db_user_name = "none"
            db_user_password = "none"
        else:
            db_user_name = db_user["db_user_name"]
            db_user_password = db_user["db_user_password"]
        # construct dsn from instance and db_account fields
        db_dsn = LaminDsn.build(
            scheme=instance["db_scheme"],
            user=db_user_name,
            password=db_user_password,
            host=instance["db_host"],
            port=instance["db_port"],
            database=instance["db_database"],
        )
        # override the db string with the constructed dsn
        instance["db"] = db_dsn
    # get default storage
    storage = sb_select_storage(instance["storage_id"], client)
    if storage is None:
        return "storage-does-not-exist-on-hub"
    return instance, storage


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


def _sign_in_hub(email: str, password: str, handle: Optional[str], client: Client):
    auth = client.auth.sign_in_with_password(
        {
            "email": email,
            "password": password,
        }
    )
    data = client.table("account").select("*").eq("id", auth.user.id).execute().data
    if data:  # sync data from hub to local cache in case it was updated on the hub
        user_uuid = UUID(data[0]["id"])
        user_id = data[0]["lnid"]
        user_handle = data[0]["handle"]
        user_name = data[0]["name"]
        if handle is not None and handle != user_handle:
            logger.warning(
                f"using account handle {user_handle} (cached handle was {handle})"
            )
    else:  # user did not complete signup as usermeta has no matching row
        logger.error("complete signup on your account page.")
        return "complete-signup"
    return (
        user_uuid,
        user_id,
        user_handle,
        user_name,
        auth.session.access_token,
    )


def sign_in_hub(
    email: str, password: str, handle: Optional[str] = None
) -> Union[Exception, Tuple[UUID, str, str, str, str]]:
    try:
        result = call_with_fallback(
            _sign_in_hub, email=email, password=password, handle=handle
        )
    except Exception as exception:  # this is bad, but I don't find APIError right now
        logger.error(exception)
        logger.error(
            "Could not login. Probably your password is wrong or you didn't complete"
            " signup."
        )
        return exception
    return result
