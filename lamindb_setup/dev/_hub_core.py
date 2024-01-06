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
)


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
    id: UUID,
    name: str,  # instance name
    storage: str,  # storage location on cloud
    db: Optional[str] = None,  # str has to be postgresdsn (use pydantic in the future)
    schema: Optional[str] = None,  # comma-separated list of schema names
    lamindb_version: Optional[str] = None,  # the installed lamindb version, optional
) -> Union[str, UUID]:
    return call_with_fallback_auth(
        _init_instance,
        id=id,
        name=name,
        storage=storage,
        db=db,
        schema=schema,
        lamindb_version=lamindb_version,
    )


def _init_instance(
    *,
    id: UUID,
    name: str,  # instance name
    storage: str,  # storage location on cloud
    db: Optional[str] = None,  # str has to be postgresdsn (use pydantic in the future)
    schema: Optional[str] = None,  # comma-separated list of schema names
    client: Client,
    lamindb_version: Optional[str] = None,
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
    # sqlite
    common_fields = {
        "id": id.hex,
        "account_id": usettings.uuid.hex,
        "name": name,
        "storage_id": storage_id.hex,
        "schema_str": schema_str,
        "lamindb_version": lamindb_version,
        "public": False,
    }
    if db is None:
        sb_insert_instance(
            common_fields,
            client,
        )
    # postgres
    else:
        db_dsn = LaminDsnModel(db=db)
        common_fields.update(
            {
                "db_scheme": db_dsn.db.scheme,
                "db_host": db_dsn.db.host,
                "db_port": db_dsn.db.port,
                "db_database": db_dsn.db.database,
            }
        )
        sb_insert_instance(common_fields, client)
    sb_insert_collaborator(
        {
            "instance_id": id.hex,
            "account_id": usettings.uuid.hex,
            "role": "admin",
        },
        client,
    )
    return id


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
    from .._settings import settings

    if settings.user.handle != "anonymous":
        return call_with_fallback_auth(_load_instance, owner=owner, name=name)
    else:
        return call_with_fallback(_load_instance, owner=owner, name=name)


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


def sign_up_local_hub(email) -> Union[str, Tuple[str, str, str]]:
    # raises gotrue.errors.AuthApiError: User already registered
    password = secret()  # generate new password
    sign_up_kwargs = {"email": email, "password": password}
    client = connect_hub()
    auth_response = client.auth.sign_up(sign_up_kwargs)
    client.auth.sign_out()
    return (
        password,
        auth_response.session.user.id,
        auth_response.session.access_token,
    )


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
