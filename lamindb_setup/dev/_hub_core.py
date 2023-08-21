from typing import Optional, Tuple, Union
from uuid import UUID, uuid4

from lamin_utils import logger
from postgrest.exceptions import APIError

from ._hub_client import connect_hub, connect_hub_with_auth, get_lamin_site_base_url
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


def add_storage(
    root: str, account_handle: str, _access_token: Optional[str] = None
) -> Tuple[Optional[UUID], Optional[str]]:
    from botocore.exceptions import ClientError

    hub = connect_hub_with_auth(access_token=_access_token)
    try:
        validate_storage_root_arg(root)
        # get account
        account = sb_select_account_by_handle(account_handle, hub)

        # check if storage exists already
        storage = sb_select_storage_by_root(root, hub)
        if storage is not None:
            return storage["id"], None

        storage_region = get_storage_region(root)
        storage_type = get_storage_type(root)
        storage = sb_insert_storage(
            {
                "id": uuid4().hex,
                "lnid": base62(8),
                "created_by": account["id"],
                "root": root,
                "region": storage_region,
                "type": storage_type,
            },
            hub,
        )
        assert storage is not None

        return storage["id"], None
    except ClientError as exception:
        if exception.response["Error"]["Code"] == "NoSuchBucket":
            return None, "bucket-does-not-exists"
        else:
            return None, exception.response["Error"]["Message"]
    finally:
        hub.auth.sign_out()


def init_instance(
    *,
    owner: str,  # owner handle
    name: str,  # instance name
    storage: str,  # storage location on cloud
    db: Optional[str] = None,  # str has to be postgresdsn (use pydantic in the future)
    schema: Optional[str] = None,  # comma-separated list of schema names
    description: Optional[str] = None,
    public: Optional[bool] = None,
    # replace with token-based approach!
    _email: Optional[str] = None,
    _password: Optional[str] = None,
    _access_token: Optional[str] = None,
) -> Optional[str]:
    hub = connect_hub_with_auth(
        email=_email, password=_password, access_token=_access_token
    )
    try:
        # validate input arguments
        schema_str = validate_schema_arg(schema)
        # storage is validated in add_storage
        validate_db_arg(db)

        if db is not None:
            db_dsn = LaminDsnModel(db=db)
        else:
            db_dsn = None

        # get account
        account = sb_select_account_by_handle(owner, hub)
        if account is None:
            return "account-not-exists"

        # get storage and add if not yet there
        storage_root = storage.rstrip("/")  # current fix because of upath migration
        storage_id, message = add_storage(
            storage_root, account_handle=account["handle"], _access_token=_access_token
        )
        if message is not None:
            return message
        assert storage_id is not None

        instance = sb_select_instance_by_name(account["id"], name, hub)
        if instance is not None:
            return "instance-exists-already"

        validate_unique_sqlite(
            hub=hub, db=db, storage_id=storage_id, name=name, account=account
        )

        instance_id = uuid4().hex
        db_user_id = None

        if db_dsn is not None:
            db_user_id = uuid4().hex
            instance = sb_insert_instance(  # noqa
                {
                    "id": instance_id,
                    "account_id": account["id"],
                    "name": name,
                    "storage_id": storage_id,
                    "db": db,
                    "db_scheme": db_dsn.db.scheme,
                    "db_host": db_dsn.db.host,
                    "db_port": db_dsn.db.port,
                    "db_database": db_dsn.db.database,
                    "schema_str": schema_str,
                    "public": False if public is None else public,
                    "description": description,
                },
                hub,
            )

            db_user = sb_insert_db_user(  # noqa
                {
                    "id": db_user_id,
                    "instance_id": instance_id,
                    "db_user_name": db_dsn.db.user,
                    "db_user_password": db_dsn.db.password,
                },
                hub,
            )
        else:
            sb_insert_instance(
                {
                    "id": instance_id,
                    "account_id": account["id"],
                    "name": name,
                    "storage_id": storage_id,
                    "db": db,
                    "schema_str": schema_str,
                    "public": False if public is None else public,
                    "description": description,
                },
                hub,
            )

        sb_insert_collaborator(
            {
                "instance_id": instance_id,
                "account_id": account["user_id"],
                "db_user_id": db_user_id,
                "role": "admin",
            },
            hub,
        )

        # upon successful insert of a new row in the instance table
        # (and all associated tables), return None
        # clients test for this return value, hence, don't change it
        return None
    except APIError as api_error:
        uq_instance_db_error = (
            'duplicate key value violates unique constraint "uq_instance_db"'
        )
        if api_error.message == uq_instance_db_error:
            return "db-already-exists"
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


def sign_up_hub(email) -> str:
    from .._settings_store import settings_dir

    hub = connect_hub()
    password = secret()  # generate new password
    auth_response = hub.auth.sign_up(
        {
            "email": email,
            "password": password,
            "options": {"redirect_to": f"{get_lamin_site_base_url()}/signup"},
        }
    )
    user = auth_response.user
    # if user already exists a fake user object without identity is returned
    if auth_response.user.identities:
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
            "Please *confirm* the sign-up email. After that, login with `lamin login"
            f" {email}`!\n\n"
            f"Generated password: {password}\n"
            f"Email & password are cached: {usettings_file}\n"  # noqa
            "Going forward, credentials are auto-loaded! "  # noqa
            "In case of loss, recover your password via email: https://lamin.ai"
        )
        return password
    else:
        return "user-exists"


def sign_in_hub(email, password, handle=None):
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
    return user_id, user_handle, user_name, auth_response.session.access_token
