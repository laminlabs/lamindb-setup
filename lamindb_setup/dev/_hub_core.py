import os
from typing import Optional, Tuple, Union
import uuid
from uuid import UUID, uuid4
from lamin_utils import logger
from supabase import Client
from supafunc.errors import FunctionsRelayError, FunctionsHttpError
import lamindb_setup
import json
from importlib import metadata
from ._settings_instance import InstanceSettings
from ._settings_storage import StorageSettings, base62


from ._hub_client import (
    connect_hub,
    call_with_fallback_auth,
    call_with_fallback,
)
from ._hub_crud import (
    select_instance_by_owner_name,
    sb_insert_db_user,
    sb_update_db_user,
    sb_select_account_by_handle,
    sb_select_db_user_by_instance,
    sb_select_instance_by_name,
    sb_select_storage,
)
from ._hub_utils import (
    LaminDsn,
    LaminDsnModel,
)


def delete_storage(
    ssettings: StorageSettings,
) -> None:
    return call_with_fallback_auth(
        _delete_storage,
        ssettings=ssettings,
    )


def _delete_storage(ssettings: StorageSettings, client: Client) -> None:
    if ssettings.uuid is None:
        return None
    logger.important(f"deleting storage {ssettings.root_as_str}")
    client.table("storage").delete().eq("id", ssettings.uuid.hex).execute()


def init_storage(
    ssettings: StorageSettings,
) -> UUID:
    return call_with_fallback_auth(
        _init_storage,
        ssettings=ssettings,
    )


def _init_storage(ssettings: StorageSettings, client: Client) -> UUID:
    from lamindb_setup import settings

    # storage roots are always stored without the trailing slash in the SQL
    # database
    root = ssettings.root_as_str
    # in the future, we could also encode a prefix to model local storage
    # locations
    # f"{prefix}{root}"
    id = uuid.uuid5(uuid.NAMESPACE_URL, root)
    fields = dict(
        id=id.hex,
        lnid=ssettings.uid,
        created_by=settings.user.uuid.hex,
        root=root,
        region=ssettings.region,
        type=ssettings.type,
        aws_account_id=ssettings._aws_account_id,
        description=ssettings._description,
    )
    client.table("storage").upsert(fields).execute()
    return id


def delete_instance(
    isettings: InstanceSettings,
) -> None:
    return call_with_fallback_auth(
        _delete_instance,
        isettings=isettings,
    )


def _delete_instance(isettings: InstanceSettings, client: Client) -> None:
    logger.important(f"deleting instance {isettings.id}")
    client.table("instance").delete().eq("id", isettings.id.hex).execute()


def init_instance(isettings: InstanceSettings) -> None:
    return call_with_fallback_auth(_init_instance, isettings=isettings)


def _init_instance(isettings: InstanceSettings, client: Client) -> None:
    from .._settings import settings

    try:
        lamindb_version = metadata.version("lamindb")
    except metadata.PackageNotFoundError:
        lamindb_version = None
    fields = dict(
        id=isettings.id.hex,
        account_id=settings.user.uuid.hex,
        name=isettings.name,
        storage_id=isettings.storage.uuid.hex,  # type: ignore
        schema_str=isettings._schema_str,
        lamindb_version=lamindb_version,
        public=False,
    )
    if isettings.dialect != "sqlite":
        db_dsn = LaminDsnModel(db=isettings.db)
        fields.update(
            {
                "db_scheme": db_dsn.db.scheme,
                "db_host": db_dsn.db.host,
                "db_port": db_dsn.db.port,
                "db_database": db_dsn.db.database,
            }
        )
    # I'd like the following to be an upsert, but this seems to violate RLS
    # Similarly, if we don't specify `returning="minimal"`, we'll violate RLS
    # Hence, to make this idempotent, we'll use an insert and catch the erro
    client.table("instance").insert(fields, returning="minimal").execute()
    logger.save(f"browse to: https://lamin.ai/{isettings.owner}/{isettings.name}")


def set_db_user(
    *,
    db: str,
    instance_id: UUID,
) -> None:
    return call_with_fallback_auth(_set_db_user, db=db, instance_id=instance_id)


def _set_db_user(
    *,
    db: str,
    instance_id: UUID,
    client: Client,
) -> None:
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


def access_aws() -> None:
    from .._settings import settings

    if settings.user.handle != "anonymous":
        call_with_fallback_auth(_access_aws)
        logger.important(
            "exported AWS credentials as env variables, valid for 12 hours"
        )
    else:
        raise RuntimeError("Can only get access to AWS if authenticated.")


def _access_aws(client: Client):
    from time import sleep

    response = None
    max_retries = 5
    for retry in range(max_retries):
        try:
            response = client.functions.invoke("access-aws")
        except (FunctionsRelayError, FunctionsHttpError, json.JSONDecodeError) as error:
            print("no valid response, retry", response)
            sleep(1)
            if retry == max_retries - 1:
                raise error
            else:
                continue
        if response is not None:
            credentials = json.loads(response)["Credentials"]
            os.environ["AWS_ACCESS_KEY_ID"] = credentials["AccessKeyId"]
            os.environ["AWS_SECRET_ACCESS_KEY"] = credentials["SecretAccessKey"]
            os.environ["AWS_SESSION_TOKEN"] = credentials["SessionToken"]
            break
        elif lamindb_setup._TESTING:
            raise RuntimeError(f"access-aws errored: {response}")


def get_lamin_site_base_url():
    if "LAMIN_ENV" in os.environ:
        if os.environ["LAMIN_ENV"] == "local":
            return "http://localhost:3000"
        elif os.environ["LAMIN_ENV"] == "staging":
            return "https://staging.lamin.ai"
    return "https://lamin.ai"


def sign_up_local_hub(email) -> Union[str, Tuple[str, str, str]]:
    # raises gotrue.errors.AuthApiError: User already registered
    password = base62(40)  # generate new password
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
