from __future__ import annotations

import json
import os
import uuid
from importlib import metadata
from typing import TYPE_CHECKING
from uuid import UUID

from lamin_utils import logger
from postgrest.exceptions import APIError

from ._hub_client import (
    call_with_fallback,
    call_with_fallback_auth,
    connect_hub,
)
from ._hub_crud import (
    _delete_instance_record,
    select_account_by_handle,
    select_db_user_by_instance,
    select_instance_by_id_with_storage,
    select_instance_by_name,
    select_instance_by_owner_name,
    select_storage,
)
from ._hub_crud import update_instance as _update_instance_record
from ._hub_utils import (
    LaminDsn,
    LaminDsnModel,
)
from ._settings import settings
from ._settings_storage import StorageSettings, base62

if TYPE_CHECKING:
    from supabase import Client  # type: ignore

    from ._settings_instance import InstanceSettings


def delete_storage_record(
    storage_uuid: UUID,
) -> None:
    return call_with_fallback_auth(
        _delete_storage_record,
        storage_uuid=storage_uuid,
    )


def _delete_storage_record(storage_uuid: UUID, client: Client) -> None:
    if storage_uuid is None:
        return None
    response = client.table("storage").delete().eq("id", storage_uuid.hex).execute()
    if response.data:
        logger.important(f"deleted storage record on hub {storage_uuid.hex}")
    else:
        raise PermissionError(
            f"Deleting of storage with {storage_uuid.hex} was not successful. Probably, you"
            " don't have sufficient permissions."
        )


def update_instance_record(instance_uuid: UUID, fields: dict) -> None:
    return call_with_fallback_auth(
        _update_instance_record, instance_id=instance_uuid.hex, instance_fields=fields
    )


def get_storage_records_for_instance(
    instance_id: UUID,
) -> list[dict[str, str | int]]:
    return call_with_fallback_auth(
        _get_storage_records_for_instance,
        instance_id=instance_id,
    )


def _get_storage_records_for_instance(
    instance_id: UUID, client: Client
) -> list[dict[str, str | int]]:
    response = (
        client.table("storage").select("*").eq("instance_id", instance_id.hex).execute()
    )
    return response.data


def _select_storage(
    ssettings: StorageSettings, update_uid: bool, client: Client
) -> bool:
    root = ssettings.root_as_str
    response = client.table("storage").select("*").eq("root", root).execute()
    if not response.data:
        return False
    else:
        existing_storage = response.data[0]
        if ssettings._instance_id is not None:
            # consider storage settings that are meant to be managed by an instance
            if UUID(existing_storage["instance_id"]) != ssettings._instance_id:
                # everything is alright if the instance_id matches
                # we're probably just switching storage locations
                # below can be turned into a warning and then delegate the error
                # to a unique constraint violation below
                raise ValueError(
                    f"Storage root {root} is already managed by instance {existing_storage['instance_id']}."
                )
        else:
            # if the request is agnostic of the instance, that's alright,
            # we'll update the instance_id with what's stored in the hub
            ssettings._instance_id = UUID(existing_storage["instance_id"])
        ssettings._uuid_ = UUID(existing_storage["id"])
        if update_uid:
            ssettings._uid = existing_storage["lnid"]
        else:
            assert ssettings._uid == existing_storage["lnid"]
        return True


def init_storage(
    ssettings: StorageSettings,
) -> None:
    if settings.user.handle != "anonymous":
        return call_with_fallback_auth(
            _init_storage,
            ssettings=ssettings,
        )
    else:
        storage_exists = call_with_fallback(
            _select_storage, ssettings=ssettings, update_uid=True
        )
        if storage_exists:
            return None
        else:
            raise ValueError("Log in to create a storage location on the hub.")


def _init_storage(ssettings: StorageSettings, client: Client) -> None:
    from lamindb_setup import settings

    # storage roots are always stored without the trailing slash in the SQL
    # database
    root = ssettings.root_as_str
    if _select_storage(ssettings, update_uid=True, client=client):
        return None
    if ssettings.type_is_cloud:
        id = uuid.uuid5(uuid.NAMESPACE_URL, root)
    else:
        id = uuid.uuid4()
    if ssettings._instance_id is None:
        logger.warning(
            f"will manage storage location {ssettings.root_as_str} with instance {settings.instance.slug}"
        )
        ssettings._instance_id = settings.instance._id
    fields = {
        "id": id.hex,
        "lnid": ssettings.uid,
        "created_by": settings.user._uuid.hex,  # type: ignore
        "root": root,
        "region": ssettings.region,
        "type": ssettings.type,
        "instance_id": ssettings._instance_id.hex,
        # the empty string is important as we want the user flow to be through LaminHub
        # if this errors with unique constraint error, the user has to update
        # the description in LaminHub
        "description": "",
    }
    # TODO: add error message for violated unique constraint
    # on root & description
    client.table("storage").upsert(fields).execute()
    ssettings._uuid_ = id
    return None


def delete_instance(identifier: UUID | str, require_empty: bool = True) -> str | None:
    return call_with_fallback_auth(
        _delete_instance, identifier=identifier, require_empty=require_empty
    )


def _delete_instance(
    identifier: UUID | str, require_empty: bool, client: Client
) -> str | None:
    """Fully delete an instance in the hub.

    This function deletes the relevant instance and storage records in the hub,
    conditional on the emptiness of the storage location.
    """
    from ._settings_storage import mark_storage_root
    from .upath import check_storage_is_empty, create_path

    # the "/" check is for backward compatibility with the old identifier format
    if isinstance(identifier, UUID) or "/" not in identifier:
        if isinstance(identifier, UUID):
            instance_id_str = identifier.hex
        else:
            instance_id_str = identifier
        instance_with_storage = select_instance_by_id_with_storage(
            instance_id=instance_id_str, client=client
        )
    else:
        owner, name = identifier.split("/")
        instance_with_storage = select_instance_by_owner_name(
            owner=owner, name=name, client=client
        )

    if instance_with_storage is None:
        logger.important("not deleting instance from hub as instance not found there")
        return "instance-not-found"

    storage_records = _get_storage_records_for_instance(
        UUID(instance_with_storage["id"]),
        client,
    )
    if require_empty:
        for storage_record in storage_records:
            account_for_sqlite_file = (
                instance_with_storage["db_scheme"] is None
                and instance_with_storage["storage"]["root"] == storage_record["root"]
            )
            root_string = storage_record["root"]
            # gate storage and instance deletion on empty storage location for
            if client.auth.get_session() is not None:
                access_token = client.auth.get_session().access_token
            else:
                access_token = None
            root_path = create_path(root_string, access_token)
            mark_storage_root(
                root_path,
                storage_record["lnid"],  # type: ignore
            )  # address permission error
            check_storage_is_empty(
                root_path, account_for_sqlite_file=account_for_sqlite_file
            )
    _update_instance_record(instance_with_storage["id"], {"storage_id": None}, client)
    # first delete the storage records because we will turn instance_id on
    # storage into a FK soon
    for storage_record in storage_records:
        _delete_storage_record(UUID(storage_record["id"]), client)  # type: ignore
    _delete_instance_record(UUID(instance_with_storage["id"]), client)
    return None


def delete_instance_record(
    instance_id: UUID,
) -> None:
    return call_with_fallback_auth(
        _delete_instance_record,
        instance_id=instance_id,
    )


def init_instance(isettings: InstanceSettings) -> None:
    return call_with_fallback_auth(_init_instance, isettings=isettings)


def _init_instance(isettings: InstanceSettings, client: Client) -> None:
    from ._settings import settings

    try:
        lamindb_version = metadata.version("lamindb")
    except metadata.PackageNotFoundError:
        lamindb_version = None
    fields = {
        "id": isettings._id.hex,
        "account_id": settings.user._uuid.hex,  # type: ignore
        "name": isettings.name,
        "storage_id": isettings.storage._uuid.hex,  # type: ignore
        "lnid": isettings.uid,
        "schema_str": isettings._schema_str,
        "lamindb_version": lamindb_version,
        "public": False,
    }
    if isettings.dialect != "sqlite":
        db_dsn = LaminDsnModel(db=isettings.db)
        db_fields = {
            "db_scheme": db_dsn.db.scheme,
            "db_host": db_dsn.db.host,
            "db_port": db_dsn.db.port,
            "db_database": db_dsn.db.database,
        }
        fields.update(db_fields)
    # I'd like the following to be an upsert, but this seems to violate RLS
    # Similarly, if we don't specify `returning="minimal"`, we'll violate RLS
    # we could make this idempotent by catching an error, but this seems dangerous
    # as then init_instance is no longer idempotent
    try:
        client.table("instance").insert(fields, returning="minimal").execute()
    except APIError:
        logger.warning(
            f"instance already existed at: https://lamin.ai/{isettings.owner}/{isettings.name}"
        )
        return None
    client.table("storage").update(
        {"instance_id": isettings._id.hex, "is_default": True}
    ).eq("id", isettings.storage._uuid.hex).execute()  # type: ignore
    logger.important(f"go to: https://lamin.ai/{isettings.owner}/{isettings.name}")


def connect_instance(
    *,
    owner: str,  # account_handle
    name: str,  # instance_name
) -> tuple[dict, dict] | str:
    from ._settings import settings

    if settings.user.handle != "anonymous":
        return call_with_fallback_auth(_connect_instance, owner=owner, name=name)
    else:
        return call_with_fallback(_connect_instance, owner=owner, name=name)


def _connect_instance(
    *,
    owner: str,  # account_handle
    name: str,  # instance_name
    client: Client,
) -> tuple[dict, dict] | str:
    instance_account_storage = select_instance_by_owner_name(owner, name, client)
    if instance_account_storage is None:
        # try the via single requests, will take more time
        account = select_account_by_handle(owner, client)
        if account is None:
            return "account-not-exists"
        instance = select_instance_by_name(account["id"], name, client)
        if instance is None:
            return "instance-not-found"
        # get default storage
        storage = select_storage(instance["storage_id"], client)
        if storage is None:
            return "storage-does-not-exist-on-hub"
    else:
        account = instance_account_storage.pop("account")
        storage = instance_account_storage.pop("storage")
        instance = instance_account_storage
    # check if is postgres instance
    # this used to be a check for `instance["db"] is not None` in earlier versions
    # removed this on 2022-10-25 and can remove from the hub probably for lamindb 1.0
    if instance["db_scheme"] is not None:
        db_user = select_db_user_by_instance(instance["id"], client)
        if db_user is None:
            name, password = "none", "none"
        else:
            name, password = db_user["db_user_name"], db_user["db_user_password"]
        # construct dsn from instance and db_account fields
        db_dsn = LaminDsn.build(
            scheme=instance["db_scheme"],
            user=name,
            password=password,
            host=instance["db_host"],
            port=instance["db_port"],
            database=instance["db_database"],
        )
        instance["db"] = db_dsn
    return instance, storage


def access_aws(storage_root: str, access_token: str | None = None) -> dict[str, dict]:
    from ._settings import settings

    if settings.user.handle != "anonymous" or access_token is not None:
        storage_root_info = call_with_fallback_auth(
            _access_aws, storage_root=storage_root, access_token=access_token
        )
        return storage_root_info
    else:
        raise RuntimeError("Can only get access to AWS if authenticated.")


def _access_aws(*, storage_root: str, client: Client) -> dict[str, dict]:
    import lamindb_setup

    storage_root_info: dict[str, dict] = {"credentials": {}, "accessibility": {}}
    response = client.functions.invoke(
        "access-aws",
        invoke_options={"body": {"storage_root": storage_root}},
    )
    if response is not None and response != b"{}":
        data = json.loads(response)

        loaded_credentials = data["Credentials"]
        loaded_accessibility = data["StorageAccessibility"]

        credentials = storage_root_info["credentials"]
        credentials["key"] = loaded_credentials["AccessKeyId"]
        credentials["secret"] = loaded_credentials["SecretAccessKey"]
        credentials["token"] = loaded_credentials["SessionToken"]

        accessibility = storage_root_info["accessibility"]
        accessibility["storage_root"] = loaded_accessibility["storageRoot"]
        accessibility["is_managed"] = loaded_accessibility["isManaged"]
    elif lamindb_setup._TESTING:
        raise RuntimeError(f"access-aws errored: {response}")
    return storage_root_info


def get_lamin_site_base_url():
    if "LAMIN_ENV" in os.environ:
        if os.environ["LAMIN_ENV"] == "local":
            return "http://localhost:3000"
        elif os.environ["LAMIN_ENV"] == "staging":
            return "https://staging.lamin.ai"
    return "https://lamin.ai"


def sign_up_local_hub(email) -> str | tuple[str, str, str]:
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


def _sign_in_hub(email: str, password: str, handle: str | None, client: Client):
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
    email: str, password: str, handle: str | None = None
) -> Exception | tuple[UUID, str, str, str, str]:
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
