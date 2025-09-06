from __future__ import annotations

import json
import os
import uuid
from importlib import metadata
from typing import TYPE_CHECKING, Literal
from uuid import UUID

import jwt
from lamin_utils import logger
from postgrest.exceptions import APIError

from lamindb_setup._migrate import check_whether_migrations_in_sync

from ._hub_client import (
    call_with_fallback,
    call_with_fallback_auth,
    connect_hub,
    request_with_auth,
)
from ._hub_crud import (
    _delete_instance_record,
    select_account_by_handle,
    select_db_user_by_instance,
    select_default_storage_by_instance_id,
    select_instance_by_id_with_storage,
    select_instance_by_name,
    select_instance_by_owner_name,
)
from ._hub_crud import update_instance as _update_instance_record
from ._hub_utils import (
    LaminDsn,
    LaminDsnModel,
)
from ._settings import settings
from ._settings_instance import InstanceSettings
from ._settings_storage import StorageSettings, base62, instance_uid_from_uuid
from .hashing import hash_and_encode_as_b62

if TYPE_CHECKING:
    from supabase import Client  # type: ignore


def delete_storage_record(
    storage_info: dict[str, str] | StorageSettings,
    access_token: str | None = None,
) -> None:
    if isinstance(storage_info, StorageSettings):
        storage_info = {"id": storage_info._uuid.hex, "root": storage_info.root_as_str}  # type: ignore
    return call_with_fallback_auth(
        _delete_storage_record, storage_info=storage_info, access_token=access_token
    )


def _delete_storage_record(storage_info: dict[str, str], client: Client) -> None:
    storage_uuid = UUID(storage_info["id"])
    if storage_uuid is None:
        return None
    response = client.table("storage").delete().eq("id", storage_uuid.hex).execute()
    if response.data:
        logger.important(
            f"deleted storage record on hub {storage_uuid.hex} | {storage_info['root']}"
        )
    else:
        raise PermissionError(
            f"Deleting of storage {storage_uuid.hex} ({storage_info['root']}) was not successful. Probably, you"
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


def select_space(lnid: str) -> dict | None:
    return call_with_fallback_auth(
        _select_space,
        lnid=lnid,
    )


def _select_space(lnid: str, client: Client) -> dict | None:
    response = client.table("space").select("*").eq("lnid", lnid).execute()
    return response.data[0] if response.data else None


def update_storage_with_space(storage_lnid: str, space_lnid: str) -> dict | None:
    return call_with_fallback_auth(
        _update_storage_with_space,
        storage_lnid=storage_lnid,
        space_lnid=space_lnid,
    )


def _update_storage_with_space(
    storage_lnid: str, space_lnid: str, client: Client
) -> dict | None:
    # unfortunately these are two network requests
    # but postgrest doesn't allow doing it in one
    try:
        space_response = (
            client.table("space").select("id").eq("lnid", space_lnid).execute()
        )

        if not space_response.data:
            raise ValueError(
                f"Try again! Space with lnid '{space_lnid}' not found on hub"
            )

        space_id = space_response.data[0]["id"]

        update_response = (
            client.table("storage")
            .update({"space_id": space_id})
            .eq("lnid", storage_lnid)
            .execute()
        )

        if not update_response.data:
            raise ValueError(
                f"Try again! Storage with lnid '{storage_lnid}' not found or update failed on hub"
            )

        return update_response.data[0]

    except Exception as e:
        print(f"Try again! Error updating storage in hub: {e}")
        return None


def select_storage(lnid: str) -> dict | None:
    return call_with_fallback_auth(
        _select_storage,
        lnid=lnid,
    )


def _select_storage(lnid: str, client: Client) -> dict | None:
    response = client.table("storage").select("*").eq("lnid", lnid).execute()
    return response.data[0] if response.data else None


def _select_storage_by_settings(
    ssettings: StorageSettings, update_uid: bool, client: Client
) -> bool:
    root = ssettings.root_as_str
    response = client.table("storage").select("*").eq("root", root).execute()
    if not response.data:
        return False
    else:
        existing_storage = response.data[0]
        if existing_storage["instance_id"] is None:
            # if there is no instance_id, the storage location should not be on the hub
            # this can only occur if instance init fails halfway through and is not cleaned up
            # we're patching the situation here
            existing_storage["instance_id"] = (
                ssettings._instance_id.hex
                if ssettings._instance_id is not None
                else None
            )
        if ssettings._instance_id is not None:
            if UUID(existing_storage["instance_id"]) != ssettings._instance_id:
                logger.debug(
                    f"referencing storage location {root}, which is managed by instance {existing_storage['instance_id']}"
                )
        ssettings._instance_id = UUID(existing_storage["instance_id"])
        ssettings._uuid_ = UUID(existing_storage["id"])
        if update_uid:
            ssettings._uid = existing_storage["lnid"]
        else:
            assert ssettings._uid == existing_storage["lnid"]
        return True


def _select_storage_or_parent(path: str, client: Client) -> dict | None:
    result = client.rpc("existing_root_or_child", {"_path": path}).execute().data
    if result["root"] is None:
        return None
    result["uid"] = result.pop("lnid")
    result["instance_uid"] = instance_uid_from_uuid(UUID(result.pop("instance_id")))
    return result


def select_storage_or_parent(path: str, access_token: str | None = None) -> dict | None:
    if settings.user.handle != "anonymous" or access_token is not None:
        return call_with_fallback_auth(
            _select_storage_or_parent,
            path=path,
            access_token=access_token,
        )
    else:
        return call_with_fallback(_select_storage_or_parent, path=path)


def init_storage_hub(
    ssettings: StorageSettings,
    created_by: UUID | None = None,
    access_token: str | None = None,
    prevent_creation: bool = False,
    is_default: bool = False,
    space_id: UUID | None = None,
) -> Literal["hub-record-retrieved", "hub-record-created", "hub-record-not-created"]:
    """Creates or retrieves an existing storage record from the hub."""
    if settings.user.handle != "anonymous" or access_token is not None:
        return call_with_fallback_auth(
            _init_storage_hub,
            ssettings=ssettings,
            created_by=created_by,
            access_token=access_token,
            prevent_creation=prevent_creation,
            is_default=is_default,
            space_id=space_id,
        )
    else:
        storage_exists = call_with_fallback(
            _select_storage_by_settings, ssettings=ssettings, update_uid=True
        )
        if storage_exists:
            return "hub-record-retrieved"
        else:
            return "hub-record-not-created"


def _init_storage_hub(
    client: Client,
    ssettings: StorageSettings,
    created_by: UUID | None = None,
    prevent_creation: bool = False,
    is_default: bool = False,
    space_id: UUID | None = None,
) -> Literal["hub-record-retrieved", "hub-record-created", "hub-record-not-created"]:
    from lamindb_setup import settings

    created_by = settings.user._uuid if created_by is None else created_by
    # storage roots are always stored without the trailing slash in the SQL
    # database
    root = ssettings.root_as_str
    if _select_storage_by_settings(ssettings, update_uid=True, client=client):
        return "hub-record-retrieved"
    if prevent_creation:
        return "hub-record-not-created"
    if ssettings.type_is_cloud:
        hash_string = (
            root if ssettings.type_is_cloud else f"{ssettings.region}://{root}"
        )
        id = uuid.uuid5(uuid.NAMESPACE_URL, hash_string)
    else:
        id = uuid.uuid4()
    if ssettings._instance_id is None and settings._instance_exists:
        logger.warning(
            f"will manage storage location {ssettings.root_as_str} with instance {settings.instance.slug}"
        )
        ssettings._instance_id = settings.instance._id
    assert ssettings._instance_id is not None, "connect to an instance"
    fields = {
        "id": id.hex,
        "lnid": ssettings.uid,
        "created_by": created_by.hex,  # type: ignore
        "root": root,
        "region": ssettings.region,
        "type": ssettings.type,
        "instance_id": ssettings._instance_id.hex,
        "description": "",
        "is_default": is_default,
        "space_id": space_id.hex if space_id is not None else None,
    }
    # TODO: add error message for violated unique constraint
    # on root & description
    client.table("storage").upsert(fields).execute()
    ssettings._uuid_ = id
    return "hub-record-created"


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
            root_string: str = storage_record["root"]  # type: ignore
            account_for_sqlite_file = (
                instance_with_storage["db_scheme"] is None
                and instance_with_storage["storage"]["root"] == root_string
            )
            # gate storage and instance deletion on empty storage location for
            # normally auth.get_session() doesn't have access_token
            # so this block is useless i think (Sergei)
            # the token is received from user settings inside create_path
            # might be needed in the hub though
            if client.auth.get_session() is not None:
                access_token = client.auth.get_session().access_token
            else:
                access_token = None
            root_path = create_path(root_string, access_token)
            check_storage_is_empty(
                root_path, account_for_sqlite_file=account_for_sqlite_file
            )
    for storage_record in storage_records:
        _delete_storage_record(storage_record, client)  # type: ignore
    _delete_instance_record(UUID(instance_with_storage["id"]), client)
    return None


def delete_instance_record(instance_id: UUID, access_token: str | None = None) -> None:
    return call_with_fallback_auth(
        _delete_instance_record, instance_id=instance_id, access_token=access_token
    )


def init_instance_hub(
    isettings: InstanceSettings,
    account_id: UUID | None = None,
    access_token: str | None = None,
) -> None:
    return call_with_fallback_auth(
        _init_instance_hub,
        isettings=isettings,
        account_id=account_id,
        access_token=access_token,
    )


def _init_instance_hub(
    client: Client, isettings: InstanceSettings, account_id: UUID | None = None
) -> None:
    from ._settings import settings

    account_id = settings.user._uuid if account_id is None else account_id

    try:
        lamindb_version = metadata.version("lamindb")
    except metadata.PackageNotFoundError:
        lamindb_version = None
    fields = {
        "id": isettings._id.hex,
        "account_id": account_id.hex,  # type: ignore
        "name": isettings.name,
        "lnid": isettings.uid,
        "schema_str": isettings._schema_str,
        "lamindb_version": lamindb_version,
        "public": False,
        "created_by_id": account_id.hex,  # type: ignore
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
    slug = isettings.slug
    # I'd like the following to be an upsert, but this seems to violate RLS
    # Similarly, if we don't specify `returning="minimal"`, we'll violate RLS
    # we could make this idempotent by catching an error, but this seems dangerous
    # as then init_instance is no longer idempotent
    try:
        client.table("instance").insert(fields, returning="minimal").execute()
    except APIError:
        logger.warning(f"instance already existed at: https://lamin.ai/{slug}")
        return None
    if isettings.dialect != "sqlite" and isettings.is_remote:
        logger.important(f"go to: https://lamin.ai/{slug}")


def _connect_instance_hub(
    owner: str,  # account_handle
    name: str,  # instance_name
    client: Client,
) -> tuple[dict, dict] | str:
    response = client.functions.invoke(
        "get-instance-settings-v1",
        invoke_options={"body": {"owner": owner, "name": name}},
    )
    # check instance renames
    if response == b"{}":
        data = (
            client.table("instance_previous_name")
            .select(
                "instance!instance_previous_name_instance_id_17ac5d61_fk_instance_id(name, account!instance_account_id_28936e8f_fk_account_id(handle))"
            )
            .eq("instance.account.handle", owner)
            .eq("previous_name", name)
            .execute()
            .data
        )
        if len(data) != 0 and (instance_data := data[0]["instance"]) is not None:
            new_name = instance_data["name"]
            # the instance was renamed
            if new_name != name:
                logger.warning(
                    f"'{owner}/{name}' was renamed, please use '{owner}/{new_name}'"
                )
                response = client.functions.invoke(
                    "get-instance-settings-v1",
                    invoke_options={"body": {"owner": owner, "name": new_name}},
                )
    # no instance found, check why is that
    if response == b"{}":
        # try the via single requests, will take more time
        account = select_account_by_handle(owner, client)
        if account is None:
            return "account-not-exists"
        instance = select_instance_by_name(account["id"], name, client)
        if instance is None:
            return "instance-not-found"
        # get default storage
        storage = select_default_storage_by_instance_id(instance["id"], client)
        if storage is None:
            return "default-storage-does-not-exist-on-hub"
        logger.warning(
            "could not find instance via API, but found directly querying hub"
        )
    else:
        instance = json.loads(response)
        storage = instance.pop("storage")

    if instance["db_scheme"] is not None:
        db_user_name, db_user_password = None, None
        if "db_user_name" in instance and "db_user_password" in instance:
            db_user_name, db_user_password = (
                instance["db_user_name"],
                instance["db_user_password"],
            )
        else:
            db_user = select_db_user_by_instance(instance["id"], client)
            if db_user is not None:
                db_user_name, db_user_password = (
                    db_user["db_user_name"],
                    db_user["db_user_password"],
                )
        db_dsn = LaminDsn.build(
            scheme=instance["db_scheme"],
            user=db_user_name if db_user_name is not None else "none",
            password=db_user_password if db_user_password is not None else "none",
            host=instance["db_host"],
            port=instance["db_port"],
            database=instance["db_database"],
        )
        instance["db"] = db_dsn
    check_whether_migrations_in_sync(instance["lamindb_version"])
    return instance, storage  # type: ignore


def connect_instance_hub(
    *,
    owner: str,  # account_handle
    name: str,  # instance_name
    access_token: str | None = None,
) -> tuple[dict, dict] | str:
    from ._settings import settings

    if settings.user.handle != "anonymous" or access_token is not None:
        return call_with_fallback_auth(
            _connect_instance_hub, owner=owner, name=name, access_token=access_token
        )
    else:
        return call_with_fallback(_connect_instance_hub, owner=owner, name=name)


def access_aws(storage_root: str, access_token: str | None = None) -> dict[str, dict]:
    from ._settings import settings

    if settings.user.handle != "anonymous" or access_token is not None:
        storage_root_info = call_with_fallback_auth(
            _access_aws, storage_root=storage_root, access_token=access_token
        )
    else:
        storage_root_info = call_with_fallback(_access_aws, storage_root=storage_root)
    return storage_root_info


def _access_aws(*, storage_root: str, client: Client) -> dict[str, dict]:
    import lamindb_setup

    storage_root_info: dict[str, dict] = {"credentials": {}, "accessibility": {}}
    response = client.functions.invoke(
        "get-cloud-access-v1",
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
        accessibility["extra_parameters"] = loaded_accessibility.get("extraParameters")

    return storage_root_info


def access_db(
    instance: InstanceSettings | dict, access_token: str | None = None
) -> str:
    instance_id: UUID
    instance_slug: str
    instance_api_url: str | None
    if (
        "LAMIN_DB_TOKEN" in os.environ
        and (env_db_token := os.environ["LAMIN_DB_TOKEN"]) != ""
    ):
        return env_db_token

    if isinstance(instance, InstanceSettings):
        instance_id = instance._id
        instance_slug = instance.slug
        instance_api_url = instance._api_url
    else:
        instance_id = UUID(instance["id"])
        instance_slug = instance["owner"] + "/" + instance["name"]
        instance_api_url = instance["api_url"]

    if access_token is None:
        if settings.user.handle == "anonymous":
            raise RuntimeError(
                f"Can only get fine-grained access to {instance_slug} if authenticated."
            )
        else:
            access_token = settings.user.access_token
            renew_token = True
    else:
        renew_token = False
    # local is used in tests
    url = f"/access_v2/instances/{instance_id}/db_token"
    if os.environ.get("LAMIN_ENV", "prod") != "local":
        if instance_api_url is None:
            raise RuntimeError(
                f"Can only get fine-grained access to {instance_slug} if api_url is present."
            )
        url = instance_api_url + url

    response = request_with_auth(url, "get", access_token, renew_token)  # type: ignore
    status_code = response.status_code
    if not (200 <= status_code < 300):
        raise PermissionError(
            f"Fine-grained access to {instance_slug} failed: {status_code} {response.text}"
        )

    response_json = response.json()
    if "token" not in response_json:
        raise RuntimeError("The response of access_db does not contain a db token.")
    return response_json["token"]


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
        user = data[0]
        user_uuid = UUID(user["id"])
        user_id = user["lnid"]
        user_handle = user["handle"]
        user_name = user["name"]
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
) -> Exception | str | tuple[UUID, str, str, str, str]:
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


def _sign_in_hub_api_key(api_key: str, client: Client):
    response = client.functions.invoke(
        "get-jwt-v1",
        invoke_options={"body": {"api_key": api_key}},
    )
    access_token = json.loads(response)["accessToken"]
    # probably need more info here to avoid additional queries
    # like handle, uid etc
    account_id = jwt.decode(access_token, options={"verify_signature": False})["sub"]
    client.postgrest.auth(access_token)
    # normally public.account.id is equal to auth.user.id
    data = client.table("account").select("*").eq("id", account_id).execute().data
    if data:
        user = data[0]
        user_uuid = UUID(user["id"])
        user_id = user["lnid"]
        user_handle = user["handle"]
        user_name = user["name"]
    else:
        logger.error("Invalid API key.")
        return "invalid-api-key"
    return (user_uuid, user_id, user_handle, user_name, access_token)


def sign_in_hub_api_key(
    api_key: str,
) -> Exception | str | tuple[UUID, str, str, str, str]:
    try:
        result = call_with_fallback(_sign_in_hub_api_key, api_key=api_key)
    except Exception as exception:
        logger.error(exception)
        logger.error("Could not login. Probably your API key is wrong.")
        return exception
    return result


def _create_api_key(body: dict, client: Client) -> str:
    response = client.functions.invoke(
        "create-api-key-v1",
        invoke_options={"body": body},
    )
    api_key = json.loads(response)["apiKey"]
    return api_key


def create_api_key(body: dict) -> str:
    api_key = call_with_fallback_auth(_create_api_key, body=body)
    return api_key
