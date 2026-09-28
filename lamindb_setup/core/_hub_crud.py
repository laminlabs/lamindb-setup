from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from lamin_utils import logger
from supabase.client import Client  # noqa


def select_instance_by_owner_name(
    owner: str,
    name: str,
    client: Client,
) -> dict | None:
    # this won't find an instance without the default storage
    data = (
        client.table("instance")
        .select(
            "*, account!inner!instance_account_id_28936e8f_fk_account_id(*),"
            " storage!inner!storage_instance_id_359fca71_fk_instance_id(*)"
        )
        .eq("name", name)
        .eq("account.handle", owner)
        .eq("storage.is_default", True)
        .execute()
        .data
    )
    if len(data) == 0:
        return None
    result = data[0]
    # this is a list
    # assume only one default storage
    result["storage"] = result["storage"][0]
    return result


# --------------- ACCOUNT ----------------------
def select_account_by_handle(
    handle: str,
    client: Client,
):
    data = client.table("account").select("*").eq("handle", handle).execute().data
    if len(data) == 0:
        return None
    return data[0]


def select_account_handle_name_by_lnid(lnid: str, client: Client):
    data = (
        client.table("account").select("handle, name").eq("lnid", lnid).execute().data
    )
    if not data:
        return None
    return data[0]


# --------------- INSTANCE ----------------------
def select_instance_by_name(
    account_id: str,
    name: str,
    client: Client,
):
    data = (
        client.table("instance")
        .select("*")
        .eq("account_id", account_id)
        .eq("name", name)
        .execute()
        .data
    )
    if len(data) != 0:
        return data[0]

    data = (
        client.table("instance_previous_name")
        .select(
            "instance!instance_previous_name_instance_id_17ac5d61_fk_instance_id(*)"
        )
        .eq("instance.account_id", account_id)
        .eq("previous_name", name)
        .execute()
        .data
    )
    if len(data) != 0:
        return data[0]["instance"]

    return None


def select_instance_by_id(
    instance_id: str,
    client: Client,
):
    response = client.table("instance").select("*").eq("id", instance_id).execute()
    if len(response.data) == 0:
        return None
    return response.data[0]


def select_instance_by_id_with_storage(
    instance_id: str,
    client: Client,
):
    # this won't find an instance without the default storage
    data = (
        client.table("instance")
        .select("*, storage!inner!storage_instance_id_359fca71_fk_instance_id(*)")
        .eq("id", instance_id)
        .eq("storage.is_default", True)
        .execute()
        .data
    )
    if len(data) == 0:
        return None
    result = data[0]
    # this is a list
    # assume only one default storage
    result["storage"] = result["storage"][0]
    return result


def update_instance(instance_id: str, instance_fields: dict, client: Client):
    response = (
        client.table("instance").update(instance_fields).eq("id", instance_id).execute()
    )
    if len(response.data) == 0:
        raise PermissionError(
            f"Update of instance with {instance_id} was not successful. Probably, you"
            " don't have sufficient permissions."
        )
    return response.data[0]


# --------------- COLLABORATOR ----------------------


def select_collaborator(
    instance_id: str,
    account_id: str,
    client: Client,
):
    data = (
        client.table("access_instance")
        .select("*")
        .eq("instance_id", instance_id)
        .eq("account_id", account_id)
        .execute()
        .data
    )
    if len(data) == 0:
        return None
    return data[0]


# --------------- STORAGE ----------------------


def select_default_storage_by_instance_id(
    instance_id: str, client: Client
) -> dict | None:
    data = (
        client.table("storage")
        .select("*")
        .eq("instance_id", instance_id)
        .eq("is_default", True)
        .execute()
        .data
    )
    if len(data) == 0:
        return None
    return data[0]


# --------------- DBUser ----------------------


def insert_db_user(
    *,
    name: str,
    db_user_name: str,
    db_user_password: str,
    instance_id: UUID,
    fine_grained_access: bool,
    client: Client,
) -> None:
    fields = {"instance_id": instance_id.hex}
    if fine_grained_access:
        table = "access_db_user"
        fields.update(
            {
                "name": db_user_name,
                "password": db_user_password,
                "type": name,
            }
        )
    else:
        table = "db_user"
        fields.update(
            {
                "id": uuid4().hex,
                "name": name,
                "db_user_name": db_user_name,
                "db_user_password": db_user_password,
            }
        )

    data = client.table(table).insert(fields).execute().data
    return data[0]


def select_db_user_by_instance(
    instance_id: str, fine_grained_access: bool, client: Client
):
    """Get db_user for which client has permission."""
    # jwt and public come from access_db_user; write and read come from db_user.
    # Prefer jwt over public, and the root (write) user over read.
    db_types = ["jwt", "public"] if fine_grained_access else ["write", "read"]
    # get=True puts arguments in the query string. PostgREST casts that string to
    # text[], so a JSON list is read as one element ("read") and rejected.
    db_type_param = "{" + ",".join(db_types) + "}"
    rows = (
        client.rpc(
            "get_instance_db_user",
            {"_instance_id": instance_id, "_type": db_type_param},
            get=True,
        )
        .execute()
        .data
    )
    by_type = {row["type"]: row for row in rows}
    for db_type in db_types:
        row = by_type.get(db_type)
        if row is not None:
            return row
    return None


def _delete_instance_record(instance_id: UUID | str, client: Client) -> None:
    if not isinstance(instance_id, UUID):
        instance_id = UUID(instance_id)
    response = client.table("instance").delete().eq("id", instance_id.hex).execute()
    if response.data:
        logger.important(f"deleted instance record on hub {instance_id.hex}")
    else:
        raise PermissionError(
            f"Deleting of instance with {instance_id.hex} was not successful. Probably, you"
            " don't have sufficient permissions."
        )
