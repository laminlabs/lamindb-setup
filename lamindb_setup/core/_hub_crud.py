from supabase.client import Client
from typing import Optional, Dict
from lamin_utils import logger
from uuid import UUID


def select_instance_by_owner_name(
    owner: str,
    name: str,
    client: Client,
) -> Optional[Dict]:
    try:
        data = (
            client.table("instance")
            .select(
                "*, account!inner!instance_account_id_28936e8f_fk_account_id(*),"
                " storage(*)"
            )
            .eq("account.handle", owner)
            .eq("name", name)
            .execute()
            .data
        )
    except Exception:
        return None
    if len(data) == 0:
        return None
    return data[0]


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
    if len(data) == 0:
        return None
    return data[0]


def select_instance_by_id(
    instance_id: str,
    client: Client,
):
    response = client.table("instance").select("*").eq("id", instance_id).execute()
    if len(response.data) == 0:
        return None
    return response.data[0]


def update_instance(instance_id: str, instance_fields: dict, client: Client):
    response = (
        client.table("instance").update(instance_fields).eq("id", instance_id).execute()
    )
    if len(response.data) == 0:
        raise RuntimeError(
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
        client.table("account_instance")
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


def select_storage(id: str, client: Client):
    data = client.table("storage").select("*").eq("id", id).execute().data
    if len(data) == 0:
        return None
    return data[0]


# --------------- DBUser ----------------------


def select_db_user_by_instance(instance_id: str, client: Client):
    """Get the DBAccount directly associated with Instance.

    By contrast this is not the DBAccount that is linked through the
    UserInstance table.
    """
    data = (
        client.table("db_user")
        .select("*")
        .eq("instance_id", instance_id)
        .execute()
        .data
    )
    if len(data) == 0:
        return None
    return data[0]


def _delete_instance_record(instance_id: UUID, client: Client) -> None:
    if not isinstance(instance_id, UUID):
        instance_id = UUID(instance_id)
    logger.important(f"deleting instance {instance_id.hex}")
    client.table("instance").delete().eq("id", instance_id.hex).execute()


sb_delete_instance = _delete_instance_record
