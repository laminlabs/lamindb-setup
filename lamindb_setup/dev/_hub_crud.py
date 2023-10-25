from postgrest.exceptions import APIError
from supabase.client import Client
from typing import Optional, Dict


def select_instance_by_owner_name(
    owner: str,
    name: str,
    client: Client,
) -> Optional[Dict]:
    data = (
        client.table("instance")
        .select("*, account!inner!fk_instance_account_id_account(*)")
        .eq("account.handle", owner)
        .eq("name", name)
        .execute()
        .data
    )
    if len(data) == 0:
        return None
    return data[0]


# --------------- ACCOUNT ----------------------
def sb_select_account_by_handle(
    handle: str,
    client: Client,
):
    data = client.table("account").select("*").eq("handle", handle).execute().data
    if len(data) == 0:
        return None
    return data[0]


def sb_select_account_handle_name_by_lnid(lnid: str, client: Client):
    data = (
        client.table("account").select("handle, name").eq("lnid", lnid).execute().data
    )
    if not data:
        return None
    return data[0]


# --------------- INSTANCE ----------------------
def sb_insert_instance(instance_fields: dict, client: Client):
    (
        client.table("instance")
        .insert(instance_fields, returning="minimal")
        .execute()
        .data
    )


def sb_select_instance_by_name(
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


def sb_update_instance(instance_id: str, instance_fields: dict, client: Client):
    data = (
        client.table("instance")
        .update(instance_fields)
        .eq("id", instance_id)
        .execute()
        .data
    )
    if len(data) == 0:
        return None
    return data[0]


def sb_delete_instance(
    id: str,
    client: Client,
):
    data = client.table("instance").delete().eq("id", id).execute().data
    if len(data) == 0:
        return None
    return data[0]


# --------------- COLLABORATOR ----------------------
def sb_insert_collaborator(account_instance_fields: dict, client: Client):
    try:
        (
            client.table("account_instance")
            .insert(account_instance_fields, returning="minimal")
            .execute()
            .data
        )
    except APIError as api_error:
        pk_violation_msg = (
            'duplicate key value violates unique constraint "pk_account_instance"'
        )
        if api_error.message == pk_violation_msg:
            return "collaborator-exists-already"
        raise api_error


def sb_select_collaborator(
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
def sb_insert_storage(storage_fields: dict, client: Client):
    data = client.table("storage").insert(storage_fields).execute().data
    if len(data) == 0:
        return None
    return data[0]


def sb_select_storage(id: str, client: Client):
    data = client.table("storage").select("*").eq("id", id).execute().data
    if len(data) == 0:
        return None
    return data[0]


def sb_select_storage_by_root(root: str, client: Client):
    data = client.table("storage").select("*").eq("root", root).execute().data
    if len(data) == 0:
        return None
    return data[0]


# --------------- DBUser ----------------------
def sb_insert_db_user(db_user_fields: dict, client: Client):
    try:
        data = client.table("db_user").insert(db_user_fields).execute().data
    except Exception as e:
        if str(e) == str("Expecting value: line 1 column 1 (char 0)"):
            pass
        else:
            raise e
    return data[0]


def sb_update_db_user(db_user_id: str, db_user_fields: dict, client: Client):
    data = (
        client.table("db_user")
        .update(db_user_fields)
        .eq("id", db_user_id)
        .execute()
        .data
    )
    if len(data) == 0:
        return None
    return data[0]


def sb_select_db_user_by_instance(instance_id: str, client: Client):
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
