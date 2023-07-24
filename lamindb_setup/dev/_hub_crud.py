from postgrest.exceptions import APIError
from supabase.client import Client


# --------------- ACCOUNT ----------------------
def sb_select_account_by_handle(
    handle: str,
    supabase_client: Client,
):
    data = (
        supabase_client.table("account").select("*").eq("handle", handle).execute().data
    )
    if len(data) == 0:
        return None
    return data[0]


# --------------- INSTANCE ----------------------
def sb_insert_instance(instance_fields: dict, supabase_client: Client):
    try:
        (
            supabase_client.table("instance")
            .insert(instance_fields, returning="minimal")
            .execute()
            .data
        )
    except Exception as e:
        if str(e) == str("Expecting value: line 1 column 1 (char 0)"):
            pass
        else:
            raise e
    return sb_select_instance_by_name(
        instance_fields["account_id"], instance_fields["name"], supabase_client
    )


def sb_select_instance_by_name(
    account_id: str,
    name: str,
    supabase_client: Client,
):
    data = (
        supabase_client.table("instance")
        .select("*")
        .eq("account_id", account_id)
        .eq("name", name)
        .execute()
        .data
    )
    if len(data) == 0:
        return None
    return data[0]


def sb_update_instance(
    instance_id: str, instance_fields: dict, supabase_client: Client
):
    data = (
        supabase_client.table("instance")
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
    supabase_client: Client,
):
    data = supabase_client.table("instance").delete().eq("id", id).execute().data
    if len(data) == 0:
        return None
    return data[0]


# --------------- COLLABORATOR ----------------------
def sb_insert_collaborator(account_instance_fields: dict, supabase_client: Client):
    try:
        (
            supabase_client.table("account_instance")
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
    except Exception as e:
        if str(e) == str("Expecting value: line 1 column 1 (char 0)"):
            pass
        else:
            raise e
    return sb_select_collaborator(
        instance_id=account_instance_fields["instance_id"],
        account_id=account_instance_fields["account_id"],
        supabase_client=supabase_client,
    )


def sb_select_collaborator(
    instance_id: str,
    account_id: str,
    supabase_client: Client,
):
    data = (
        supabase_client.table("account_instance")
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
def sb_insert_storage(storage_fields: dict, supabase_client: Client):
    data = supabase_client.table("storage").insert(storage_fields).execute().data
    if len(data) == 0:
        return None
    return data[0]


def sb_select_storage(id: str, supabase_client: Client):
    data = supabase_client.table("storage").select("*").eq("id", id).execute().data
    if len(data) == 0:
        return None
    return data[0]


def sb_select_storage_by_root(root: str, supabase_client: Client):
    data = supabase_client.table("storage").select("*").eq("root", root).execute().data
    if len(data) == 0:
        return None
    return data[0]


# --------------- DBUser ----------------------
def sb_insert_db_user(instance_fields: dict, supabase_client: Client):
    try:
        data = supabase_client.table("db_user").insert(instance_fields).execute().data
    except Exception as e:
        if str(e) == str("Expecting value: line 1 column 1 (char 0)"):
            pass
        else:
            raise e
    return data[0]


def sb_select_db_user_by_instance(instance_id: str, supabase_client: Client):
    """Get the DBAccount directly associated with Instance.

    By contrast this is not the DBAccount that is linked through the
    UserInstance table.
    """
    data = (
        supabase_client.table("db_user")
        .select("*")
        .eq("instance_id", instance_id)
        .execute()
        .data
    )
    if len(data) == 0:
        return None
    return data[0]
