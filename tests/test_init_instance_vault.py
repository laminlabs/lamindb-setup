from lamindb_setup.dev._vault import (
    init_instance_vault,
    connection_config_db_exists,
    create_vault_client,
    create_vault_admin_client,
)
from lamindb_setup._settings import settings
from lamindb_setup.dev._hub_utils import LaminDsnModel


def test_init_instance_vault():
    vault_client_test = create_vault_client()
    vault_admin_client_test = create_vault_admin_client(settings.instance.id)

    db_dsn = LaminDsnModel(db=settings.instance.db)
    instance_id = settings.instance.id
    admin_account_id = settings.user.uuid

    try:
        init_instance_vault(
            instance_id=instance_id,
            admin_account_id=admin_account_id,
            db_host=db_dsn.db.host,
            db_port=db_dsn.db.port,
            db_name=db_dsn.db.database,
            vault_db_username=db_dsn.db.user,
            vault_db_password=db_dsn.db.password,
        )

        # Verify connection configuration exists
        assert connection_config_db_exists(
            instance_id
        ), "Connection configuration should exist in vault"

        # Verify role and policy exist
        role_name = f"{instance_id}-{admin_account_id}-db"
        policy_name = f"{role_name}-policy"

        assert (
            vault_client_test.secrets.database.read_role(name=role_name) is not None
        ), "Role should exist in vault"
        assert (
            vault_client_test.sys.read_policy(name=policy_name) is not None
        ), "Policy should exist in vault"

    finally:
        # Delete the created resources
        role_name = f"{instance_id}-{admin_account_id}-db"
        policy_name = f"{role_name}-policy"
        connection_config_path = f"database/config/{instance_id}"

        vault_admin_client_test.secrets.database.delete_role(name=role_name)
        vault_admin_client_test.sys.delete_policy(name=policy_name)
        vault_admin_client_test.delete(connection_config_path)
