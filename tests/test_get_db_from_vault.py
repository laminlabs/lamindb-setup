import psycopg2
from lamindb_setup.dev._vault import (
    init_instance_vault,
    create_vault_admin_client,
    get_db_from_vault,
)
from lamindb_setup._settings import settings
from lamindb_setup.dev._hub_utils import LaminDsnModel


def test_get_db_from_vault():
    vault_admin_client_test = create_vault_admin_client(settings.instance.id)

    db_dsn_admin = LaminDsnModel(db=settings.instance.db)
    instance_id = settings.instance.id
    admin_account_id = settings.user.uuid

    role_name = f"{instance_id}-{admin_account_id}-db"

    init_instance_vault(
        instance_id=instance_id,
        admin_account_id=admin_account_id,
        db_host=db_dsn_admin.db.host,
        db_port=db_dsn_admin.db.port,
        db_name=db_dsn_admin.db.database,
        vault_db_username=db_dsn_admin.db.user,
        vault_db_password=db_dsn_admin.db.password,
    )

    db_dsn = get_db_from_vault(
        scheme="postgresql",
        host=db_dsn_admin.db.host,
        port=db_dsn_admin.db.port,
        name=db_dsn_admin.db.database,
        role=role_name,
    )

    try:
        connection = psycopg2.connect(dsn=db_dsn)
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert (
            result[0] == 1
        ), "Should be able to execute a query with the obtained credentials"
    finally:
        # Close the connection and cursor
        cursor.close()
        connection.close()
        # Delete the created resources
        role_name = f"{instance_id}-{admin_account_id}-db"
        policy_name = f"{role_name}-policy"
        connection_config_path = f"database/config/{instance_id}"

        vault_admin_client_test.secrets.database.delete_role(name=role_name)
        vault_admin_client_test.sys.delete_policy(name=policy_name)
        vault_admin_client_test.delete(connection_config_path)
