import secrets
import string

import pytest
from lamin_vault.client._create_vault_client import (
    create_vault_admin_client,
)
from lamin_vault.client.postgres._connection_config_db_exists import (
    connection_config_db_exists,
)
from lamin_vault.client.postgres._role_and_policy_exist import role_and_policy_exist
from lamin_vault.utils._lamin_dsn import LaminDsn
from lamindb_setup._init_instance import init
from lamindb_setup._init_vault import init_vault

# from lamindb_setup._load_instance import load
from lamindb_setup._settings import settings
from lamindb_setup.dev._hub_client import connect_hub_with_auth
from lamindb_setup.dev._hub_crud import sb_delete_instance
from psycopg2 import connect, sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

hub = connect_hub_with_auth()


def base62(n_char: int) -> str:
    """Like nanoid without hyphen and underscore."""
    alphabet = string.digits + string.ascii_letters.swapcase()
    id = "".join(secrets.choice(alphabet) for i in range(n_char))
    return id


@pytest.fixture(scope="session")
def run_id():
    return base62(6)


@pytest.fixture(scope="session")
def db_name(run_id):
    return f"instance-ci-{run_id}"


# This fixture enable to create a test db in GCP using the following server:
# https://console.cloud.google.com/sql/instances/lamindb-ci/overview?hl=fr&project=data-totality-352303
@pytest.fixture(scope="session")
def db_url(db_name):
    connection = None
    user = "postgres"
    password = "JC^0ozMGprQdHrSv"
    host = "34.123.208.102"
    port = "5432"

    try:
        connection = connect(
            user=user,
            password=password,
            host=host,
            dbname="postgres",
        )

        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()

        # Create a new temporary database
        cursor.execute(f'DROP DATABASE IF EXISTS "{db_name}";')
        cursor.execute(f'CREATE DATABASE "{db_name}";')

        # Yield the URL of the temporary database
        yield LaminDsn.build(
            scheme="postgresql",
            user=user,
            password=password,
            host=host,
            port=port,
            database=db_name,
        )

    finally:
        if connection:
            # Terminate open sessions
            cursor.execute(
                sql.SQL(
                    "SELECT pg_terminate_backend(pg_stat_activity.pid) "
                    "FROM pg_stat_activity "
                    f"WHERE datname = '{db_name}' "
                    "AND pid <> pg_backend_pid();"
                )
            )

            # Drop the temporary database after the test function has completed
            cursor.execute(f'DROP DATABASE IF EXISTS "{db_name}";')
            connection.close()


def test_init_instance_with_vault(db_url, db_name):
    instance_name = db_name + "_1"
    instance_id = None

    try:
        init(
            name=instance_name,
            storage="s3://lamindata",
            db=db_url,
            _vault=True,
            _test=True,
        )
        instance_id = settings.instance.id
        admin_account_id = settings.user.uuid
        vault_admin_client_test = create_vault_admin_client(
            access_token=settings.user.access_token, instance_id=instance_id
        )

        # Verify connection configuration exists
        assert connection_config_db_exists(
            vault_client=vault_admin_client_test, instance_id=instance_id
        ), "Connection configuration should exist in vault."

        # Verify connection admin role and policy exist
        assert role_and_policy_exist(
            vault_client=vault_admin_client_test,
            instance_id=instance_id,
            account_id=admin_account_id,
        ), "Admin role and policy should exist in vault."

    finally:
        if instance_id is not None:
            # Delete the created resources
            role_name = f"{instance_id}-{admin_account_id}-db"
            policy_name = f"{role_name}-policy"
            connection_config_path = f"database/config/{instance_id}"

            vault_admin_client_test.secrets.database.delete_role(name=role_name)
            vault_admin_client_test.sys.delete_policy(name=policy_name)
            vault_admin_client_test.delete(connection_config_path)
            sb_delete_instance(instance_id, hub)


def test_init_vault(db_url, db_name):
    instance_name = db_name + "_2"
    instance_id = None

    try:
        init(
            name=instance_name,
            storage="s3://lamindata",
            db=db_url,
            _vault=False,
            _test=True,
        )
        instance_id = settings.instance.id
        admin_account_id = settings.user.uuid
        vault_admin_client_test = create_vault_admin_client(
            access_token=settings.user.access_token, instance_id=instance_id
        )

        # Verify connection configuration does not exist
        assert not connection_config_db_exists(
            vault_client=vault_admin_client_test, instance_id=instance_id
        ), "Connection configuration should not exist in vault."

        # Verify connection admin role and policy do not exist
        assert not role_and_policy_exist(
            vault_client=vault_admin_client_test,
            instance_id=instance_id,
            account_id=admin_account_id,
        ), "Admin role and policy should not exist in vault."

        init_vault(db=settings.instance.db)

        # Verify connection configuration exists
        assert connection_config_db_exists(
            vault_client=vault_admin_client_test, instance_id=instance_id
        ), "Connection configuration should exist in vault."

        # Verify connection admin role and policy exist
        assert role_and_policy_exist(
            vault_client=vault_admin_client_test,
            instance_id=instance_id,
            account_id=admin_account_id,
        ), "Admin role and policy should exist in vault."

    finally:
        if instance_id is not None:
            # Delete the created resources
            role_name = f"{instance_id}-{admin_account_id}-db"
            policy_name = f"{role_name}-policy"
            connection_config_path = f"database/config/{instance_id}"

            vault_admin_client_test.secrets.database.delete_role(name=role_name)
            vault_admin_client_test.sys.delete_policy(name=policy_name)
            vault_admin_client_test.delete(connection_config_path)
            sb_delete_instance(instance_id, hub)


# def test_load_with_vault(db_url, db_name):
#     instance_name = db_name + "_3"
#     instance_id = None

#     try:
#         init(
#             name=instance_name,
#             storage="s3://lamindata",
#             db=db_url,
#             _vault=True,
#             _test=True,
#         )
#         instance_id = settings.instance.id
#         admin_account_id = settings.user.uuid
#         vault_admin_client_test = create_vault_admin_client(
#             access_token=settings.user.access_token, instance_id=instance_id
#         )

#         load(identifier=instance_name, _vault=True, _test=True)

#         # Verify generated db credentails exist
#         assert (
#             settings.instance._db_from_vault is not None
#         ), "Generated db credentials should exist."

#         # Verify generated db credentails are used
#         assert (
#             settings.instance._db_from_vault == settings.instance.db
#         ), "Generated db credentials should be used."

#         # Verify admin connection string is still stored in settings
#         assert (
#             settings.instance._db == db_url
#         ), "Admin connection string should be stored in settings."

#     finally:
#         if instance_id is not None:
#             # Delete the created resources
#             role_name = f"{instance_id}-{admin_account_id}-db"
#             policy_name = f"{role_name}-policy"
#             connection_config_path = f"database/config/{instance_id}"

#             vault_admin_client_test.secrets.database.delete_role(name=role_name)
#             vault_admin_client_test.sys.delete_policy(name=policy_name)
#             vault_admin_client_test.delete(connection_config_path)
#             sb_delete_instance(instance_id, hub)
