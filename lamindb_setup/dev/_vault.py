import hvac
import requests  # type: ignore
from lamindb_setup._settings import settings
from lamindb_setup.dev._hub_client import Environment

from ._hub_utils import LaminDsn

# Create vault client


def create_vault_client_anonymous():
    return hvac.Client(
        url="https://vault-cluster-2-public-vault-91dbdbcc.459cc10d.z1.hashicorp.cloud:8200",  # noqa
        namespace="admin",
    )


def create_vault_client():
    vault_access_token = requests.post(
        f"{Environment().hub_rest_server_url}/vault/token/from-jwt",
        headers={"authentication": f"Bearer {settings.user.access_token}"},
    ).json()
    vault_client = create_vault_client_anonymous()
    vault_client.auth_cubbyhole(vault_access_token)
    return vault_client


def create_vault_admin_client(instance_id):
    vault_access_token = requests.post(
        f"{Environment().hub_rest_server_url}/vault/token/from-jwt/admin/{instance_id}",
        headers={"authentication": f"Bearer {settings.user.access_token}"},
    ).json()
    vault_client = create_vault_client_anonymous()
    vault_client.auth_cubbyhole(vault_access_token)
    return vault_client


# Configure vault connection to Postgres roles and credentials


def create_or_update_connection_config_db(
    vault_admin_client,
    instance_id,
    db_host,
    db_port,
    db_name,
    vault_db_username,
    vault_db_password,
):
    vault_admin_client.secrets.database.configure(
        name=instance_id,
        plugin_name="postgresql-database-plugin",
        allowed_roles=[],
        connection_url=f"postgresql://{{{{username}}}}:{{{{password}}}}@{db_host}:{db_port}/{db_name}?sslmode=disable",  # noqa
        username=vault_db_username,
        password=vault_db_password,
        password_authentication="scram-sha-256",
    )


def connection_config_db_exists(instance_id):
    vault_client = create_vault_client()
    connection_config_path = f"database/config/{instance_id}"
    if vault_client.read(connection_config_path) is None:
        return False
    else:
        return True


# Configure db authorizations with a user specific role
# define in creation_statements


def create_or_update_role_and_policy_db(
    vault_admin_client, instance_id, account_id, creation_statements
):
    role_name = f"{instance_id}-{account_id}-db"

    # Create or update role
    vault_admin_client.secrets.database.create_role(
        name=role_name,
        db_name=str(instance_id),
        creation_statements=creation_statements,
        default_ttl="1h",
        max_ttl="24h",
    )

    # Create or update policy to access role
    policy_name = f"{role_name}-policy"
    policy = f"""
    path "database/creds/{role_name}" {{
      capabilities = ["read"]
    }}
    """
    vault_admin_client.sys.create_or_update_policy(name=policy_name, policy=policy)

    # Update allowed_roles of connection configuration
    connection_config_path = f"database/config/{instance_id}"
    current_config = vault_admin_client.read(connection_config_path)["data"]
    allowed_roles = set(current_config.get("allowed_roles", []))
    allowed_roles.add(role_name)
    vault_admin_client.write(
        connection_config_path,
        allowed_roles=list(allowed_roles),
    )


# Init instance vault


def init_instance_vault(
    instance_id,
    admin_account_id,
    db_host,
    db_port,
    db_name,
    vault_db_username,
    vault_db_password,
):
    vault_admin_client = create_vault_admin_client(instance_id)

    print(
        "Args",
        instance_id,
        admin_account_id,
        db_host,
        db_port,
        db_name,
        vault_db_username,
        vault_db_password,
    )

    create_or_update_connection_config_db(
        vault_admin_client=vault_admin_client,
        instance_id=instance_id,
        db_host=db_host,
        db_port=db_port,
        db_name=db_name,
        vault_db_username=vault_db_username,
        vault_db_password=vault_db_password,
    )

    admin_role_creation_statements = [
        "CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL"
        " '{{expiration}}';",
        'GRANT SELECT ON ALL TABLES IN SCHEMA public TO "{{name}}";',
    ]

    create_or_update_role_and_policy_db(
        vault_admin_client=vault_admin_client,
        instance_id=instance_id,
        account_id=admin_account_id,
        creation_statements=admin_role_creation_statements,
    )


# Generate credential for db


def generate_db_postgres_credentials(vault_client, role_name):
    credentials = vault_client.secrets.database.generate_credentials(
        name=role_name, mount_point="database"
    )

    username = credentials["data"]["username"]
    password = credentials["data"]["password"]

    return username, password


def get_db_postgres_connection_string(scheme, host, port, name, role):
    vault_client = create_vault_client()

    # TODO: Fetch a user role id for postgres to replace "read"
    # something like f"{account_id}-{instance_id}-db-credentials"
    username, password = generate_db_postgres_credentials(vault_client, role)

    return LaminDsn.build(
        scheme=scheme,
        user=username,
        password=password,
        host=host,
        port=str(port),
        database=name,
    )
