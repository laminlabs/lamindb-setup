from lamindb_setup.dev._hub_client import Environment
import hvac
import requests  # type: ignore
from lamindb_setup._settings import settings
from hvac.exceptions import InvalidPath

vault_server_url = (
    "https://vault-cluster-public-vault-4bccf3a3.599cc808.z1.hashicorp.cloud:8200"
)
hub_rest_server_url = Environment().hub_rest_server_url


def create_authenticated_client(access_token, instance_id, admin=False):
    vault_client = hvac.Client(url=vault_server_url, namespace="admin")
    if admin:
        url = f"{hub_rest_server_url}/vault/token/from-jwt/admin/{instance_id}"
    else:
        url = f"{hub_rest_server_url}/vault/token/from-jwt/{instance_id}"
    vault_access_token = requests.post(
        url, headers={"authentication": f"Bearer {access_token}"}
    ).json()
    vault_client.auth_cubbyhole(vault_access_token)
    return vault_client


class PostgresVaultManager:
    def __init__(self, instance_id: str) -> None:
        self.instance_id = instance_id
        self.config_path = f"database/config/{instance_id}"
        self._client = None
        self._admin_client = None

    @property
    def client(self):
        if self._client is None:
            self._client = create_authenticated_client(
                settings.user.access_token, self.instance_id
            )
        return self._client

    @property
    def admin_client(self):
        if self._admin_client is None:
            self._admin_client = create_authenticated_client(
                settings.user.access_token, self.instance_id, True
            )
        return self._admin_client

    def exists(self):
        return not self.client.read(self.config_path) is None

    def create(
        self, db_host, db_port, db_name, db_admin_user_name, db_admin_user_password
    ):
        self.admin_client.secrets.database.configure(
            name=self.instance_id,
            plugin_name="postgresql-database-plugin",
            allowed_roles=[],
            connection_url=f"postgresql://{{{{username}}}}:{{{{password}}}}@{db_host}:{db_port}/{db_name}?sslmode=allow",  # noqa
            username=db_admin_user_name,
            password=db_admin_user_password,
            password_authentication="scram-sha-256",
        )

    def delete(self):
        # Delete connection configuration
        self.admin_client.secrets.database.delete_connection(name=self.instance_id)

        # Get all roles
        roles = self.admin_client.secrets.database.list_roles()

        # Delete roles and policies associated with the instance
        for role_name in roles:
            if role_name.startswith(str(self.instance_id)):
                policy_name = f"{role_name}-policy"
                delete_db_role(
                    self.admin_client, self.config_path, role_name, policy_name
                )

    def create_public_role(self):
        public_role_name = f"{self.instance_id}-public-read-db"
        public_role_policy_name = f"{public_role_name}-policy"
        self.admin_client.secrets.database.create_role(
            name=public_role_name,
            db_name=str(self.instance_id),
            creation_statements=public_read_role_creation_statements,
            default_ttl="0",
            max_ttl="0",
        )
        create_db_policy(self.admin_client, public_role_name, public_role_policy_name)
        update_allowed_roles(self.admin_client, self.config_path, public_role_name)


class PostgresRoleManager:
    def __init__(self, instance_id: str, account_id: str) -> None:
        self.instance_id = instance_id
        self.account_id = account_id
        self.config_path = f"database/config/{instance_id}"
        self._client = None
        self._admin_client = None
        self.role_name = f"{instance_id}-{account_id}-db"
        self.policy_name = f"{self.role_name}-policy"

    @property
    def client(self):
        if self._client is None:
            self._client = create_authenticated_client(
                settings.user.access_token, self.instance_id
            )
        return self._client

    @property
    def admin_client(self):
        if self._admin_client is None:
            self._admin_client = create_authenticated_client(
                settings.user.access_token, self.instance_id, True
            )
        return self._admin_client

    def exists(self):
        try:
            self.client.secrets.database.read_role(name=self.role_name)
            self.client.sys.read_policy(name=self.policy_name)
            return True
        except InvalidPath:
            return False

    def create(self, role_creation_statements, role_name):
        if role_creation_statements is not None:
            self.__create(role_creation_statements)
        else:
            if role_name == "admin":
                self.__create(admin_role_creation_statements)
            elif role_name == "write":
                self.__create(write_role_creation_statements)
            elif role_name == "read":
                self.__create(read_role_creation_statements)

    def delete(self):
        delete_db_role(
            self.admin_client, self.config_path, self.role_name, self.policy_name
        )

    def __create(self, creation_statements):
        self.admin_client.secrets.database.create_role(
            name=self.role_name,
            db_name=str(self.instance_id),
            creation_statements=creation_statements,
            default_ttl="1h",
            max_ttl="24h",
        )
        create_db_policy(self.admin_client, self.role_name, self.policy_name)
        update_allowed_roles(self.admin_client, self.config_path, self.role_name)


# Helpers


def create_db_policy(vault_admin_client, role_name, policy_name):
    policy = f"""
    path "database/creds/{role_name}" {{
      capabilities = ["read"]
    }}
    """
    vault_admin_client.sys.create_or_update_policy(name=policy_name, policy=policy)


def update_allowed_roles(vault_admin_client, config_path, role_name):
    current_config = vault_admin_client.read(config_path)["data"]
    allowed_roles = set(current_config.get("allowed_roles", []))
    allowed_roles.add(role_name)
    vault_admin_client.write(
        config_path,
        allowed_roles=list(allowed_roles),
    )


def delete_db_role(vault_admin_client, connection_config_path, role_name, policy_name):
    # Delete role
    vault_admin_client.secrets.database.delete_role(name=role_name)

    # Delete policy
    vault_admin_client.sys.delete_policy(name=policy_name)

    # Update allowed_roles of connection configuration
    current_config = vault_admin_client.read(connection_config_path)["data"]
    allowed_roles = set(current_config.get("allowed_roles", []))
    allowed_roles.discard(role_name)
    vault_admin_client.write(
        connection_config_path,
        allowed_roles=list(allowed_roles),
    )


# Create statements


admin_role_creation_statements = [
    "CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL"
    " '{{expiration}}';",
    'GRANT CREATE ON SCHEMA public TO "{{name}}";',
    "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO"
    ' "{{name}}";',
    "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE"
    ' ON TABLES TO "{{name}}";',
    'GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO "{{name}}";',
    "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT, UPDATE ON"
    ' SEQUENCES TO "{{name}}";',
]

write_role_creation_statements = [
    "CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL"
    " '{{expiration}}';",
    'GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO "{{name}}";',
    "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE ON"
    ' TABLES TO "{{name}}";',
    'GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO "{{name}}";',
    "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT, UPDATE ON"
    ' SEQUENCES TO "{{name}}";',
]

read_role_creation_statements = [
    "CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL"
    " '{{expiration}}';",
    'GRANT SELECT ON ALL TABLES IN SCHEMA public TO "{{name}}";',
    'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO "{{name}}";',
]

public_read_role_creation_statements = [
    "CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}';",
    'GRANT SELECT ON ALL TABLES IN SCHEMA public TO "{{name}}";',
    'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO "{{name}}";',
]
