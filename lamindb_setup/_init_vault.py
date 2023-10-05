from typing import Optional

from pydantic import PostgresDsn

from ._settings import settings
from .dev._hub_utils import LaminDsnModel


def init_vault(
    *,
    db: Optional[PostgresDsn] = None,
) -> Optional[str]:
    """Initialize vault for current LaminDB instance.

    Args:
        db: {}
    """
    _init_vault(db, settings.instance.id)
    return None


def _init_vault(db, instance_id):
    from lamin_vault.client._create_vault_client import create_vault_admin_client
    from lamin_vault.client._init_instance_vault import init_instance_vault

    db_dsn = LaminDsnModel(db=db)
    vault_admin_client = create_vault_admin_client(
        access_token=settings.user.access_token, instance_id=instance_id
    )
    init_instance_vault(
        vault_admin_client=vault_admin_client,
        instance_id=instance_id,
        admin_account_id=settings.user.uuid,
        db_host=db_dsn.db.host,
        db_port=db_dsn.db.port,
        db_name=db_dsn.db.database,
        vault_db_username=db_dsn.db.user,
        vault_db_password=db_dsn.db.password,
    )

    _set_public_read_db_role(vault_admin_client, instance_id)


def _set_public_read_db_role(vault_admin_client, instance_id):
    from lamin_vault.client.postgres._set_db_role import set_public_read_db_role

    set_public_read_db_role(vault_admin_client, instance_id)
