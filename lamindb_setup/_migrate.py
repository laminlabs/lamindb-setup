from lamin_logger import logger

from ._settings import settings
from .dev._django import setup_django


class migrate:
    """Manage migrations.

    Examples:

    >>> import lamindb as ln
    >>> ln.setup.migrate.create()
    >>> ln.setup.migrate.deploy()
    >>> ln.setup.migrate.check()

    """

    @classmethod
    def create(cls) -> None:
        """Create a migration."""
        setup_django(settings.instance, create_migrations=True)

    @classmethod
    def deploy(cls) -> None:
        """Deploy a migration."""
        setup_django(settings.instance, deploy_migrations=True)

    @classmethod
    def check(cls) -> bool:
        """Check whether ORM definitions are in sync with migrations."""
        from django.core.management import call_command

        setup_django(settings.instance)
        try:
            call_command("makemigrations", check_changes=True)
        except SystemExit:
            logger.error(
                "Migrations are not in sync with ORMs, please create a migration: lamin"
                " migrate create"
            )
            return False
        return True
