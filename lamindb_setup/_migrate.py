from ._settings import settings
from .dev._django import setup_django


class migrate:
    """Manage migrations."""

    @classmethod
    def create(cls):
        setup_django(settings.instance, create_migrations=True)

    @classmethod
    def deploy(cls):
        setup_django(settings.instance, deploy_migrations=True)
