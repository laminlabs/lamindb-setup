from __future__ import annotations

from django.db import connection
from django.db.migrations.loader import MigrationLoader
from lamin_utils import logger
from packaging import version

from ._check_setup import _check_instance_setup
from .core._settings import settings
from .core.django import setup_django


# for the django-based synching code, see laminhub_rest
def check_whether_migrations_in_sync(db_version_str: str):
    from importlib import metadata

    try:
        installed_version_str = metadata.version("lamindb")
    except metadata.PackageNotFoundError:
        return None
    if db_version_str is None:
        logger.warning("no lamindb version stored to compare with installed version")
        return None
    installed_version = version.parse(installed_version_str)
    db_version = version.parse(db_version_str)
    if (
        installed_version.major < db_version.major
        or installed_version.minor < db_version.minor
    ):
        db_version_lower = f"{db_version.major}.{db_version.minor}"
        db_version_upper = f"{db_version.major}.{db_version.minor + 1}"
        logger.warning(
            f"your database ({db_version_str}) is ahead of your installed lamindb"
            f" package ({installed_version_str}) \n❗ please update lamindb: pip install"
            f' "lamindb>={db_version_lower},<{db_version_upper}"'
        )
    elif (
        installed_version.major > db_version.major
        or installed_version.minor > db_version.minor
    ):
        logger.warning(
            f"your database ({db_version_str}) is behind your installed lamindb package"
            f" ({installed_version_str}) \n❗ please migrate your database: lamin"
            " migrate deploy"
        )


# for tests, see lamin-cli
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
        if _check_instance_setup():
            raise RuntimeError("Restart Python session to create migration or use CLI!")
        setup_django(settings.instance, create_migrations=True)

    @classmethod
    def deploy(cls) -> None:
        """Deploy a migration."""
        if _check_instance_setup():
            raise RuntimeError("Restart Python session to migrate or use CLI!")
        from lamindb_setup.core._hub_client import call_with_fallback_auth
        from lamindb_setup.core._hub_crud import (
            select_collaborator,
            select_instance_by_id,
            update_instance,
        )

        instance_id_str = settings.instance._id.hex
        instance = call_with_fallback_auth(
            select_instance_by_id, instance_id=instance_id_str
        )
        instance_is_on_hub = instance is not None
        if instance_is_on_hub:
            # double check that user is an admin, otherwise will fail below
            # without idempotence
            collaborator = call_with_fallback_auth(
                select_collaborator,
                instance_id=instance_id_str,
                account_id=settings.user._uuid,
            )
            if collaborator is None or collaborator["role"] != "admin":
                raise SystemExit(
                    "❌ Only admins can deploy migrations, please ensure that you're an"
                    f" admin: https://lamin.ai/{settings.instance.slug}/settings"
                )
            # we need lamindb to be installed, otherwise we can't populate the version
            # information in the hub
            import lamindb

        # this sets up django and deploys the migrations
        setup_django(settings.instance, deploy_migrations=True)
        # this populates the hub
        if instance_is_on_hub:
            logger.important(f"updating lamindb version in hub: {lamindb.__version__}")
            call_with_fallback_auth(
                update_instance,
                instance_id=settings.instance._id.hex,
                instance_fields={"lamindb_version": lamindb.__version__},
            )

    @classmethod
    def check(cls) -> bool:
        """Check whether Registry definitions are in sync with migrations."""
        from django.core.management import call_command

        setup_django(settings.instance)
        try:
            call_command("makemigrations", check_changes=True)
        except SystemExit:
            logger.error(
                "migrations are not in sync with ORMs, please create a migration: lamin"
                " migrate create"
            )
            return False
        return True

    @classmethod
    def squash(
        cls, package_name, migration_nr, start_migration_nr: str | None = None
    ) -> None:
        """Squash migrations."""
        from django.core.management import call_command

        setup_django(settings.instance)
        if start_migration_nr is not None:
            call_command(
                "squashmigrations", package_name, start_migration_nr, migration_nr
            )
        else:
            call_command("squashmigrations", package_name, migration_nr)

    @classmethod
    def show(cls) -> None:
        """Show migrations."""
        from django.core.management import call_command

        setup_django(settings.instance)
        call_command("showmigrations")

    @classmethod
    def defined_migrations(cls, latest: bool = False):
        from io import StringIO

        from django.core.management import call_command

        def parse_migration_output(output):
            """Parse the output of the showmigrations command to get migration names."""
            lines = output.splitlines()

            # Initialize an empty dict to store migration names of each module
            migration_names = {}

            # Process each line
            for line in lines:
                if " " not in line:
                    # CLI displays the module name in bold
                    name = line.strip().replace("\x1b[1m", "")
                    migration_names[name] = []
                    continue
                # Strip whitespace and split the line into status and migration name
                migration_name = line.strip().split("] ")[-1].split(" ")[0]
                # The second part is the migration name
                migration_names[name].append(migration_name)

            return migration_names

        out = StringIO()
        call_command("showmigrations", stdout=out)
        out.seek(0)
        output = out.getvalue()
        if latest:
            return {k: v[-1] for k, v in parse_migration_output(output).items()}
        else:
            return parse_migration_output(output)

    @classmethod
    def deployed_migrations(cls, latest: bool = False):
        """Get the list of deployed migrations from Migration table in DB."""
        if latest:
            latest_migrations = {}
            with connection.cursor() as cursor:
                # query to get the latest migration for each app that is not squashed
                cursor.execute(
                    """
                    SELECT app, name
                    FROM django_migrations
                    WHERE id IN (
                        SELECT MAX(id)
                        FROM django_migrations
                        WHERE name NOT LIKE '%%_squashed_%%'
                        GROUP BY app
                    )
                """
                )
                # fetch all the results
                for app, name in cursor.fetchall():
                    latest_migrations[app] = name

            return latest_migrations
        else:
            # Load all migrations using Django's migration loader
            loader = MigrationLoader(connection)
            squashed_replacements = set()
            for _key, migration in loader.disk_migrations.items():
                if hasattr(migration, "replaces"):
                    squashed_replacements.update(migration.replaces)

            deployed_migrations: dict = {}
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT app, name, deployed
                    FROM django_migrations
                    ORDER BY app, deployed DESC
                """
                )
                for app, name, _deployed in cursor.fetchall():
                    # skip migrations that are part of a squashed migration
                    if (app, name) in squashed_replacements:
                        continue

                    if app not in deployed_migrations:
                        deployed_migrations[app] = []
                    deployed_migrations[app].append(name)
            return deployed_migrations
