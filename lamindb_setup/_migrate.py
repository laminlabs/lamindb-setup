from typing import Optional, Dict
from lamin_utils import logger

from ._check_instance_setup import check_instance_setup
from ._settings import settings
from .dev.django import setup_django
from django.db import connection
from django.db.migrations.loader import MigrationLoader


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
        if check_instance_setup():
            raise RuntimeError("Restart Python session to create migration or use CLI!")
        setup_django(settings.instance, create_migrations=True)

    @classmethod
    def deploy(cls) -> None:
        """Deploy a migration."""
        if check_instance_setup():
            raise RuntimeError("Restart Python session to migrate or use CLI!")
        setup_django(settings.instance, deploy_migrations=True)

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
        cls, package_name, migration_nr, start_migration_nr: Optional[str] = None
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
        from django.core.management import call_command
        from io import StringIO

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
            for key, migration in loader.disk_migrations.items():
                if hasattr(migration, "replaces"):
                    squashed_replacements.update(migration.replaces)

            deployed_migrations: Dict = {}
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT app, name, deployed
                    FROM django_migrations
                    ORDER BY app, deployed DESC
                """
                )
                for app, name, deployed in cursor.fetchall():
                    # skip migrations that are part of a squashed migration
                    if (app, name) in squashed_replacements:
                        continue

                    if app not in deployed_migrations:
                        deployed_migrations[app] = []
                    deployed_migrations[app].append(name)
            return deployed_migrations
