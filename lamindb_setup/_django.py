from __future__ import annotations

from .core._settings import settings
from .core.django import setup_django


def django(command: str, package_name: str | None = None, **kwargs):
    r"""Call Django commands.

    Examples:

    Reset auto-incrementing primary integer ids after a database import:

    >>> import lamindb as ln
    >>> ln.setup.django("sqlsequencereset", "lamindb")
    BEGIN;
    SELECT setval(pg_get_serial_sequence('"lamindb_user"','id'), coalesce(max("id"), 1), max("id") IS NOT null) FROM "lamindb_user";  # noqa
    SELECT setval(pg_get_serial_sequence('"lamindb_storage"','id'), coalesce(max("id"), 1), max("id") IS NOT null) FROM "lamindb_storage";  # noqa
    COMMIT;

    You can then run the SQL output that you'll see like so:

    >>> sql = \"\"\"BEGIN;
        SELECT setval(pg_get_serial_sequence('"lamindb_user"','id'), coalesce(max("id"), 1), max("id") IS NOT null) FROM "lamindb_user";  # noqa
        SELECT setval(pg_get_serial_sequence('"lamindb_storage"','id'), coalesce(max("id"), 1), max("id") IS NOT null) FROM "lamindb_storage";  # noqa
        COMMIT;\"\"\"
    >>> from django.db import connection
    >>> with connection.cursor() as cursor:
            cursor.execute(sql)

    """
    from django.core.management import call_command

    setup_django(settings.instance)
    if package_name is not None:
        args = [package_name]
    else:
        args = []
    call_command(command, *args, **kwargs)
