import lndb_schema_core as schema_core
import sqlmodel as sqm

from ._settings_load import load_or_create_instance_settings


class insert_if_not_exists:
    """Insert data if it does not yet exist."""

    @classmethod
    def user(cls, email, user_id, handle):
        settings = load_or_create_instance_settings()
        engine = settings.db_engine()
        with sqm.Session(engine) as session:
            user = session.get(schema_core.user, user_id)
        if user is None:
            user_id = insert.user(email, user_id, handle)  # type: ignore
        return user_id


class insert:
    """Insert data."""

    @classmethod
    def version_yvzi(cls, version, user_id):
        """User."""
        settings = load_or_create_instance_settings()
        engine = settings.db_engine()

        with sqm.Session(engine) as session:
            row = schema_core.version_yvzi(v=version, user_id=user_id)
            session.add(row)
            session.commit()
        settings._update_cloud_sqlite_file()

    @classmethod
    def user(cls, email, user_id, handle):
        """User."""
        settings = load_or_create_instance_settings()
        engine = settings.db_engine()

        with sqm.Session(engine) as session:
            user = schema_core.user(id=user_id, email=email, handle=handle)
            session.add(user)
            session.commit()
            session.refresh(user)

        settings._update_cloud_sqlite_file()

        return user.id
