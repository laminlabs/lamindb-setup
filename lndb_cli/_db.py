import lamindb_schema as schema
import sqlmodel as sqm

from . import load_or_create_instance_settings


class insert_if_not_exists:
    """Insert data if it does not yet exist."""

    @classmethod
    def user(cls, user_email, user_id):
        settings = load_or_create_instance_settings()
        engine = settings.db_engine()
        with sqm.Session(engine) as session:
            user = session.get(schema.core.user, user_id)
        if user is None:
            user_id = insert.user(user_email, user_id)  # type: ignore
        return user_id


class insert:
    """Insert data."""

    @classmethod
    def schema_version(cls, version, user_id):
        """User."""
        settings = load_or_create_instance_settings()
        engine = settings.db_engine()

        with sqm.Session(engine) as session:
            user = schema.core.schema_version(id=version, user_id=user_id)
            session.add(user)
            session.commit()

        settings._update_cloud_sqlite_file()

    @classmethod
    def user(cls, user_email, user_id):
        """User."""
        settings = load_or_create_instance_settings()
        engine = settings.db_engine()

        with sqm.Session(engine) as session:
            user = schema.core.user(id=user_id, email=user_email)
            session.add(user)
            session.commit()
            session.refresh(user)

        settings._update_cloud_sqlite_file()

        return user.id
