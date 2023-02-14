from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlmodel import SQLModel

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name, disable_existing_loggers=False)

target_metadata = SQLModel.metadata
target_metadata.naming_convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_`%(constraint_name)s`",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

_schema_id = None
# from {package_name} import *  # noqa
# from {package_name} import _schema_id  # noqa


def _upg_dwg_token(db: str):
    if db != "sqlite:///testdb/testdb.lndb":
        return {"upgrade_token": "", "downgrade_token": ""}
    else:
        return {}


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    add_kwargs = {}
    if url.startswith("sqlite:///"):
        add_kwargs = _upg_dwg_token(url)
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        **add_kwargs,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    config_section = config.get_section(config.config_ini_section)
    # see https://pytest-alembic.readthedocs.io/en/latest/setup.html#env-py
    connectable = context.config.attributes.get("connection", None)
    # below follows the standard case
    if connectable is None:
        connectable = engine_from_config(
            config_section,
            prefix="sqlalchemy.",
            poolclass=pool.NullPool,
        )

    add_kwargs = {}
    render_as_batch = False
    if "sqlalchemy.url" in config_section:
        if config_section["sqlalchemy.url"].startswith("sqlite:///"):
            render_as_batch = True
            # for users that rely on CI to generate migration script
            # we generate empty script here
            add_kwargs = _upg_dwg_token(config_section["sqlalchemy.url"])

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table=f"migration_{_schema_id}",
            render_as_batch=render_as_batch,
            compare_type=True,
            include_schemas=True,
            **add_kwargs,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
