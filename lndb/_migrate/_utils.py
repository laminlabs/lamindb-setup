import importlib
from pathlib import Path
from typing import Optional

from lndb.test import get_package_name
from lndb.test._migrations_unit import get_migration_id_from_scripts

from ..dev._settings_instance import InstanceSettings
from ..dev._setup_schema import get_schema_module_name


def get_package_info(
    schema_root: Optional[Path] = None, package_name: Optional[str] = None
):
    if package_name is None:
        package_name = get_package_name(schema_root)
    package = importlib.import_module(package_name)
    if not hasattr(package, "_schema_id"):
        package_name = f"{package_name}.schema"
        package = importlib.import_module(package_name)
    schema_id = getattr(package, "_schema_id")
    migrations_path = Path(package.__file__).parent / "migrations"  # type:ignore
    return package_name, migrations_path, schema_id


def generate_alembic_ini(
    package_name: str,
    migrations_path: Optional[Path] = None,
    schema_id: Optional[str] = None,
):
    if migrations_path is None:
        _, migrations_path, schema_id = get_package_info(package_name=package_name)
    _migrations_path = Path(__file__).parent

    if not (migrations_path.parent / "alembic.ini").exists():
        content = (
            _readfile(_migrations_path / "alembic.ini")
            .replace("[{schema_id}]", f"[{schema_id}]")
            .replace("{package_name}/migrations", f"{package_name}/migrations")
        )
        _writefile(migrations_path.parent / "alembic.ini", content)


def modify_migration_id_in__init__(package_name) -> None:
    """Rewrite the migration_id in the __init__.py of the schema module."""
    package = importlib.import_module(package_name)
    filepath = str(package.__file__)

    with open(filepath) as f:
        content = f.read()

    # get line with migration id
    for line in content.split("\n"):
        if line.startswith("_migration = "):
            current_line = line
            break

    migration_id = get_migration_id_from_scripts(package_name)
    content = content.replace(current_line, f'_migration = "{migration_id}"')

    with open(filepath, "w") as f:
        f.write(content)


def modify_alembic_ini(
    filepath: Path,
    isettings: InstanceSettings,
    schema_name: Optional[str] = None,
    package_name: Optional[str] = None,
    revert: bool = False,
    move_sl: bool = True,
):
    if package_name is None:
        package_name = get_schema_module_name(schema_name)
    schema_module_path = package_name.replace(".", "/") + "/migrations"
    sl_from = schema_module_path
    sl_to = "migrations" if move_sl else schema_module_path
    url_from = "sqlite:///testdb/testdb.lndb"
    url_to_sqlite = f"sqlite:///{isettings._sqlite_file_local}"
    url_to = url_to_sqlite if isettings.dialect == "sqlite" else isettings.db

    if revert:
        sl_from, sl_to = sl_to, sl_from
        url_from, url_to = url_to, url_from

    with open(filepath) as f:
        content = f.read()

    content = content.replace(
        f"script_location = {sl_from}",
        f"script_location = {sl_to}",
    ).replace(
        f"sqlalchemy.url = {url_from}",
        f"sqlalchemy.url = {url_to}",
    )

    with open(filepath, "w") as f:
        f.write(content)


def generate_module_files(
    package_name: str,
    migrations_path: Optional[Path] = None,
    schema_id: Optional[str] = None,
):
    if migrations_path is None:
        _, migrations_path, schema_id = get_package_info(package_name=package_name)
    _migrations_path = Path(__file__).parent

    # ensures migrations/versions folder exists
    (migrations_path / "versions").mkdir(exist_ok=True, parents=True)

    if not (migrations_path / "env.py").exists():
        content = (
            _readfile(_migrations_path / "env.py")
            .replace("_schema_id = None\n", "")
            .replace("# from {package_name} import *", f"from {package_name} import *")
            .replace(
                "# from {package_name} import _schema_id",
                f"from {package_name} import _schema_id",
            )
        )
        _writefile(migrations_path / "env.py", content)

    if not (migrations_path / "script.py.mako").exists():
        import shutil

        shutil.copyfile(
            _migrations_path / "script.py.mako", migrations_path / "script.py.mako"
        )

    generate_alembic_ini(
        package_name=package_name, migrations_path=migrations_path, schema_id=schema_id
    )


def set_alembic_logging_level(migrations_path: Path, level="INFO"):
    alembic_ini_path = migrations_path.parent / "alembic.ini"
    text = _readfile(alembic_ini_path)
    current_level = text.split("[logger_alembic]\n")[1].split("\n")[0]
    new_level = f"level = {level}"
    _writefile(
        alembic_ini_path,
        text.replace(
            f"[logger_alembic]\n{current_level}", f"[logger_alembic]\n{new_level}"
        ),
    )


def _readfile(filename: Path):
    with open(filename, "r") as f:
        return f.read()


def _writefile(filename: Path, content: str):
    with open(filename, "w") as f:
        return f.write(content)
