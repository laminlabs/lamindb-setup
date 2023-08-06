import importlib
from types import ModuleType

from importlib_metadata import requires as importlib_requires
from importlib_metadata import version as get_pip_version
from lamin_utils import logger
from packaging.requirements import Requirement

from ._django import setup_django
from ._settings_instance import InstanceSettings


def get_schema_module_name(schema_name):
    return f"lnschema_{schema_name.replace('-', '_')}"


def check_schema_version_and_import(schema_name) -> ModuleType:
    lamindb_installed = True
    try:
        get_pip_version("lamindb")  # noqa
    except Exception:
        lamindb_installed = False

    def check_version(module_version):
        if not lamindb_installed:
            return None
        schema_module_name = get_schema_module_name(schema_name)
        lamindb_version = get_pip_version("lamindb")
        for req in importlib_requires("lamindb"):
            req = Requirement(req)
            if schema_module_name == req.name:
                if not req.specifier.contains(module_version):
                    raise RuntimeError(
                        f"lamindb v{lamindb_version} needs"
                        f" lnschema_{schema_name}{req.specifier}, you have"
                        f" {module_version}"
                    )

    try:
        # use live version if we can import
        module = importlib.import_module(get_schema_module_name(schema_name))
        module_version = module.__version__
    except Exception as import_error:
        # use pypi version instead
        module_version = get_pip_version(get_schema_module_name(schema_name))
        check_version(module_version)
        raise import_error

    check_version(module_version)
    return module


def load_schema(isettings: InstanceSettings, *, init: bool = False):
    setup_django(isettings, deploy_migrations=init, init=init)

    schema_names = ["core"] + list(isettings.schema)
    msg = ""
    for schema_name in schema_names:
        module = check_schema_version_and_import(schema_name)
        msg += f"{schema_name}=={module.__version__} "
    if init:
        logger.info(f"creating schemas: {msg}")
    return msg, schema_names
