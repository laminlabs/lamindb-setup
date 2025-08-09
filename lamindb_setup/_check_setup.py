from __future__ import annotations

import functools
import importlib as il
import inspect
import os
from importlib.metadata import distributions
from typing import TYPE_CHECKING
from uuid import UUID

from lamin_utils import logger

from ._silence_loggers import silence_loggers
from .core import django as django_lamin
from .core._settings import settings
from .core._settings_store import current_instance_settings_file
from .errors import (
    MODULE_WASNT_CONFIGURED_MESSAGE_TEMPLATE,
    InstanceNotSetupError,
    ModuleWasntConfigured,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from .core._settings_instance import InstanceSettings


CURRENT_ISETTINGS: InstanceSettings | None = None
MODULE_CANDIDATES: set[str] | None = None
IS_LOADING: bool = False


# decorator to disable auto-connect when importing a module such as lamindb
def disable_auto_connect(func: Callable):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        global IS_LOADING
        IS_LOADING = True
        try:
            return func(*args, **kwargs)
        finally:
            IS_LOADING = False

    return wrapper


def find_module_candidates():
    """Find all local packages that depend on lamindb."""
    global MODULE_CANDIDATES
    if MODULE_CANDIDATES is not None:
        return MODULE_CANDIDATES
    all_dists = list(distributions())
    lamindb_deps = {
        dist.metadata["Name"].lower()
        for dist in all_dists
        if dist.requires and any("lamindb" in req.lower() for req in dist.requires)
    }
    lamindb_deps.remove("lamindb")
    MODULE_CANDIDATES = lamindb_deps
    return lamindb_deps


def _get_current_instance_settings(from_module: str | None = None) -> InstanceSettings:
    from .core._settings_instance import InstanceSettings

    global CURRENT_ISETTINGS

    if CURRENT_ISETTINGS is not None:
        return CURRENT_ISETTINGS
    if current_instance_settings_file().exists():
        from .core._settings_load import load_instance_settings

        try:
            isettings = load_instance_settings()
        except Exception as e:
            # user will get more detailed traceback once they run the CLI
            logger.error(
                "Current instance cannot be reached, disconnect from it: `lamin disconnect`\n"
                "Alternatively, init or load a connectable instance on the"
                " command line: `lamin connect <instance>` or `lamin init <...>`"
            )
            raise e
    else:
        module_candidates = find_module_candidates()
        isettings = InstanceSettings(
            id=UUID("00000000-0000-0000-0000-000000000000"),
            owner="none",
            name="none",
            storage=None,
            modules=",".join(module_candidates),
        )
    CURRENT_ISETTINGS = isettings
    return isettings


def _normalize_module_name(module_name: str) -> str:
    return module_name.replace("lnschema_", "").replace("_", "-")


# checks that the provided modules is in the modules of the provided instance
# or in the apps setup by django
def _check_module_in_instance_modules(
    module: str, isettings: InstanceSettings | None = None
) -> None:
    if isettings is not None:
        modules_raw = isettings.modules
        modules = set(modules_raw).union(
            _normalize_module_name(module) for module in modules_raw
        )
        if _normalize_module_name(module) not in modules and module not in modules:
            raise ModuleWasntConfigured(
                MODULE_WASNT_CONFIGURED_MESSAGE_TEMPLATE.format(module)
            )
        else:
            return

    from django.apps import apps

    for app in apps.get_app_configs():
        # app.name is always unnormalized module (python package) name
        if module == app.name or module == _normalize_module_name(app.name):
            return
    raise ModuleWasntConfigured(MODULE_WASNT_CONFIGURED_MESSAGE_TEMPLATE.format(module))


# infer the name of the module that calls this function
def _infer_callers_module_name() -> str | None:
    stack = inspect.stack()
    if len(stack) < 3:
        return None
    module = inspect.getmodule(stack[2][0])
    return module.__name__.partition(".")[0] if module is not None else None


# we make this a private function because in all the places it's used,
# users should not see it
def _check_instance_setup(from_module: str | None = None) -> bool:
    if django_lamin.IS_SETUP:
        # reload logic here because module might not yet have been imported
        # upon first setup
        if from_module is not None:
            if from_module != "lamindb":
                _check_module_in_instance_modules(from_module)
                il.reload(il.import_module(from_module))
        else:
            infer_module = _infer_callers_module_name()
            if infer_module is not None and infer_module not in {
                "lamindb",
                "lamindb_setup",
                "lamin_cli",
            }:
                _check_module_in_instance_modules(infer_module)
        return True
    silence_loggers()
    if os.environ.get("LAMINDB_MULTI_INSTANCE") == "true":
        logger.warning(
            "running LaminDB in multi-instance mode; you'll experience "
            "errors in regular lamindb usage"
        )
        return True
    isettings = _get_current_instance_settings()
    if isettings is not None:
        if from_module is not None and not django_lamin.IS_SETUP and not IS_LOADING:
            if from_module != "lamindb":
                _check_module_in_instance_modules(from_module, isettings)

                import lamindb

                il.reload(il.import_module(from_module))
            else:
                django_lamin.setup_django(isettings)
                if isettings.slug != "none/none":
                    logger.important(f"connected lamindb: {isettings.slug}")
                    # update of local storage location through search_local_root()
                    settings._instance_settings = isettings
                else:
                    logger.warning("not connected, call: ln.connect('account/name')")
        return django_lamin.IS_SETUP
    else:
        if from_module is not None:
            # the below enables users to auto-connect to an instance
            # simply by setting an environment variable, bypassing the
            # need of calling connect() manually
            if os.environ.get("LAMIN_CURRENT_INSTANCE") is not None:
                from ._connect_instance import connect

                connect(_write_settings=False, _reload_lamindb=False)
                return django_lamin.IS_SETUP
            else:
                logger.warning(InstanceNotSetupError.default_message)
        return False
