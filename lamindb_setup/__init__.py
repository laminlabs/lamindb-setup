"""Setup & configure LaminDB.

Many functions in the `setup` module have a matching command in the :doc:`docs:cli`.

Guide: :doc:`docs:setup`.

Basic operations
----------------

.. autofunction:: login
.. autofunction:: logout
.. autofunction:: init
.. autofunction:: disconnect
.. autofunction:: delete

Modules & settings
------------------

.. autosummary::
   :toctree:

   settings
   core
   django
   errors
   types

Migration management
--------------------

.. autosummary::
   :toctree:

   migrate

"""

__version__ = "1.25.5"  # denote a release candidate for 0.1.0 with 0.1rc1

import os
from importlib import import_module

# do not import io by default to reduce import time
# it's not immediately needed in the default user workflows
from ._entry_points import call_registered_entry_points as _call_registered_entry_points

_LAZY_ATTRS: dict[str, tuple[str, str | None]] = {
    "core": (".core", None),
    "errors": (".errors", None),
    "types": (".types", None),
    "_check_instance_setup": ("._check_setup", "_check_instance_setup"),
    "connect": ("._connect_instance", "connect"),
    "delete": ("._delete", "delete"),
    "disconnect": ("._disconnect", "disconnect"),
    "django": ("._django", "django"),
    "init": ("._init_instance", "init"),
    "migrate": ("._migrate", "migrate"),
    "register": ("._register_instance", "register"),
    "login": ("._setup_user", "login"),
    "logout": ("._setup_user", "logout"),
    "settings": (".core._settings", "settings"),
}


def _is_CI_environment() -> bool:
    ci_env_vars = [
        "LAMIN_TESTING",  # Set by our nox configurations
        "CI",  # Commonly set by many CI systems
        "TRAVIS",  # Travis CI
        "GITHUB_ACTIONS",  # GitHub Actions
        "GITLAB_CI",  # GitLab CI/CD
        "CIRCLECI",  # CircleCI
        "JENKINS_URL",  # Jenkins
        "TEAMCITY_VERSION",  # TeamCity
        "BUILDKITE",  # Buildkite
        "BITBUCKET_BUILD_NUMBER",  # Bitbucket Pipelines
        "APPVEYOR",  # AppVeyor
        "AZURE_HTTP_USER_AGENT",  # Azure Pipelines
        "BUDDY",  # Buddy
        "DRONE",  # Drone CI
        "HUDSON_URL",  # Hudson
        "CF_BUILD_ID",  # Codefresh
        "WERCKER",  # Wercker
        "NOW_BUILDER",  # ZEIT Now
        "TASKCLUSTER_ROOT_URL",  # TaskCluster
        "SEMAPHORE",  # Semaphore CI
        "BUILD_ID",  # Generic build environments
    ]
    return any(env_var in os.environ for env_var in ci_env_vars)


_TESTING = _is_CI_environment()

# provide a way for other packages to run custom code on import
_call_registered_entry_points("lamindb_setup.on_import")


def __getattr__(name: str):
    if name == "close":
        return import_module("._disconnect", __name__).disconnect
    if name in _LAZY_ATTRS:
        module_name, attr_name = _LAZY_ATTRS[name]
        module = import_module(module_name, __name__)
        value = module if attr_name is None else getattr(module, attr_name)
        if name == "settings":
            value.__doc__ = """Global :class:`~lamindb.setup.core.SetupSettings`."""
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
