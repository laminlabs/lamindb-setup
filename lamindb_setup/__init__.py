"""Setup & configure LaminDB.

Many functions in this "setup API" have a matching command in the :doc:`docs:cli` CLI.

Guide: :doc:`docs:setup`.

Basic operations:

.. autosummary::
   :toctree:

   login
   logout
   init
   disconnect
   delete

Instance operations:

.. autosummary::
   :toctree:

   migrate

Modules & settings:

.. autosummary::
   :toctree:

   settings
   core
   django

"""

__version__ = "1.5.0"  # denote a release candidate for 0.1.0 with 0.1rc1

import os

from packaging import version as packaging_version

from . import core
from ._check_setup import _check_instance_setup
from ._connect_instance import connect, load
from ._delete import delete
from ._disconnect import disconnect
from ._django import django
from ._entry_points import call_registered_entry_points as _call_registered_entry_points
from ._init_instance import init
from ._migrate import migrate
from ._register_instance import register
from ._setup_user import login, logout
from .core._settings import settings

# check that the version of s3fs is higher than the lower bound
# needed because spatialdata installs old versions of s3fs
try:
    from s3fs import __version__ as s3fs_version

    if packaging_version.parse(s3fs_version) < packaging_version.parse("2023.12.2"):
        raise RuntimeError(
            f"The version of s3fs you have ({s3fs_version}) is impompatible "
            "with lamindb, please upgrade it: pip install s3fs>=2023.12.2"
        )
except ImportError:
    # might be not installed
    pass


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

settings.__doc__ = """Global :class:`~lamindb.setup.core.SetupSettings`."""


close = disconnect  # backward compatibility
