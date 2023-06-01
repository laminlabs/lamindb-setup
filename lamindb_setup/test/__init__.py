"""Test Lamin instances.

.. autosummary::
   :toctree:

   migrate_clone
   migrate_clones
   migration_id_is_consistent
   model_definitions_match_ddl

"""
# backward compat
from ._env import get_package_name
from ._migrations_e2e import migrate_clone, migrate_clones
from ._migrations_unit import migration_id_is_consistent, model_definitions_match_ddl
