Data migrations
===============

.. currentmodule:: lamindb_setup.evolve

Fill, map, move, or drop existing values when applying a plan.

Attach a migration to its target with :meth:`PlanContext.migrate`. Preview it
with `lamin evolve plan --preview-migrations` before applying it.

Migration types
---------------

.. autoclass:: DataMigration

.. autoclass:: MigrationReport

.. autoexception:: MigrationError

Fill and map values
-------------------

.. autofunction:: fill_nulls

.. autofunction:: fill_missing

.. autofunction:: map_values

.. autofunction:: map_to_records

.. autofunction:: map_to_str

Remove and revalidate data
--------------------------

.. autofunction:: drop_values

.. autofunction:: drop_rows

.. autofunction:: revalidate_against

.. autofunction:: revalidate_rows

.. autofunction:: reexport_sheets

Accept schema changes
---------------------

.. autofunction:: accept_non_null

.. autofunction:: accept_index_change

.. autofunction:: accept_flexible_change

Handle existing metadata
------------------------

.. autofunction:: drop_member_values

.. autofunction:: keep_member_values

.. autofunction:: drop_nonmember_annotations

.. autofunction:: keep_stale_exports

.. autofunction:: keep_values

.. autofunction:: keep_validated

.. autofunction:: keep_rows

Migrate records
---------------

.. autofunction:: move_rows

.. autofunction:: adopt_children

.. autofunction:: update_record

.. autofunction:: drop_record

Fill values
-----------

.. autodata:: NULL

.. autodata:: DEFAULT
