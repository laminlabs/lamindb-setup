Custom migrations
=================

.. currentmodule:: lamindb_setup.evolve

Write a migration for a change that needs custom logic.

The change being applied determines which context the migration receives.

Migration contexts
------------------

.. autoclass:: MigrationContext

.. autoclass:: DeclarationTarget

.. autoclass:: MemberMigrationContext

.. autoclass:: MemberRemovalContext

.. autoclass:: IndexChangeContext

.. autoclass:: FlexibleChangeContext

.. autoclass:: FeatureRemovalContext

.. autoclass:: RecordTypeRemovalContext

.. autoclass:: SchemaRemovalContext

.. autoclass:: SheetExportContext

.. autoclass:: RecordUpdateContext

.. autoclass:: RecordRemovalContext

Migration helpers
-----------------

.. autofunction:: backfill_members

.. autofunction:: clear_values

.. autofunction:: clear_member_values

.. autofunction:: clear_nonmember_annotations

.. autofunction:: relocate_to_records

.. autofunction:: relocate_to_str

.. autofunction:: rewrite_values
