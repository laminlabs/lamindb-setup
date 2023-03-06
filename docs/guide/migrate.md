# Migrate an instance

```{warning}

Currently only supports users with registered schema modules. [Reach out](https://lamin.ai/contact) if you are interested!
```

This page documents how to create a tested migration using the CI-guided workflow for postgres-based instances.

1. Create a new branch (e.g. `migr`) in your schema module repository.
2. On your `migr` branch, update an ORM, e.g., by renaming a column.

   :::{dropdown} Example: Rename a column.
   Let's try to rename the `v` column to `version` in `Notebook` table of `lnschema_core`.

   In the `lnschema_core/_core.py` `Notebook` class, modify line:

   ```{code-block} python
   ---
   emphasize-lines: 1, 3
   ---
   v: str = Field(default="1", primary_key=True)
   to:
   version: str = Field(default="1", primary_key=True)
   ```

   :::

3. On the command line (at the root of the repository), run `lamin migrate generate` to generate an empty script file under `{package_name}/migrations/versions/`.

   :::{dropdown} Example: Migration script location.
   The script will be named `{date}-{revision}-vx_x_x.py`.

   ```{code-block} yaml
   ---
   emphasize-lines: 5
   ---
   -- lnschema_core
   |-- dev
   |-- migrations
   |   |-- versions
   |       |-- 2023-02-16-dd2b4a9499f2-vx_x_x.py
   |-- __init__.py
   |-- _core.py
   ```

   :::

   :::{dropdown} Example: Content of migration script.

   ```{code-block} python
   ---
   emphasize-lines: 10
   ---
   """vX.X.X."""
   from alembic import op
   import sqlalchemy as sa  # noqa
   import sqlmodel as sqm # noqa

   revision = 'dd2b4a9499f2'
   down_revision = '8280855a5064'


   def upgrade() -> None:
       pass


   def downgrade() -> None:
       pass
   ```

   :::

4. Populate `_migration` with the revision id in the `{package_name}/__init__.py` file:

   :::{dropdown} Example: Updated \_migration.

   ```{code-block} python
   _migration = "dd2b4a9499f2"
   ```

   :::

5. Create a pull request from the `migr` branch and inspect the output of the failing CI step `Build`.

   :::{dropdown} Example: Bottom of failed CI output.

   ```{code-block} python
   op.alter_column("notebook", column_name="v", new_column_name="version", schema="core")
   ```

   :::

6. Copy the migration script inside the `update()` function in the `{date}-{revision}-vx_x_x.py` file.

   :::{dropdown} Example: Modified migration script.

   ```{code-block} python
   def upgrade() -> None:
       op.alter_column("notebook", column_name="v", new_column_name="version", schema="core")
   ```

   :::

7. Commit & push changes: Now CI should pass and you have created a successful migration! 🎉

8. Merge PR and make a release with a new `__version__`.

   :::{dropdown} Example: Make a new release, e.g., "1.0.0".

   1. Modify `__version__` in the `__init__.py` file: `__version__ = "1.0.0"`
   2. Modify the migration script file name to: `2023-02-16-dd2b4a9499f2-v1_0_0.py`
   3. Modify the first line of the migration script file from `"""vX.X.X."""` to `"""v1.0.0."""`
      :::

**Next time you load your instance, you will be asked to deploy the migration!**

```{note}

Under the hood `lamin` leverages & is fully compatible with [alembic](https://alembic.sqlalchemy.org/en/latest/).
```
