# Generate migrations

```{warning}

Currently only supports users with registered schema modules, [contact us](https://lamin.ai/contact) if you are interested!
```

To generate robust migrations for your database based on [alembic](https://alembic.sqlalchemy.org/en/latest/):

- For postgres instances, we offer a CI-guided workflow.
- For sqlite instances, we offer a CLI based workflow.

## Before starting

- Your schema module repository is set up to test migrations (`lnschema_{module_name}`)
- You have are created a LaminDB instance with your schema module mounted
- Your instance is up-to-date on the main branch

## Steps

1. Create a new branch (e.g. `migration`) in your schema module repository.
2. On your `migration` branch, modify the sqlmodel code (typically located in `{package_name/_core.py}`) to reflect the desired changes of the schema.
3. On the command line (at the root of the repository), run `lndb migrate generate` to generate an empty script file under `{package_name}/versions/`. The script will be named as `{date}-{revision_id}-vx_x_x.py`. You will notice that script or `upgrade` and `downgrade` are empty.
4. Update the revision_id in the {package_name}/**init**.py file: `_migration = {revision_id}`.
5. Commit the changes and push the `migration` branch to remote.
6. Create a Pull Request from the `migration` branch, and inspect the CI results.
7. CI will fail and prints out the migration code to be added to the migration script file.
