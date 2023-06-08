# Migrate an instance

Edit the Django ORMs in your schema (you need to have the schema package installed locally: `cd lnschema-custom; pip install -e .`).

Then, call

```
lamin migrate create
```

to create the migration script.

Deploy the migration to your instance via

```
lamin migrate deploy
```

```{note}

The `lamin` migration commands are a shallow wrapper around Django's migration manager.

```
