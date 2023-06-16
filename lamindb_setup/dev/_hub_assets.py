# these are globally unique schema handles
# we might loosen the uniqueness requirement
# currently, these handles are usually called "name"
# this is confusing as there are also module names and lookup names

schema_handles = [
    "hub",
    "bionty",
    "wetlab",
    "lamin1",
    "hedera",
    "retro",
    "swarm",
    "harmonic-docking",
    "trexbio",
]
schemas = schema_handles


def get_schema_module_name(schema_name):
    if schema_name == "bfx":
        return "lnbfx.schema"
    elif schema_name == "hub":
        return "lnhub_rest.schema"
    else:
        return f"lnschema_{schema_name.replace('-', '_')}"
