# schemas


# the below is the actual *python package name*
# that is, fully qualified and hopefully unique
# within the python package namespace of every user
# should be if starting with lnschema_...
def get_schema_module_name(schema_name):
    if schema_name == "bfx":
        return "lnbfx.schema"
    else:
        return f"lnschema_{schema_name.replace('-', '_')}"


# I believe the below is what is equivalent to the _name
# attribute in the schema module package
# but we need a test to enforce this!
def get_schema_lookup_name(schema_name):
    if schema_name == "harmonic-docking":
        return "docking"
    else:
        return schema_name


# these are globally unique schema handles
# we might loosen the uniqueness requirement
# currently, these handles are usually called "name"
# this is confusing as there are also module names and lookup names
schema_handles = [
    "bionty",
    "wetlab",
    "drylab",
    "bfx",
    "retro",
    "swarm",
    "harmonic-docking",
]
schemas = schema_handles
