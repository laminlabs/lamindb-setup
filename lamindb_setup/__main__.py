import argparse
from importlib.metadata import PackageNotFoundError, version

# most important dynamic to optimize import time
from ._docstrings import instance_description as instance
from .dev._settings_user import user_description as user

login_help = "Log in an already-signed-up user."
init_help = "Init & config instance with db & storage."
load_help = "Load instance by name."
set_storage_help = "Set storage used by an instance."
load_storage_help = "Load the instance with an updated default storage."
info_help = "Show current instance information."
close_help = "Close instance."
migr_help = "Manage migrations."
delete_help = "Delete an instance."
schema_help = "View schema."
register_help = (
    "Register instance on hub (local instances are not automatically registered)."
)
track_help = "Track a notebook (init notebook metadata)."
save_help = "Save a notebook."
cache_help = "Manage cache."
version_help = "Show the version and exit."

description_cli = "Configure LaminDB and perform simple actions."
parser = argparse.ArgumentParser(
    description=description_cli, formatter_class=argparse.RawTextHelpFormatter
)
subparsers = parser.add_subparsers(dest="command")

# init instance
init = subparsers.add_parser("init", help=init_help)
aa = init.add_argument
aa("--storage", type=str, metavar="s", help=instance.storage_root)
aa("--db", type=str, metavar="d", default=None, help=instance.db)
aa("--schema", type=str, metavar="schema", default=None, help=instance.schema)
aa("--name", type=str, metavar="n", default=None, help=instance.name)
aa(
    "--vault",
    default=False,
    action="store_true",
    help="Use vault to manage credentials.",
)

# init instance vault
init = subparsers.add_parser("init-vault", help=init_help)
aa = init.add_argument
aa("--db", type=str, metavar="d", default=None, help=instance.db)

# load instance
load = subparsers.add_parser("load", help=load_help)
aa = load.add_argument
instance_help = """
The instance identifier can the instance name (owner is
current user), handle/name, or the URL: https://lamin.ai/handle/name."""
aa("instance", type=str, metavar="i", default=None, help=instance_help)
aa("--db", type=str, metavar="d", default=None, help=instance.db)
aa("--storage", type=str, metavar="s", default=None, help=load_storage_help)
aa(
    "--vault",
    default=False,
    action="store_true",
    help="Use vault to manage credentials.",
)

# delete instance
delete_parser = subparsers.add_parser("delete", help=delete_help)
aa = delete_parser.add_argument
aa("instance", type=str, metavar="i", default=None, help=instance.name)
aa = delete_parser.add_argument
aa("--force", default=False, action="store_true", help="Do not ask for confirmation")

# show instance info
info_parser = subparsers.add_parser("info", help=info_help)

# set storage
set_storage_parser = subparsers.add_parser("set", help=set_storage_help)
aa = set_storage_parser.add_argument
aa("--storage", type=str, metavar="f", help=instance.storage_root)

# close instance
subparsers.add_parser("close", help=close_help)

# register instance
subparsers.add_parser("register", help=register_help)

# migrate
migr = subparsers.add_parser("migrate", help=migr_help)
aa = migr.add_argument
aa(
    "action",
    choices=["create", "deploy", "squash"],
    help="Manage migrations.",
)
aa("--package-name", type=str, default=None)
aa("--end-number", type=str, default=None)
aa("--start-number", type=str, default=None)

# schema
schema_parser = subparsers.add_parser("schema", help=schema_help)
aa = schema_parser.add_argument
aa(
    "action",
    choices=["view"],
    help="View schema.",
)

# track a notebook (init nbproject metadata)
track_parser = subparsers.add_parser("track", help=track_help)
aa = track_parser.add_argument
filepath_help = "A path to the notebook."
aa("filepath", type=str, metavar="filepath", help=filepath_help)
pypackage_help = "One or more (delimited by ',') python packages to track."
aa("--pypackage", type=str, metavar="pypackage", default=None, help=pypackage_help)

# save a notebook (in the future, any file)
save_parser = subparsers.add_parser("save", help=save_help)
aa = save_parser.add_argument
filepath_help = "A path to the notebook."
aa("filepath", type=str, metavar="filepath", help=filepath_help)

# login user
login = subparsers.add_parser("login", help=login_help)
aa = login.add_argument
aa(
    "user",
    type=str,
    metavar="user",
    help="Email or user handle. Email is needed at first login.",
)  # noqa
aa("--key", type=str, metavar="key", default=None, help=user.password)
aa("--password", type=str, metavar="pw", default=None, help=user.password)

# manage cache
cache_parser = subparsers.add_parser("cache", help=cache_help)
cache_subparser = cache_parser.add_subparsers(dest="cache_action")
clear_parser = cache_subparser.add_parser("clear", help="Clear the cache directory.")
set_parser = cache_subparser.add_parser("set", help="Set the cache directory.")
aa = set_parser.add_argument
aa(
    "cache_dir",
    type=str,
    metavar="cache_dir",
    help="A new directory for the lamindb cache.",
)

# show version
try:
    lamindb_version = version("lamindb")
except PackageNotFoundError:
    lamindb_version = "Cannot be determined."

parser.add_argument("--version", action="version", version=lamindb_version)

# parse args
args = parser.parse_args()


def main():
    from ._silence_loggers import silence_loggers

    silence_loggers()
    if args.command == "login":
        from ._setup_user import login

        return login(
            args.user,
            key=args.key,
            password=args.password,
        )
    elif args.command == "init":
        from ._init_instance import init

        return init(
            storage=args.storage,
            db=args.db,
            schema=args.schema,
            name=args.name,
            _vault=args.vault,
        )
    elif args.command == "init-vault":
        from ._init_vault import init_vault

        return init_vault(
            db=args.db,
        )
    elif args.command == "load":
        from ._load_instance import load

        return load(
            identifier=args.instance,
            db=args.db,
            storage=args.storage,
            _vault=args.vault,
        )
    elif args.command == "close":
        from ._close import close

        return close()
    elif args.command == "register":
        from ._register_instance import register

        return register()
    elif args.command == "delete":
        from ._delete import delete

        return delete(args.instance, force=args.force)
    elif args.command == "info":
        from ._info import info

        return info()
    elif args.command == "set":
        from ._set import set

        return set.storage(args.storage)
    elif args.command == "migrate":
        from ._migrate import migrate

        if args.action == "create":
            return migrate.create()
        elif args.action == "deploy":
            return migrate.deploy()
        elif args.action == "squash":
            return migrate.squash(
                package_name=args.package_name,
                migration_nr=args.end_number,
                start_migration_nr=args.start_number,
            )
    elif args.command == "schema":
        from ._schema import view

        if args.action == "view":
            return view()
    elif args.command == "track":
        from ._notebook import track

        track(args.filepath, args.pypackage)
    elif args.command == "save":
        from ._notebook import save

        return save(args.filepath)
    elif args.command == "cache":
        from ._cache import get_cache_dir, set_cache_dir, clear_cache_dir

        if args.cache_action == "set":
            set_cache_dir(args.cache_dir)
        elif args.cache_action == "clear":
            clear_cache_dir()
        else:
            print(f"The cache directory of the current instance is {get_cache_dir()}.")
    else:
        parser.print_help()
    return 0
