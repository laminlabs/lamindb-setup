import argparse
from importlib.metadata import PackageNotFoundError, version

# most important dynamic to optimize import time
from ._docstrings import instance_description as instance
from .dev._settings_user import user_description as user

signup_help = "First time sign up."
login_help = "Log in an already-signed-up user."
init_help = "Init & config instance with db & storage."
load_help = "Load instance by name."
set_storage_help = "Set storage used by an instance."
load_storage_help = "Load the instance with an updated default storage."
info_help = "Show current instance information."
close_help = "Close instance."
migr_help = "Manage migrations."
delete_help = "Delete an instance."
register_help = (
    "Register instance on hub (local instances are not automatically registered)."
)
track_help = "Track a notebook (init metadata)."
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

# load instance
load = subparsers.add_parser("load", help=load_help)
aa = load.add_argument
instance_help = """
The instance identifier can the instance name (owner is
current user), handle/name, or the URL: https://lamin.ai/handle/name."""
aa("instance", type=str, metavar="i", default=None, help=instance_help)
aa("--storage", type=str, metavar="s", default=None, help=load_storage_help)

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
aa("action", choices=["create", "deploy"], help="Manage migrations.")

# track a notebook (init nbproject metadata)
track_parser = subparsers.add_parser("track", help=track_help)
aa = track_parser.add_argument
filepath_help = "A path to the notebook to track."
aa("filepath", type=str, metavar="filepath", help=filepath_help)
pypackage_help = "One or more (delimited by ',') python packages to track."
aa("--pypackage", type=str, metavar="pypackage", default=None, help=pypackage_help)

# signup user
signup = subparsers.add_parser("signup", help=signup_help)
aa = signup.add_argument
aa("email", type=str, metavar="email", help=user.email)

# login user
login = subparsers.add_parser("login", help=login_help)
aa = login.add_argument
aa(
    "user",
    type=str,
    metavar="user",
    help="Email or user handle. Email is needed at first login.",
)  # noqa
aa("--password", type=str, metavar="pw", default=None, help=user.password)

# show version
try:
    lamindb_version = version("lamindb")
except PackageNotFoundError:
    lamindb_version = "Cannot be determined."

parser.add_argument("--version", action="version", version=lamindb_version)

# parse args
args = parser.parse_args()


def process_result(result):
    if result in ["migrate-unnecessary", "migrate-success", "migrate-skipped", None]:
        return None  # is interpreted as success (exit code 0) by shell
    else:
        return result


def main():
    if args.command == "signup":
        from ._setup_user import signup

        return signup(email=args.email)
    if args.command == "login":
        from ._setup_user import login

        return login(
            args.user,
            password=args.password,
        )
    elif args.command == "init":
        from ._init_instance import init

        result = init(
            storage=args.storage,
            db=args.db,
            schema=args.schema,
            name=args.name,
        )
        return process_result(result)
    elif args.command == "load":
        from ._load_instance import load

        result = load(
            identifier=args.instance,
            storage=args.storage,
        )
        return process_result(result)
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
    elif args.command == "track":
        from ._notebook import track

        track(args.filepath, args.pypackage)

    else:
        parser.print_help()
    return -1
