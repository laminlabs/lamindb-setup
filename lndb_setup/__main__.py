import argparse

from lamin_logger import logger

from . import _init_instance, _setup_user, delete, info, set_storage
from ._close import close as close_instance
from ._init_instance import description as instance
from ._settings_user import user_description as user

signup_help = "First time sign up."
login_help = "Log in an already-signed-up user."
init_help = "Init & config instance with db & storage."
load_help = "Load instance by name."
set_storage_help = "Set storage used by an instance."
info_help = "Show current instance information."
close_help = "Close instance."
migr_help = "Manage migrations."
delete_help = "Delete an instance."

description_cli = "Configure LaminDB and perform simple actions."
parser = argparse.ArgumentParser(
    description=description_cli, formatter_class=argparse.RawTextHelpFormatter
)
subparsers = parser.add_subparsers(dest="command")

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
aa("--password", type=str, metavar="s", default=None, help=user.password)

# init instance
init = subparsers.add_parser("init", help=init_help)
aa = init.add_argument
aa("--storage", type=str, metavar="s", help=instance.storage_root)
aa("--db", type=str, metavar="s", default=None, help=instance.db)
aa("--schema", type=str, metavar="s", default=None, help=instance.schema)
aa("--name", type=str, metavar="s", default=None, help=instance.name)

# load instance
load = subparsers.add_parser("load", help=load_help)
aa = load.add_argument
instance_help = """
The instance identifier can the instance name (owner is
current user), handle/name, or the URL: https://lamin.ai/handle/name."""
aa("instance", type=str, metavar="s", default=None, help=instance_help)

# delete instance
delete_parser = subparsers.add_parser("delete", help=delete_help)
aa = delete_parser.add_argument
aa("instance", type=str, metavar="s", default=None, help=instance.name)

# show instance info
info_parser = subparsers.add_parser("info", help=info_help)

# set storage
set_storage_parser = subparsers.add_parser("set", help=set_storage_help)
aa = set_storage_parser.add_argument
aa("--storage", type=str, metavar="s", help=instance.storage_root)

# close instance
close = subparsers.add_parser("close", help=close_help)

# migrate
migr = subparsers.add_parser("migrate", help=migr_help)
aa = migr.add_argument
aa("action", choices=["generate"], help="Generate migration.")

# parse args
args = parser.parse_args()


def process_result(result):
    if result in ["migrate-unnecessary", "migrate-success", "migrate-skipped", None]:
        return None  # is interpreted as success (exit code 0) by shell
    else:
        return result


def main():
    if args.command == "signup":
        return _setup_user.signup(email=args.email)
    if args.command == "login":
        return _setup_user.login(
            args.user,
            password=args.password,
        )
    elif args.command == "init":
        result = _init_instance.init(
            storage=args.storage, db=args.db, schema=args.schema, name=args.name
        )
        return process_result(result)
    elif args.command == "load":
        result = _init_instance.load(
            identifier=args.instance,
        )
        return process_result(result)
    elif args.command == "close":
        return close_instance()
    elif args.command == "delete":
        return delete(
            instance_name=args.instance,
        )
    elif args.command == "info":
        return info()
    elif args.command == "set":
        return set_storage(storage=args.storage)
    elif args.command == "migrate":
        from . import migrate

        if args.action == "generate":
            return migrate.generate()
    else:
        logger.error("Invalid command. Try `lndb -h`.")
        return 1
