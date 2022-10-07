import argparse

from lamin_logger import logger

from . import _setup_instance, _setup_user
from ._settings_instance import instance_description as instance
from ._settings_user import user_description as user

signup_help = "First time sign up."
login_help = "Log in an already-signed-up user."
init_help = "Init & config instance with db & storage."
load_help = "Load instance by name."

description_cli = "Configure LaminDB and perform simple actions."
parser = argparse.ArgumentParser(
    description=description_cli, formatter_class=argparse.RawTextHelpFormatter
)
subparsers = parser.add_subparsers(dest="command")
# user settings
signup = subparsers.add_parser("signup", help=signup_help)
aa = signup.add_argument
aa("email", type=str, metavar="email", help=user.email)
login = subparsers.add_parser("login", help=login_help)
aa = login.add_argument
aa(
    "user",
    type=str,
    metavar="user",
    help="Email or user handle. Email is needed at first login.",
)  # noqa
aa("--password", type=str, metavar="s", default=None, help=user.password)
# instance settings
init = subparsers.add_parser("init", help=init_help)
aa = init.add_argument
aa("--storage", type=str, metavar="s", help=instance.storage_dir)
aa("--schema", type=str, metavar="s", default=None, help=instance.schema_modules)
aa("--db-type", type=str, metavar="s", default="sqlite", help=instance._db_type)
aa(
    "--db-cloud-provider",
    type=str,
    metavar="s",
    default=None,
    help=instance._db_cloud_provider,
)
aa("--db-connection-string", type=str, metavar="s", help=instance._db_connection_string)
aa("--db-host", type=str, metavar="s", help=instance._db_host)
aa("--db-port", type=str, metavar="s", help=instance._db_port)
aa("--db-name", type=str, metavar="s", help=instance._db_name)
load = subparsers.add_parser("load", help=load_help)
aa = load.add_argument
aa("instance", type=str, metavar="s", default=None, help=instance.name)
args = parser.parse_args()


def main():
    if args.command == "signup":
        return _setup_user.signup(email=args.email)
    if args.command == "login":
        return _setup_user.login(
            args.user,
            password=args.password,
        )
    elif args.command == "init":
        return _setup_instance.init(
            storage=args.storage,
            schema=args.schema,
            db_type=args.db_type,
            db_cloud_provider=args.db_cloud_provider,
            db_connection_string=args.db_connection_string,
            db_host=args.db_host,
            db_port=args.db_port,
            db_name=args.db_name,
        )
    elif args.command == "load":
        return _setup_instance.load(
            instance_name=args.instance,
        )
    else:
        logger.error("Invalid command. Try `lndb -h`.")
        return 1
