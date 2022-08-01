import argparse

from lamin_logger import logger

from . import _setup_instance, _setup_user
from ._settings_instance import instance_description as instance
from ._settings_user import user_description as user

signup_help = "First time sign up & log in after email is confirmed."
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
aa("email", type=str, metavar="s", help=user.email)
aa("--handle", type=str, metavar="s", default=None, help=user.handle)
login = subparsers.add_parser("login", help=login_help)
aa = login.add_argument
aa("user", type=str, metavar="s", help="Email or user handle.")
aa("--password", type=str, metavar="s", default=None, help=user.password)
# instance settings
init = subparsers.add_parser("init", help=init_help)
aa = init.add_argument
aa("--storage", type=str, metavar="s", default=None, help=instance.storage_dir)
aa("--db", type=str, metavar="s", default="sqlite", help=instance._dbconfig)
aa("--schema", type=str, metavar="s", default=None, help=instance.schema_modules)
load = subparsers.add_parser("load", help=load_help)
aa = load.add_argument
aa("--name", type=str, metavar="s", default=None, help=instance.name)
args = parser.parse_args()


def main():
    if args.command == "signup":
        if args.handle is None:
            response = input(
                "Do you want to provide a unique user handle like on Twitter or"
                " GitHub?\nType your desired handle or abort with 'n'."
            )
            if response == "n":
                handle = None
            else:
                handle = response
        return _setup_user.sign_up_user(
            email=args.email,
            handle=handle,
        )
    if args.command == "login":
        if "@" in args.user:
            email, handle = args.user, None
        else:
            email, handle = None, args.user
        return _setup_user.log_in_user(
            email=email,
            handle=handle,
            password=args.password,
        )
    elif args.command == "init":
        return _setup_instance.init_instance(
            storage=args.storage,
            dbconfig=args.db,
            schema=args.schema,
        )
    elif args.command == "load":
        return _setup_instance.load_instance(
            instance_name=args.name,
        )
    else:
        logger.error("Invalid command. Try `lndb -h`.")
        return 1
