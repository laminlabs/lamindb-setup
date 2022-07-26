import argparse

from lamin_logger import logger

from . import _setup_instance, _setup_user
from ._settings_instance import description

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
aa("--email", type=str, metavar="s", default=None, help=description.user_email)
aa("--secret", type=str, metavar="s", default=None, help=description.user_secret)
login = subparsers.add_parser("login", help=login_help)
aa = login.add_argument
aa("--email", type=str, metavar="s", default=None, help=description.user_email)
aa("--secret", type=str, metavar="s", default=None, help=description.user_secret)
# instance settings
init = subparsers.add_parser("init", help=init_help)
aa = init.add_argument
aa("--storage", type=str, metavar="s", default=None, help=description.storage_dir)
aa("--db", type=str, metavar="s", default="sqlite", help=description._dbconfig)
aa("--schema", type=str, metavar="s", default=None, help=description.schema_modules)
load = subparsers.add_parser("load", help=load_help)
aa = load.add_argument
aa("--name", type=str, metavar="s", default=None, help=description.instance_name)
args = parser.parse_args()


def main():
    if args.command == "signup":
        return _setup_user.sign_up_user(
            email=args.email,
        )
    if args.command == "login":
        return _setup_user.log_in_user(
            email=args.email,
            secret=args.secret,
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
