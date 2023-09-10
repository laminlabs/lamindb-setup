# see the overlap with connector.py in laminhub-rest
import os
from typing import Optional
from lamin_utils import logger
from supabase.lib.client_options import ClientOptions

from supabase import create_client, Client


class Environment:
    def __init__(self):
        lamin_env = os.getenv("LAMIN_ENV")
        if lamin_env is None:
            lamin_env = "prod"
        # set public key
        if lamin_env == "prod":
            url = "https://laesaummdydllppgfchu.supabase.co"
            key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxhZXNhdW1tZHlkbGxwcGdmY2h1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE2NTY4NDA1NTEsImV4cCI6MTk3MjQxNjU1MX0.WUeCRiun0ExUxKIv5-CtjF6878H8u26t0JmCWx3_2-c"  # noqa
        elif lamin_env == "prod":
            url = "https://amvrvdwndlqdzgedrqdv.supabase.co"
            key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFtdnJ2ZHduZGxxZHpnZWRycWR2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE2NzcxNTcxMzMsImV4cCI6MTk5MjczMzEzM30.Gelt3dQEi8tT4j-JA36RbaZuUvxRnczvRr3iyRtzjY0"  # noqa
        else:
            url = os.environ["SUPABASE_API_URL"]
            key = os.environ["SUPABASE_ANON_KEY"]
        self.lamin_env: str = lamin_env
        self.supabase_api_url: str = url
        self.supabase_anon_key: str = key


def connect_hub(client_options: ClientOptions = ClientOptions()) -> Client:
    env = Environment()
    return create_client(env.supabase_api_url, env.supabase_anon_key, client_options)


def connect_hub_with_auth() -> Client:
    from lamindb_setup import settings

    hub = connect_hub()
    access_token = settings.user.access_token
    try:
        # token might be expired, hence, try-except
        hub.postgrest.auth(access_token)
        return hub
    except Exception:
        access_token = get_access_token(
            email=settings.user.email, password=settings.user.password
        )
        hub.postgrest.auth(access_token)
        return hub


def get_access_token(email: Optional[str] = None, password: Optional[str] = None):
    hub = connect_hub()
    try:
        auth_response = hub.auth.sign_in_with_password(
            {
                "email": email,
                "password": password,
            }
        )
        return auth_response.session.access_token
    except Exception as e:
        logger.error(
            f"Could not authenticate with email {email} and password {password}"
        )
        raise e
    finally:
        hub.auth.sign_out()
