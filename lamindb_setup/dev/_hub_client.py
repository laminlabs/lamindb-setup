# see the overlap with connector.py in laminhub-rest
import os
from typing import Optional
from lamin_utils import logger
from supabase.lib.client_options import ClientOptions
from urllib.request import urlretrieve
from supabase import create_client, Client
from pydantic import BaseSettings
from postgrest import APIError as PostgrestAPIError
from gotrue.errors import AuthUnknownError


class Connector(BaseSettings):
    url: str
    key: str


def load_fallback_connector() -> Connector:
    url = "https://lamin-site-assets.s3.amazonaws.com/connector.env"
    connector_file, _ = urlretrieve(url)
    connector = Connector(_env_file=connector_file)
    return connector


PROD_URL = "https://hub.lamin.ai"
PROD_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxhZXNhdW1tZHlkbGxwcGdmY2h1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE2NTY4NDA1NTEsImV4cCI6MTk3MjQxNjU1MX0.WUeCRiun0ExUxKIv5-CtjF6878H8u26t0JmCWx3_2-c"  # noqa
PROD_HUB_REST_SERVER_URL = (
    "https://laminhub-rest-cloud-run-prod-xv4y7p4gqa-uc.a.run.app"
)


class Environment:
    def __init__(self, fallback: bool = False):
        lamin_env = os.getenv("LAMIN_ENV")
        if lamin_env is None:
            lamin_env = "prod"
        # set public key
        if lamin_env == "prod":
            if not fallback:
                url = PROD_URL
                key = PROD_KEY
            else:
                connector = load_fallback_connector()
                url = connector.url
                key = connector.key
            hub_rest_server_url = PROD_HUB_REST_SERVER_URL
        elif lamin_env == "staging":
            url = "https://amvrvdwndlqdzgedrqdv.supabase.co"
            key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFtdnJ2ZHduZGxxZHpnZWRycWR2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE2NzcxNTcxMzMsImV4cCI6MTk5MjczMzEzM30.Gelt3dQEi8tT4j-JA36RbaZuUvxRnczvRr3iyRtzjY0"  # noqa
            hub_rest_server_url = (
                "https://laminhub-rest-cloud-run-staging-xv4y7p4gqa-uc.a.run.app"
            )
        else:
            url = os.environ["SUPABASE_API_URL"]
            key = os.environ["SUPABASE_ANON_KEY"]
            hub_rest_server_url = os.environ.get("LAMIN_HUB_REST_SERVER_URL", None)  # type: ignore  # noqa
        self.lamin_env: str = lamin_env
        self.supabase_api_url: str = url
        self.supabase_anon_key: str = key
        self.hub_rest_server_url: str = hub_rest_server_url


# runs ~0.5s
def connect_hub(
    fallback_env: bool = False, client_options: ClientOptions = ClientOptions()
) -> Client:
    env = Environment(fallback=fallback_env)
    return create_client(env.supabase_api_url, env.supabase_anon_key, client_options)


def connect_hub_with_auth(
    fallback_env: bool = False, renew_token: bool = False
) -> Client:
    from lamindb_setup import settings

    hub = connect_hub(fallback_env=fallback_env)
    if renew_token:
        settings.user.access_token = get_access_token(
            settings.user.email, settings.user.password
        )
    hub.postgrest.auth(settings.user.access_token)
    return hub


# runs ~0.5s
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


def call_with_fallback_auth(
    callable,
    **kwargs,
):
    for renew_token, fallback_env in [(False, False), (True, False), (False, True)]:
        try:
            client = connect_hub_with_auth(
                renew_token=renew_token, fallback_env=fallback_env
            )
            result = callable(**kwargs, client=client)
            break
        except (PostgrestAPIError, AuthUnknownError) as e:
            if fallback_env:
                raise e
        finally:
            client.auth.sign_out()
    return result


def call_with_fallback(
    callable,
    **kwargs,
):
    for fallback_env in [False, True]:
        try:
            client = connect_hub(fallback_env=fallback_env)
            result = callable(**kwargs, client=client)
            break
        except AuthUnknownError as e:
            if fallback_env:
                raise e
        finally:
            # in case there was sign in
            client.auth.sign_out()
    return result
