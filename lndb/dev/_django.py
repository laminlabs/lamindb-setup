import builtins
import os

from ._settings_instance import InstanceSettings

is_run_from_ipython = getattr(builtins, "__IPYTHON__", False)


def setup_django(isettings: InstanceSettings):
    if is_run_from_ipython:
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
    import dj_database_url
    import django
    from django.conf import settings

    default_db = dj_database_url.config(
        default=isettings.db,
        conn_max_age=600,
        conn_health_checks=True,
    )
    DATABASES = {
        "default": default_db,
    }

    print("setting up django")

    if not settings.configured:
        settings.configure(
            INSTALLED_APPS=[
                "lnschema_core",
            ],
            DATABASES=DATABASES,
        )
        django.setup(set_prefix=False)
    # else:
    #     raise RuntimeError(
    #         "Please restart Python session, django doesn't currently support "
    #         "switching among instances in one session"
    #     )
