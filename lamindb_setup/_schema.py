from django.urls import path
from schema_graph.views import Schema

urlpatterns = [
    path("schema/", Schema.as_view()),
]


def view():
    from .dev.django import setup_django
    from ._settings import settings
    from ._check_instance_setup import check_instance_setup
    from django.core.management import call_command

    if check_instance_setup():
        raise RuntimeError("Restart Python session to create migration or use CLI!")
    setup_django(settings.instance, view_schema=True)
    call_command("runserver")
