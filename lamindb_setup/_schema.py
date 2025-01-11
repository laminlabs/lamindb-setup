from __future__ import annotations

from django.urls import path
from lamin_utils import logger

try:
    from schema_graph.views import Schema
except ImportError:
    logger.error("to view the schema: pip install django-schema-graph")


urlpatterns = [
    path("schema/", Schema.as_view()),
]


def view():
    from django.core.management import call_command

    from ._check_setup import _check_instance_setup
    from .core._settings import settings
    from .core.django import setup_django

    if _check_instance_setup():
        raise RuntimeError("Restart Python session or use CLI!")
    setup_django(settings.instance, view_schema=True)
    call_command("runserver")
