from __future__ import annotations


def check():
    from django.core.management import call_command

    call_command("check")
