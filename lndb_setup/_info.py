from ._settings import settings


def info():
    """Log information about current instance."""
    # Accessing cached settings is faster than accessing the hub
    usettings = settings.user
    print(f"User: handle={usettings.handle} email={usettings.email} id={usettings.id}")
    print(f"Instance: {settings.instance.owner}/{settings.instance.name}")
