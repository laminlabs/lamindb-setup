from ._settings import settings


def info():
    """Log information about current instance."""
    # Accessing cached settings is faster than accessing the hub
    print(settings.user)
    print(settings.instance)
