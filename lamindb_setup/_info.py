from ._settings import settings


def info():
    """Log information about current instance & user."""
    # Accessing cached settings is faster than accessing the hub
    print(settings.user)
    if settings._instance_exists:
        print(settings.instance)
    else:
        print("No instance loaded: lamin load <instance>")
