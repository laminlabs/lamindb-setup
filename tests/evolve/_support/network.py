"""Keep evolution tests on the local machine."""

import ipaddress


def _is_local_host(host: object) -> bool:
    if host is None:
        return True  # getaddrinfo(None, ...) resolves the local machine
    name = host.decode() if isinstance(host, bytes) else str(host)
    if name == "localhost":
        return True
    try:
        return ipaddress.ip_address(name).is_loopback
    except ValueError:
        return False


def guard_network(event: str, args: tuple[object, ...]) -> None:
    if event == "socket.getaddrinfo" and not _is_local_host(args[0]):
        raise OSError(f"name resolution refused in tests: {args[0]}")
    # AF_UNIX sockets connect to a path, not a (host, port, ...) tuple.
    address = args[1] if event == "socket.connect" else None
    if isinstance(address, tuple) and not _is_local_host(address[0]):
        raise ConnectionRefusedError(f"network access refused in tests: {address}")
