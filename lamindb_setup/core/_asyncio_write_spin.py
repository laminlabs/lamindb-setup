"""Recovery from an asyncio write-error loop that stalls fsspec transfers.

Symptom
-------
A cloud transfer stops making progress and asyncio logs, endlessly and at a rate of
roughly 100k per second, one of::

    Exception in callback _SelectorSocketTransport._write_send()
    AssertionError: Data should not be empty

The messages and the stall are the same event: the fsspec IO loop is fully occupied
re-running a callback that can only fail, so no transfer on that loop can proceed.
Wrapping the transfer in `try` / `except` catches nothing, for the reason given under
"How the error surfaces" below.

Mechanism
---------
The socket in question is the TCP socket under one HTTPS connection to the storage
backend. A request of gcsfs or s3fs goes through an aiohttp session, whose connector
keeps a pool of such connections, and asyncio wraps each of them in a transport
identified to its selector by a file descriptor. One stuck descriptor is therefore
one connection, but all of them share the single IO loop of the filesystem, so a
descriptor that keeps firing starves every other transfer as well.

When asyncio writes to a socket and the kernel does not accept everything, the
transport keeps the remainder in a buffer and registers a writer callback for the
socket. The selector then invokes that callback whenever the socket is writable; the
callback drains the buffer and, once it is empty, unregisters itself. Unregistering
is the only thing that ends the cycle.

A connection that the peer closes while a request body is in flight can leave the
transport with an empty buffer while the writer stays registered. That state is
terminal, because the assertion is the first statement of the callback::

    def _write_send(self):
        assert self._buffer, 'Data should not be empty'
        if self._conn_lost:
            return
        ...
        if not self._buffer:
            self._loop._remove_writer(self._sock_fd)

The callback can never reach its own cleanup, and neither can anything else:
`close` unregisters the writer only for a non-empty buffer, and `_force_close`
returns early once the connection is marked lost, which by then it is.

Both write paths carry the race, `_write_sendmsg` and, where `sendmsg` is
unavailable, `_write_send`. Only selector loops are affected. fsspec runs its IO loop
on a background thread and gets a selector loop by default everywhere except on
Windows, where it takes a notebook to see this: ipykernel switches the event loop
policy away from the proactor loop because pyzmq needs `add_reader`.

How the error surfaces
----------------------
Every callback scheduled on a loop runs through `asyncio.events.Handle._run`, which
catches whatever escapes, builds a context of `message`, `exception` and `handle`,
and passes it to the exception handler of the loop::

    def _run(self):
        try:
            self._context.run(self._callback, *self._args)
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            ...
            msg = f'Exception in callback {cb}'
            context = {'message': msg, 'exception': exc, 'handle': self}

That is the only place producing an `Exception in callback ...` message, so a report
with that wording comes from here and carries a `handle`. It also explains why the
caller sees nothing to catch: the assertion never crosses the transfer coroutine, it
is swallowed here and turned into a log line.

The handle matters because this context has no `transport` key, unlike the ones that
`_fatal_error` produces. Its `_callback` is the bound `transport._write_send`, so
`__self__` is the only route from the report back to the object that needs repair.

Prior art
---------
Reported downstream twice without a diagnosis, as fsspec/gcsfs#604 (closed by the
reporter, who worked around it with a slower upload scheme) and fsspec/s3fs#909,
which pairs the assertion with the `ServerDisconnectedError` of the dropped
connection that triggers it. Upstream, python/cpython#115514 covers write-after-close
in the same transport, but its fix only stops the variant that loses `_write_ready`;
the empty-buffer spin still reproduces on 3.13.12.

Repair
------
`repair_spurious_write_errors` installs a loop exception handler that recognizes this
assertion and unregisters the stuck writer, which ends the spin. The request then
fails like any other dropped connection and the storage backend retries it, so a
permanent stall becomes a short pause. Everything that is not this assertion goes to
the default handler untouched.

The handler reaches into asyncio internals, `_sock_fd`, `loop._selector` and the
private `_remove_writer` (the public `remove_writer` refuses a descriptor owned by a
transport that is not closing yet). Those lookups are guarded, so on an asyncio that
no longer exposes them the handler degrades to doing nothing rather than breaking
transfers, and once CPython fixes the assertion ordering it stops finding anything to
repair.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fsspec.spec import AbstractFileSystem


def _stop_write_error_spin(loop, context) -> bool:
    """Unregister a socket writer that asyncio left behind with an empty write buffer.

    `context` is what asyncio passes to a loop exception handler, a dict describing a
    callback that raised. Returns whether it was the known spurious write error, in
    which case the spin has been repaired here and the caller should not log anything.

    Every lookup below is defensive because all of it is asyncio implementation
    detail: if a Python version stops providing it, we want to end up doing nothing
    instead of raising from inside the exception handler.
    """
    exc = context.get("exception")
    if not isinstance(exc, AssertionError) or "Data should not be empty" not in str(
        exc
    ):
        return False

    # `handle` is the asyncio.Handle that `Handle._run` put into the context when the
    # callback raised, and its `_callback` is the bound method `transport._write_send`,
    # cancelled handles and other contexts have none, so `__self__` gives us the transport
    # that is spinning: the wrapper around the TCP socket of a single pooled HTTPS
    # connection to the storage backend, not the session and not the loop
    callback = getattr(context.get("handle"), "_callback", None)
    transport = getattr(callback, "__self__", None)
    # the file descriptor of that socket, the key under which the loop registers
    # interest in read and write readiness with the selector
    fd = getattr(transport, "_sock_fd", None)
    if transport is None or fd is None:
        return True

    # a selector key holds `(reader, writer)` handles for the fd, the writer being the
    # registration that keeps re-running `_write_send` and that we need to drop
    try:
        writer = loop._selector.get_key(fd).data[1]
    except (AttributeError, KeyError, ValueError):
        # nothing is registered for the fd anymore, so there is no spin to stop
        return True
    # descriptors get recycled, so the fd may meanwhile belong to another connection,
    # and only the registration owned by this transport is the one that spins
    if getattr(getattr(writer, "_callback", None), "__self__", None) is not transport:
        return True

    # this is the repair: with the write registration gone the selector stops
    # reporting the socket writable, so the failing callback is never scheduled again
    # (the public `remove_writer` refuses fds owned by a transport that is not closing)
    loop._remove_writer(fd)
    if not transport._conn_lost:
        # nothing has told the protocol that this connection is gone, so the request
        # would keep waiting for a response that cannot arrive; aborting surfaces it
        # as a normal connection error that the storage backend retries
        transport.abort()
    return True


def repair_spurious_write_errors(fs: AbstractFileSystem) -> None:
    """Make the IO loop of `fs` recover from the write-error spin described above.

    Cheap to call repeatedly, an fsspec filesystem reuses one IO loop for its lifetime
    and the handler is installed at most once per loop.
    """
    loop = getattr(fs, "loop", None)
    # a filesystem without a loop is synchronous, and the proactor loop of Windows
    # outside Jupyter uses a different transport that does not carry the bug
    if not isinstance(loop, asyncio.SelectorEventLoop):
        return
    # never displace a handler somebody else installed, theirs may do more than log
    if loop.get_exception_handler() is not None:
        return

    def handler(loop, context):
        # the repair happens inside the call, the early return only keeps the
        # traceback of an error we just dealt with out of the output
        if _stop_write_error_spin(loop, context):
            return
        loop.default_exception_handler(context)

    loop.set_exception_handler(handler)
