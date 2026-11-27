"""Errors raised while reverse-engineering a plan from an instance."""

from __future__ import annotations

from ..core import LaminSchemasError

__all__ = ["IntrospectionError"]


class IntrospectionError(LaminSchemasError):
    """Raised when an instance object cannot be exactly reconstructed as a plan.

    Introspection is strict: rather than emit a plan that would not faithfully
    round-trip, it collects every object it cannot reconstruct and raises this
    error listing them.
    """
