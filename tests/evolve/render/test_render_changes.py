"""The terminal diff hides unchanged objects unless asked to list them."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import pytest
from lamindb_setup.evolve.core import (
    Action,
    ChangeItem,
    ChangeWarning,
    UidMisalignment,
    WarningCategory,
)
from lamindb_setup.evolve.render import (
    is_up_to_date,
    print_changes,
    render_changes,
    render_changes_json,
    render_changes_markdown,
)

UP_TO_DATE = "No changes. Instance is up to date."


@dataclass
class _Changes:
    changes: list[ChangeItem] = field(default_factory=list)


def _change(
    name: str,
    action: Action = Action.NO_CHANGE,
    *,
    kind: str = "Feature",
    **extra,
) -> ChangeItem:
    return ChangeItem(
        obj_id=id(name),
        key=f"{kind}:{name}",
        kind=kind,
        name=name,
        action=action,
        uid=f"{name:x<12}",
        **extra,
    )


def _probe(*changes: ChangeItem) -> _Changes:
    """The issue's plan: one feature updated among five unchanged objects."""
    return _Changes(
        [
            _change("probe_ft", kind="FeatureType"),
            _change("probe_a"),
            _change("probe_b", Action.UPDATE, details=["dtype 'str' \u2192 'int'"]),
            _change("ProbeSchema", kind="Schema"),
            _change("ProbeRT", kind="RecordType"),
            _change("ProbeSheet", kind="Sheet"),
            *changes,
        ]
    )


_OBJECT_LINE = re.compile(r"^  [ +~\-\u21aa\u2192\u2190] \w+ '([^']*)'  \S+$", re.M)


def _listed(text: str) -> list[str]:
    """The names of the objects a terminal diff lists, in order."""
    return _OBJECT_LINE.findall(text)


def test_the_terminal_diff_lists_only_what_changes() -> None:
    text = render_changes(_probe(), title="Plan: probe")

    assert _listed(text) == ["probe_b"]
    assert (
        "  ~ Feature 'probe_b'  probe_bxxxxx\n        dtype 'str' \u2192 'int'" in text
    )
    assert text.endswith("Plan: 0 to create, 1 to update, 0 to delete, 5 unchanged\n")
    assert UP_TO_DATE not in text


def test_show_unchanged_lists_every_object() -> None:
    text = render_changes(_probe(), title="Plan: probe", show_unchanged=True)

    assert _listed(text) == [
        "probe_ft",
        "probe_a",
        "probe_b",
        "ProbeSchema",
        "ProbeRT",
        "ProbeSheet",
    ]
    assert "    FeatureType 'probe_ft'  probe_ftxxxx" in text
    assert "5 unchanged" in text


@pytest.mark.parametrize(
    "flag",
    [
        {
            "uid_misalignment": UidMisalignment(
                uid="S1", others={"acme-general": "G1"}, reason="uid S1 here"
            )
        },
        {"warnings": [ChangeWarning("look here", WarningCategory.CAVEAT)]},
    ],
    ids=["misaligned", "warned"],
)
def test_an_unchanged_object_with_something_to_flag_is_listed(flag) -> None:
    ctx = _probe(_change("flagged", **flag))

    text = render_changes(ctx, title="Plan: probe")

    assert _listed(text) == ["probe_b", "flagged"]
    assert "    Feature 'flagged'  flaggedxxxxx\n      " in text
    assert "6 unchanged" in text


def test_a_borrowed_object_the_instance_has_is_hidden_as_unchanged() -> None:
    borrowed = _change("dep", borrowed=True, details=["managed by a prerequisite plan"])

    hidden = render_changes(_probe(borrowed), title="Plan: probe")
    shown = render_changes(_probe(borrowed), title="Plan: probe", show_unchanged=True)

    assert "dep" not in _listed(hidden)
    assert "managed by a prerequisite plan" not in hidden
    assert (
        "    Feature 'dep'  depxxxxxxxxx\n        managed by a prerequisite plan"
        in (shown)
    )


def test_a_plan_without_changes_says_the_instance_is_up_to_date() -> None:
    ctx = _Changes([_change("probe_ft", kind="FeatureType"), _change("probe_a")])

    text = render_changes(ctx, title="Plan: probe")
    listed = render_changes(ctx, title="Plan: probe", show_unchanged=True)

    assert is_up_to_date(ctx)
    assert text == (
        "Plan: probe\n"
        "\n"
        "Plan: 0 to create, 0 to update, 0 to delete, 2 unchanged\n"
        f"{UP_TO_DATE}\n"
    )
    assert _listed(listed) == ["probe_ft", "probe_a"]
    assert listed.endswith(f"2 unchanged\n{UP_TO_DATE}\n")


@pytest.mark.parametrize(
    "change",
    [
        _change("moved", Action.TRANSFER),
        _change(
            "flagged",
            uid_misalignment=UidMisalignment(uid="S1", others={}, reason="taken"),
        ),
        _change("created", Action.CREATE, borrowed=True),
    ],
    ids=["transfer", "misaligned", "borrowed-create"],
)
def test_an_instance_with_something_to_show_is_not_up_to_date(
    change: ChangeItem,
) -> None:
    ctx = _Changes([_change("probe_a"), change])

    assert not is_up_to_date(ctx)
    assert UP_TO_DATE not in render_changes(ctx, title="Plan: probe")


def test_print_changes_hides_unchanged_objects_unless_asked(capsys) -> None:
    print_changes(_probe(), title="Plan: probe")
    hidden = capsys.readouterr().out
    print_changes(_probe(), title="Plan: probe", err=True, show_unchanged=True)
    shown = capsys.readouterr().err

    assert "probe_a" not in hidden
    assert "probe_b" in hidden
    assert "probe_a" in shown


def test_markdown_and_json_ignore_the_terminal_default() -> None:
    misaligned = UidMisalignment(uid="S1", others={}, reason="taken")
    ctx = _probe(_change("flagged", uid_misalignment=misaligned))

    markdown = render_changes_markdown(ctx, title="Plan: probe")
    document = render_changes_json(ctx, title="Plan: probe")

    assert "Feature 'probe_a'" not in markdown
    assert "~ Feature 'probe_b'" in markdown
    assert "  Feature 'flagged'" in markdown
    assert '"name": "probe_a"' in document
    assert '"name": "ProbeSheet"' in document
