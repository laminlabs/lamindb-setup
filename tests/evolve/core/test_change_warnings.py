# Adapted source tests include deliberately invalid declarations and dynamic models.
# mypy: disable-error-code="arg-type"
# Source test semantics retained.
"""Warning categories, and the banners the change renderers derive from them."""

from __future__ import annotations

import copy
import json
import pickle
from dataclasses import dataclass, field

import pytest
from lamindb_setup.evolve.core import (
    Action,
    ChangeItem,
    ChangeWarning,
    WarningCategory,
    warning_category,
)
from lamindb_setup.evolve.render import (
    render_changes,
    render_changes_json,
    render_changes_markdown,
)

DRIFT_BANNER = (
    "object(s) changed outside the plan and will be auto-corrected to the plan; "
    "review the WARNING lines above."
)
RENAME_BANNER = (
    "change(s) look like a rename not declared with ctx.moved; review the "
    "WARNING lines above."
)
PREREQUISITE_BANNER = (
    "object(s) are owned by a prerequisite plan that is not applied to this instance."
)

_TEXTS = {
    WarningCategory.DRIFT: (
        "remote changed outside the plan and will be auto-corrected to the plan: "
        "description: instance now 'x', lock recorded 'y'"
    ),
    WarningCategory.RENAME: "looks like a rename of deleted Feature 'old'",
    WarningCategory.PREREQUISITE: (
        "owned by a prerequisite plan that has not been applied to this instance"
    ),
    WarningCategory.CAVEAT: "flexible False \u2192 True: matched by name alone",
}

# The banner each category gets, `None` for one that has none.
_BANNERS = {
    WarningCategory.DRIFT: DRIFT_BANNER,
    WarningCategory.RENAME: RENAME_BANNER,
    WarningCategory.PREREQUISITE: PREREQUISITE_BANNER,
    WarningCategory.CAVEAT: None,
}


@dataclass
class _Changes:
    changes: list[ChangeItem] = field(default_factory=list)


def _warning(category: WarningCategory) -> ChangeWarning:
    return ChangeWarning(_TEXTS[category], category)


def _change(index: int, *warnings: str, action: Action = Action.UPDATE) -> ChangeItem:
    return ChangeItem(
        obj_id=index,
        key=f"Feature:f{index}",
        kind="Feature",
        name=f"f{index}",
        action=action,
        uid=f"uid{index:011d}",
        warnings=list(warnings),
    )


def _render(changes: list[ChangeItem]) -> tuple[str, str]:
    ctx = _Changes(changes)
    return render_changes(ctx, title="Plan: t"), render_changes_markdown(
        ctx, title="Plan: t"
    )


# === ChangeWarning ===========================================================


def test_a_change_warning_is_its_text() -> None:
    warning = ChangeWarning("looks like a rename", WarningCategory.RENAME)

    assert warning == "looks like a rename"
    assert "a rename" in warning
    assert warning.startswith("looks")
    assert f"WARNING: {warning}" == "WARNING: looks like a rename"
    assert json.dumps([warning]) == '["looks like a rename"]'
    assert warning.category is WarningCategory.RENAME
    assert warning_category(warning) is WarningCategory.RENAME


@pytest.mark.parametrize(
    "duplicate",
    [copy.copy, copy.deepcopy, lambda w: pickle.loads(pickle.dumps(w))],
    ids=["copy", "deepcopy", "pickle"],
)
def test_a_copied_change_warning_keeps_its_category(duplicate) -> None:
    warning = ChangeWarning("owned by a prerequisite", WarningCategory.PREREQUISITE)

    duplicated = duplicate(warning)

    assert isinstance(duplicated, ChangeWarning)
    assert duplicated == warning
    assert duplicated.category is WarningCategory.PREREQUISITE


def test_a_plain_string_warning_is_read_as_drift() -> None:
    # What every warning was read as before warnings had a category.
    assert warning_category("changed outside the plan") is WarningCategory.DRIFT


# === banners =================================================================


@pytest.mark.parametrize("category", list(WarningCategory), ids=lambda c: c.value)
def test_each_category_gets_its_own_banner_or_none(
    category: WarningCategory,
) -> None:
    terminal, markdown = _render([_change(1, _warning(category))])

    banner = _BANNERS[category]
    others = [b for b in _BANNERS.values() if b is not None and b != banner]
    assert f"! WARNING: {_TEXTS[category]}" in terminal
    assert f"! WARNING: {_TEXTS[category]}" in markdown
    if banner is None:
        assert "[!WARNING]" not in markdown
    else:
        assert f"! 1 {banner}" in terminal
        assert f"> [!WARNING]\n> 1 {banner}" in markdown
    for other in others:
        assert other not in terminal
        assert other not in markdown


def test_an_undeclared_rename_is_not_announced_as_drift() -> None:
    terminal, markdown = _render(
        [
            _change(1, action=Action.DELETE),
            _change(2, _warning(WarningCategory.RENAME), action=Action.CREATE),
        ]
    )

    for rendered in (terminal, markdown):
        assert f"1 {RENAME_BANNER}" in rendered
        assert "changed outside the plan" not in rendered


def test_a_missing_prerequisite_is_not_announced_as_drift() -> None:
    terminal, markdown = _render(
        [
            _change(i, _warning(WarningCategory.PREREQUISITE), action=Action.CREATE)
            for i in (1, 2)
        ]
    )

    for rendered in (terminal, markdown):
        assert f"2 {PREREQUISITE_BANNER}" in rendered
        assert "changed outside the plan" not in rendered


def test_banners_count_the_changes_carrying_each_category() -> None:
    drift = _warning(WarningCategory.DRIFT)
    terminal, markdown = _render(
        [
            # Two drift warnings on one object still count it once.
            _change(1, drift, ChangeWarning("another drift", WarningCategory.DRIFT)),
            _change(2, drift, _warning(WarningCategory.CAVEAT)),
            _change(3, _warning(WarningCategory.RENAME), action=Action.CREATE),
            _change(4, _warning(WarningCategory.PREREQUISITE), action=Action.CREATE),
            _change(5, _warning(WarningCategory.CAVEAT)),
        ]
    )

    assert (
        f"! 2 {DRIFT_BANNER}\n! 1 {RENAME_BANNER}\n! 1 {PREREQUISITE_BANNER}\n"
    ) in terminal
    # One callout, a paragraph per category.
    assert markdown.count("[!WARNING]") == 1
    assert (
        f"> [!WARNING]\n> 2 {DRIFT_BANNER}\n>\n> 1 {RENAME_BANNER}\n>\n"
        f"> 1 {PREREQUISITE_BANNER}\n"
    ) in markdown


def test_no_warnings_no_banner() -> None:
    terminal, markdown = _render([_change(1)])

    assert "! " not in terminal
    assert "[!WARNING]" not in markdown


def test_json_gives_each_warning_its_category() -> None:
    ctx = _Changes(
        [
            _change(
                1, _warning(WarningCategory.DRIFT), _warning(WarningCategory.CAVEAT)
            ),
            _change(2, _warning(WarningCategory.RENAME), action=Action.CREATE),
            _change(3, _warning(WarningCategory.PREREQUISITE), action=Action.CREATE),
            _change(4, "a hand-built warning"),
            _change(5),
        ]
    )

    payload = json.loads(render_changes_json(ctx, title="Plan: t"))

    entries = {entry["name"]: entry for entry in payload["changes"]}
    assert entries["f1"]["warnings"] == [
        _TEXTS[WarningCategory.DRIFT],
        _TEXTS[WarningCategory.CAVEAT],
    ]
    assert entries["f1"]["warning_categories"] == ["drift", "caveat"]
    assert entries["f2"]["warning_categories"] == ["rename"]
    assert entries["f3"]["warning_categories"] == ["prerequisite"]
    assert entries["f4"]["warning_categories"] == ["drift"]
    assert entries["f5"]["warnings"] == []
    assert entries["f5"]["warning_categories"] == []
