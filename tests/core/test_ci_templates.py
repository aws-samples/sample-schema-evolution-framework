"""Keep the CI templates in ci/ in sync with the package."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = sorted((ROOT / "ci").rglob("*.yml"))
SCRIPTS = set(tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["scripts"])


def test_templates_exist_for_both_platforms() -> None:
    names = {p.relative_to(ROOT / "ci").parts[0] for p in TEMPLATES}
    assert names == {"github", "gitlab"}


@pytest.mark.parametrize("path", TEMPLATES, ids=lambda p: str(p.relative_to(ROOT)))
def test_template_parses_and_uses_real_commands(path: Path) -> None:
    text = path.read_text()
    assert isinstance(yaml.safe_load(text), dict)
    used = set(re.findall(r"\b(schema-evolution-[a-z-]+)\b", text)) - {"schema-evolution-toolkit"}
    used = {u for u in used if not u.startswith("schema-evolution-applier")}
    assert used, "template calls no toolkit command"
    assert used <= SCRIPTS, f"unknown commands: {used - SCRIPTS}"


@pytest.mark.parametrize("path", TEMPLATES, ids=lambda p: str(p.relative_to(ROOT)))
def test_template_validates_before_applying(path: Path) -> None:
    text = path.read_text()
    if "schema-evolution-apply" in text:
        assert text.index("schema-evolution-validate") < text.rindex("schema-evolution-apply")
