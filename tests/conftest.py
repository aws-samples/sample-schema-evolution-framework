"""Shared pytest configuration.

Spark and CDK tests are skipped automatically when their optional extras
(``[spark]`` / ``[infra]``) are not installed, so ``pip install -e .`` plus
``pytest`` always runs the core suite.
"""

from __future__ import annotations

import importlib.util
import sys

import pytest

if importlib.util.find_spec("schema_evolution") is None:
    raise pytest.UsageError(
        f"The toolkit is not installed for {sys.executable}.\n"
        "Activate its virtualenv first (source .venv/bin/activate), or install it with:\n"
        '    pip install -e ".[dev]"'
    )

collect_ignore_glob: list[str] = []

if importlib.util.find_spec("pyspark") is None:
    collect_ignore_glob.append("spark/*")

if importlib.util.find_spec("aws_cdk") is None:
    collect_ignore_glob.append("infra/*")
