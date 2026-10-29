"""Разбор .env: кавычки, export, комментарии, приоритет окружения."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from scanner.env import load_dotenv


def test_env_loader(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    body = "\n".join(
        [
            "# комментарий",
            "",
            "CRYPTODUMP_TEST_A=plain",
            'export CRYPTODUMP_TEST_B="quoted value"',
            "CRYPTODUMP_TEST_C='single'",
            "CRYPTODUMP_TEST_D=inline # пояснение",
            "CRYPTODUMP_TEST_E=",
            "CRYPTODUMP_TEST_F=already_set_in_file",
        ]
    )
    path = tmp_path / ".env"
    path.write_text(body, encoding="utf-8")

    for name in "ABCDE":
        monkeypatch.delenv(f"CRYPTODUMP_TEST_{name}", raising=False)
    monkeypatch.setenv("CRYPTODUMP_TEST_F", "from_process")

    try:
        applied = load_dotenv(str(path))
        assert os.environ["CRYPTODUMP_TEST_A"] == "plain"
        assert os.environ["CRYPTODUMP_TEST_B"] == "quoted value"
        assert os.environ["CRYPTODUMP_TEST_C"] == "single"
        assert os.environ["CRYPTODUMP_TEST_D"] == "inline"
        assert os.environ["CRYPTODUMP_TEST_E"] == ""
        # переменная процесса не перетирается файлом
        assert os.environ["CRYPTODUMP_TEST_F"] == "from_process"
        assert "CRYPTODUMP_TEST_F" not in applied
    finally:
        # load_dotenv пишет в os.environ напрямую, мимо monkeypatch
        for name in "ABCDE":
            os.environ.pop(f"CRYPTODUMP_TEST_{name}", None)
