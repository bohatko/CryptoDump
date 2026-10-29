"""Сборка конфига из JSON: пороги окон, мусорные значения, вложенный telegram."""

from __future__ import annotations

import logging
from dataclasses import asdict
from pathlib import Path

import pytest

from scanner.config import Config, config_from_dict, load_config, merge_config

EXAMPLE = Path(__file__).resolve().parent.parent / "scanner" / "config.example.json"


def test_example_config_matches_defaults(caplog: pytest.LogCaptureFixture) -> None:
    """config.example.json не должен расходиться с датаклассами: ни лишних ключей, ни других дефолтов."""
    with caplog.at_level(logging.WARNING, logger="scanner.config"):
        cfg = load_config(EXAMPLE)
    assert "неизвестные ключи" not in caplog.text, caplog.text

    loaded, defaults = asdict(cfg), asdict(Config())
    for data in (loaded, defaults):
        data.pop("source_path", None)
        data.pop("_thresholds_cache", None)
    assert loaded == defaults


def test_merge_config_replaces_windows_and_merges_sections() -> None:
    base = {"windows": {"1m": 1.5, "5m": 3.0}, "depth": {"levels": 20, "mode": "ondemand"}}
    merged = merge_config(base, {"windows": {"5m": 2.0}, "depth": {"mode": "top"}})
    assert merged == {"windows": {"5m": 2.0}, "depth": {"levels": 20, "mode": "top"}}
    assert base["depth"]["mode"] == "ondemand"  # исходник не мутирует


def test_mode_change_requires_restart() -> None:
    old = Config()
    assert "depth.mode" in old.restart_required_changes(config_from_dict({"depth": {"mode": "all"}}))
    assert "depth.mode" in old.restart_required_changes(config_from_dict({"depth": {"source": "rest"}}))
    assert old.restart_required_changes(config_from_dict({"depth": {"book_slots": 5}})) == []


def test_config_overrides() -> None:
    cfg = config_from_dict({"windows": {"1m": 2.5, "2h": 15}, "signals": {"min_score": 3}})
    assert cfg.window_thresholds() == [(60, 2.5), (7200, 15.0)]
    assert cfg.signals.min_score == 3
    assert cfg.max_window_s() == 7200

    # мусорные значения не должны ломать список окон
    cfg = config_from_dict({"windows": {"bogus": 5, "1m": 0, "5m": 3}})
    assert cfg.window_thresholds() == [(300, 3.0)]


def test_telegram_config_nesting() -> None:
    """alerts.telegram из JSON должен собираться во вложенный датакласс."""
    cfg = config_from_dict(
        {
            "alerts": {
                "sinks": ["console", "telegram"],
                "telegram": {"enabled": True, "chat_ids": ["-100123"], "min_interval_s": 2.5},
            }
        }
    )
    assert cfg.alerts.sinks == ["console", "telegram"]
    assert cfg.alerts.telegram.chat_ids == ["-100123"]
    assert cfg.alerts.telegram.min_interval_s == 2.5
    assert cfg.alerts.telegram.token_env == "TELEGRAM_BOT_TOKEN"
