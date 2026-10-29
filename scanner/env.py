"""Загрузка переменных окружения из ``.env`` без внешних зависимостей.

Приоритет у уже выставленных переменных процесса: файл лишь дополняет окружение,
поэтому в CI/докере значения можно передать напрямую и они не будут перетёрты.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)

DEFAULT_ENV_FILE = ".env"
PROJECT_ROOT = Path(__file__).resolve().parent.parent  # .../CryptoDump


def candidate_env_files(explicit: str | os.PathLike[str] | None = None) -> list[Path]:
    """Где искать ``.env``: явный путь → текущая папка → корень проекта.

    Сканер часто запускают из ``scanner/`` или из корня — второй вариант должен
    находить файл, даже если cwd другой.
    """
    if explicit:
        return [Path(explicit)]
    return [Path.cwd() / DEFAULT_ENV_FILE, PROJECT_ROOT / DEFAULT_ENV_FILE]


def load_dotenv(path: str | os.PathLike[str] | None = None, *, override: bool = False) -> dict[str, str]:
    """Прочитать ``.env`` и положить значения в ``os.environ``.

    Поддерживаются строки ``KEY=VALUE``, комментарии ``#``, ``export KEY=VALUE``,
    значения в одинарных/двойных кавычках и пустые строки.

    Возвращает словарь **только новых** для процесса переменных (то, что реально
    применилось) — удобно для диагностики, что подхватилось из файла.
    """
    applied: dict[str, str] = {}
    for file_path in candidate_env_files(path):
        if not file_path.is_file():
            if path:
                # путь указали явно — молчать о промахе нельзя
                log.warning(".env по указанному пути не найден: %s", file_path)
            else:
                log.debug(".env не найден: %s", file_path)
            continue
        applied.update(_load_one(file_path, override=override))
        # одного файла достаточно: он уже дал все свои значения
        break
    return applied


def _load_one(file_path: Path, *, override: bool) -> dict[str, str]:
    applied: dict[str, str] = {}
    try:
        text = file_path.read_text(encoding="utf-8-sig")
    except OSError as exc:
        log.warning("не удалось прочитать %s: %s", file_path, exc)
        return {}

    for line_no, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        key, sep, value = line.partition("=")
        if not sep:
            log.debug("%s:%d: строка без '=' пропущена", file_path, line_no)
            continue
        key = key.strip()
        if not key:
            continue
        value = _unquote(value.strip())
        if not override and key in os.environ:
            continue
        os.environ[key] = value
        applied[key] = value

    if applied:
        # значения не логируем: там токены
        log.debug("из %s подхвачено переменных: %s", file_path, ", ".join(sorted(applied)))
    return applied


def get_secret(name: str | None, *, default: str | None = None) -> str | None:
    """Взять значение из окружения по имени переменной, а не по самому секрету."""
    if not name:
        return default
    value = os.environ.get(name, "").strip()
    return value or default


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    # инлайновый комментарий вида `VALUE # пояснение` — только если перед ним пробел
    if " #" in value:
        value = value.split(" #", 1)[0].rstrip()
    return value
