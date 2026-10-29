"""CLI: ``python -m scanner.main`` / ``python -m scanner``."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

from . import __version__, logbuffer
from .config import Config, dump_config, load_config
from .engine import Engine, ScanStartupError
from .env import load_dotenv


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="scanner",
        description="Сканер откупов после дампа на фьючерсах OKX (public stream).",
    )
    parser.add_argument("-c", "--config", default=None, help="путь к JSON-конфигу (по умолчанию дефолты)")
    parser.add_argument(
        "-s",
        "--symbols",
        default=None,
        help="список баз через запятую, например BTC,ETH,SOL (перебивает market.symbols)",
    )
    parser.add_argument(
        "--inst-types",
        default=None,
        help="типы инструментов через запятую: SWAP,FUTURES (по умолчанию из конфига)",
    )
    parser.add_argument("--top-n", type=int, default=None, help="сколько инструментов брать по обороту")
    parser.add_argument("--duration", type=float, default=None, help="работать N секунд и выйти")
    parser.add_argument("--dry-run", action="store_true", help="без внешних каналов: только консоль и JSONL")
    parser.add_argument("--no-telegram", action="store_true", help="не отправлять в Telegram")
    parser.add_argument("--no-dashboard", action="store_true", help="не поднимать веб-дашборд")
    parser.add_argument("--port", type=int, default=None, help="порт веб-дашборда (по умолчанию 8787)")
    parser.add_argument(
        "--telegram-test",
        action="store_true",
        help="отправить тестовое сообщение в Telegram и выйти (проверка токена и чата)",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="путь к .env (по умолчанию .env в текущей папке)",
    )
    parser.add_argument("--log-level", default=None, help="DEBUG/INFO/WARNING/ERROR")
    parser.add_argument("--print-config", action="store_true", help="вывести итоговый конфиг и выйти")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser


def _apply_cli_overrides(cfg: Config, args: argparse.Namespace) -> None:
    if args.symbols:
        cfg.market.symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if args.inst_types:
        cfg.market.inst_types = [s.strip().upper() for s in args.inst_types.split(",") if s.strip()]
    if args.top_n:
        cfg.market.top_n = max(1, args.top_n)
    if args.log_level:
        cfg.engine.log_level = args.log_level.upper()
    if args.no_dashboard:
        cfg.dashboard.enabled = False
    if args.port:
        cfg.dashboard.port = args.port
    if args.no_telegram or args.dry_run:
        cfg.alerts.telegram.enabled = False
        cfg.alerts.sinks = [s for s in cfg.alerts.sinks if s != "telegram"]


async def _amain(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    _apply_cli_overrides(cfg, args)

    logging.basicConfig(
        level=getattr(logging, cfg.engine.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )

    if args.print_config:
        print(dump_config(cfg))
        return 0

    if args.telegram_test:
        return await _telegram_test(cfg)

    # журнал для дашборда подключаем один раз: он должен пережить перезапуск движка
    logbuffer.install(cfg.dashboard.log_buffer)

    while True:
        engine = Engine(cfg, dry_run=args.dry_run)
        _install_signal_handlers(engine)
        try:
            await engine.run(duration_s=args.duration)
        except KeyboardInterrupt:
            engine.stop()
            return 0
        if not engine.restart_requested:
            return 0
        # перезапуск из дашборда: перечитываем файл, чтобы применились и поля «только с рестартом»
        logging.getLogger(__name__).warning("перезапуск сканера")
        cfg = load_config(args.config)
        _apply_cli_overrides(cfg, args)
        logging.getLogger().setLevel(getattr(logging, cfg.engine.log_level.upper(), logging.INFO))


def _install_signal_handlers(engine: Engine) -> None:
    loop = asyncio.get_running_loop()
    for name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, engine.stop)
        except (NotImplementedError, AttributeError, ValueError):
            pass  # Windows/Proactor: Ctrl+C обработается как KeyboardInterrupt


async def _telegram_test(cfg: Config) -> int:
    """Проверка связки «токен + чат» без запуска сканера."""
    from .telegram import TelegramError, build_client

    tg_cfg = cfg.alerts.telegram
    tg_cfg.enabled = True  # для явного --telegram-test игнорируем --dry-run/--no-telegram
    client = build_client(tg_cfg)
    if client is None:
        print(
            f"Telegram не настроен: нужны переменные {tg_cfg.token_env} и {tg_cfg.chat_id_env} "
            "(в .env или в окружении)",
            file=sys.stderr,
        )
        return 1

    text = (
        "<b>CryptoDump</b>: проверка связи\n"
        "Если видите это сообщение — бот настроен верно и будет присылать дампы."
    )
    try:
        delivered = await client.send_text(text)
    except TelegramError as exc:
        print(f"Telegram отклонил запрос: {exc}", file=sys.stderr)
        return 1
    finally:
        await client.close()

    if delivered == 0:
        print("сообщение не доставлено ни в один чат", file=sys.stderr)
        return 1
    print(f"отправлено сообщений: {delivered} (чаты: {', '.join(client.chat_ids)})")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if sys.version_info < (3, 11):
        print(f"нужен Python 3.11+, запущен {sys.version.split()[0]}", file=sys.stderr)
        return 2

    # .env подхватываем до сборки конфига: Telegram берёт токен из окружения
    applied = load_dotenv(args.env_file)
    if applied:
        print(f"из .env подхвачено: {', '.join(sorted(applied))}", file=sys.stderr)
    try:
        return asyncio.run(_amain(args))
    except KeyboardInterrupt:
        return 130
    except ScanStartupError as exc:
        # нет сети / OKX недоступен — понятное сообщение вместо трейсбека
        print(f"сканирование не запущено: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
