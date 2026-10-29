"""Отправка алертов в Telegram Bot API.

Ключевые решения:

* токен и chat id берутся **из окружения по имени переменной** (``TELEGRAM_BOT_TOKEN``,
  ``TELEGRAM_CHAT_ID``), а не из конфига — секреты не должны попадать в JSON и логи;
* HTML-сообщение длиннее лимита отправляется без разметки: тег, разрезанный пополам,
  Telegram отклоняет целиком;
* 429 обрабатывается по ``retry_after`` из ответа, 5xx — с backoff;
* отправка идёт через общий лимитер, чтобы пачка одновременных алертов не упиралась
  в rate limit бота (30 сообщений в секунду на бота, но для чата лимит мягче и злее).
"""

from __future__ import annotations

import asyncio
import html
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .env import get_secret

if TYPE_CHECKING:  # избегаем циклического импорта config → telegram → models → config
    from .models import Alert

log = logging.getLogger(__name__)

API_BASE = "https://api.telegram.org"
MAX_MESSAGE_LEN = 4096
SAFE_CHUNK_LEN = 3900  # запас под HTML-теги при склейке


@dataclass
class TelegramConfig:
    """Настройки транспорта. Сами секреты живут в окружении, здесь только их имена."""

    enabled: bool = True
    token_env: str = "TELEGRAM_BOT_TOKEN"
    chat_id_env: str = "TELEGRAM_CHAT_ID"
    chat_ids: list[str] = field(default_factory=list)  # можно задать и в конфиге
    parse_mode: str = "HTML"  # "HTML" | "MarkdownV2" | "" (без разметки)
    disable_notification: bool = False
    disable_web_page_preview: bool = True
    min_interval_s: float = 1.0  # пауза между сообщениями в один чат
    timeout_s: float = 10.0
    retries: int = 3


class TelegramError(RuntimeError):
    pass


class _RateLimiter:
    """Не чаще одного сообщения в ``interval`` секунд (общий на все чаты)."""

    def __init__(self, interval: float) -> None:
        self.interval = max(0.0, interval)
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def wait(self) -> None:
        if self.interval <= 0:
            return
        async with self._lock:
            now = asyncio.get_running_loop().time()
            delay = self.interval - (now - self._last)
            if delay > 0:
                await asyncio.sleep(delay)
            self._last = asyncio.get_running_loop().time()


class TelegramClient:
    """Минимальный клиент Bot API: только ``sendMessage``."""

    def __init__(self, cfg: TelegramConfig, *, token: str, chat_ids: list[str]) -> None:
        if not token:
            raise TelegramError(
                f"не задан токен бота: выставьте переменную окружения {cfg.token_env} (например в .env)"
            )
        if not chat_ids:
            raise TelegramError(
                f"не задан chat id: выставьте {cfg.chat_id_env} или alerts.telegram.chat_ids"
            )
        self.cfg = cfg
        self._token = token
        self.chat_ids = chat_ids
        self._session = None
        self._limiter = _RateLimiter(cfg.min_interval_s)

    async def __aenter__(self) -> "TelegramClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    async def start(self) -> None:
        import aiohttp

        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.cfg.timeout_s),
                headers={"User-Agent": "CryptoDump/0.1"},
            )

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    # ---------- API ----------

    async def send_alert(self, alert: "Alert") -> int:
        """Отправить алерт во все настроенные чаты. Возвращает число доставленных сообщений."""
        text = format_alert_html(alert)
        if len(text) > SAFE_CHUNK_LEN:
            # HTML нельзя резать по символам: тег разорвётся и Telegram отклонит сообщение.
            # Такой алерт (обычно из-за очень длинного списка инструментов) шлём без разметки.
            log.debug("алерт %s длиннее лимита — отправляю plain text", alert.inst_id)
            return await self._send_plain_split(format_alert_plain(alert))
        return await self.send_text(text)

    async def _send_plain_split(self, text: str) -> int:
        await self.start()
        delivered = 0
        for chat_id in self.chat_ids:
            for chunk in _split_message(text):
                if await self._send_message(chat_id, chunk, parse_mode=""):
                    delivered += 1
        return delivered

    async def send_text(self, text: str, *, parse_mode: str | None = None) -> int:
        await self.start()
        delivered = 0
        for chat_id in self.chat_ids:
            for chunk in _split_message(text):
                if await self._send_message(chat_id, chunk, parse_mode=parse_mode):
                    delivered += 1
        return delivered

    async def _send_message(self, chat_id: str, text: str, *, parse_mode: str | None = None) -> bool:
        import aiohttp

        mode = self.cfg.parse_mode if parse_mode is None else parse_mode
        payload = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": self.cfg.disable_web_page_preview,
            "disable_notification": self.cfg.disable_notification,
        }
        if mode:
            payload["parse_mode"] = mode

        url = f"{API_BASE}/bot{self._token}/sendMessage"
        assert self._session is not None
        fallback_to_plain = False

        for attempt in range(1, self.cfg.retries + 1):
            await self._limiter.wait()
            try:
                async with self._session.post(url, json=payload) as resp:
                    body = await resp.json(content_type=None)
                    if resp.status == 200 and body.get("ok"):
                        log.debug("telegram: доставлено в %s", chat_id)
                        return True

                    # 429 — Telegram сам говорит, сколько ждать
                    retry_after = _retry_after(body)
                    if resp.status == 429:
                        wait = retry_after or 5.0 * attempt
                        log.warning("telegram 429 на чат %s — пауза %.0f c", chat_id, wait)
                        await asyncio.sleep(wait)
                        continue

                    description = str(body.get("description") or resp.status)
                    if resp.status >= 500:
                        log.warning(
                            "telegram %s (попытка %d/%d): %s", resp.status, attempt, self.cfg.retries, description
                        )
                        await asyncio.sleep(1.5 * attempt)
                        continue

                    # Разметка сломалась (например, текст разрезал тег) — повторяем тот же
                    # текст без parse_mode, чтобы не терять алерт. Рекурсию делаем после
                    # выхода из response-контекста, а не внутри него.
                    if mode and _is_parse_error(description):
                        log.warning("telegram не разобрал %s в чате %s — повторю без разметки", mode, chat_id)
                        fallback_to_plain = True
                        break

                    # 400/403 — это ошибка конфигурации, ретраить бессмысленно
                    raise TelegramError(f"Telegram отклонил сообщение (chat {chat_id}): {description}")

            except asyncio.CancelledError:
                raise
            except TelegramError:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                # TimeoutError приходит без текста — без типа ошибки лог бесполезен
                reason = str(exc) or type(exc).__name__
                log.warning("telegram сеть (попытка %d/%d): %s", attempt, self.cfg.retries, reason)
                await asyncio.sleep(1.0 * attempt)

        if fallback_to_plain:
            return await self._send_message(chat_id, _strip_tags(text), parse_mode="")

        log.error("telegram: сообщение в %s не доставлено после %d попыток", chat_id, self.cfg.retries)
        return False


# ---------- транспорт из конфига ----------


def build_client(cfg: TelegramConfig) -> TelegramClient | None:
    """Собрать клиент из конфига и окружения. ``None``, если Telegram отключён/не настроен."""
    if not cfg.enabled:
        return None

    token = get_secret(cfg.token_env)
    if not token:
        log.warning(
            "telegram отключён: не найдена переменная %s (положите её в .env рядом с проектом)",
            cfg.token_env,
        )
        return None

    chat_ids = list(cfg.chat_ids)
    env_chat = get_secret(cfg.chat_id_env)
    if env_chat:
        chat_ids.extend(part.strip() for part in env_chat.replace(";", ",").split(",") if part.strip())
    # убираем дубли, сохраняя порядок
    chat_ids = list(dict.fromkeys(chat_ids))
    if not chat_ids:
        log.warning("telegram отключён: не найдена переменная %s и пуст chat_ids", cfg.chat_id_env)
        return None

    return TelegramClient(cfg, token=token, chat_ids=chat_ids)


# ---------- формат сообщения ----------


def format_alert_html(alert: "Alert") -> str:
    """Текст алерта под ``parse_mode=HTML``.

    Telegram не поддерживает вложенные теги одного типа и «съедает» неэкранированные
    ``<``/``&``, поэтому все динамические значения прогоняются через ``html.escape``.
    """
    from datetime import datetime

    esc = html.escape
    when = datetime.fromtimestamp(alert.ts).strftime("%d.%m %H:%M:%S")
    lines: list[str] = [
        f"🟢 <b>ПОКУПКА {esc(alert.base)}</b> · лонг {esc(alert.inst_id)}",
        f"откуп после дампа <b>{alert.drop_pct:.2f}%</b> за {esc(alert.window_label)}"
        f" (с <code>{_px(alert.high)}</code> до <code>{_px(alert.plan.low if alert.plan else alert.price)}</code>)",
        "",
    ]

    plan = alert.plan
    if plan is not None:
        targets = " · ".join(f"<code>{_px(t)}</code> ({p:+.1f}%)" for t, p in zip(plan.targets, plan.target_pcts()))
        lines += [
            f"<b>Вход:</b> <code>{_px(plan.entry)}</code> по рынку, не выше <code>{_px(plan.entry_max)}</code>",
            f"<b>Стоп:</b> <code>{_px(plan.stop)}</code> ({plan.stop_pct:.1f}%) — под {esc(plan.stop_basis)}",
            f"<b>Цели:</b> {targets}",
            f"<b>Прибыль/риск:</b> {' · '.join(f'{r:.1f}' for r in plan.rr)}",
        ]
        if plan.qty is not None:
            lines.append(
                f"<b>Размер:</b> {_qty(plan.qty)} {esc(alert.base)} ≈ {_money(plan.notional or 0.0)} USDT"
                f" ({_qty(plan.contracts or 0.0)} контр.) — на стопе −{plan.risk_usdt:g} USDT"
            )
        lines.append(
            f"⏱ актуально до {datetime.fromtimestamp(plan.valid_until).strftime('%H:%M')};"
            f" ниже <code>{_px(plan.stop)}</code> идея отменяется"
        )
        lines += [f"⚠️ {esc(w)}" for w in plan.warnings]
    else:
        lines.append(f"цена <code>{_px(alert.price)}</code> · максимум окна <code>{_px(alert.high)}</code>")

    reasons = alert.reasons()
    if reasons:
        lines += ["", f"<b>Почему ({alert.signals.score}/3):</b>"] + [f"• {esc(r)}" for r in reasons]

    lines += [
        "",
        f"Стакан {alert.book.levels} ур.: покупатели {_money(alert.book.bid_notional)}"
        f" / продавцы {_money(alert.book.ask_notional)} USDT",
        f"Поток {alert.cvd.window_s / 60:g} мин: купили {_money(alert.cvd.buy_notional)}"
        f" / продали {_money(alert.cvd.sell_notional)} (перевес {_money_signed(alert.cvd.cvd)})",
        f"<i>{esc(alert.inst_type)} · {when} · не финансовая рекомендация, расчёт по уровням</i>",
    ]
    return "\n".join(lines)


def format_alert_plain(alert: "Alert") -> str:
    """Тот же алерт без разметки — для проверки связи и логов."""
    return alert.console_text()


# ---------- внутреннее ----------


def _split_message(text: str, limit: int = SAFE_CHUNK_LEN) -> list[str]:
    limit = max(100, min(limit, MAX_MESSAGE_LEN))
    if len(text) <= limit:
        return [text]

    chunks: list[str] = []
    current: list[str] = []
    size = 0
    for line in text.splitlines(keepends=True):
        if len(line) > limit:
            # одна гигантская строка — режем жёстко, чтобы не потерять данные
            if current:
                chunks.append("".join(current))
                current, size = [], 0
            for i in range(0, len(line), limit):
                chunks.append(line[i : i + limit])
            continue
        if size + len(line) > limit:
            chunks.append("".join(current))
            current, size = [], 0
        current.append(line)
        size += len(line)
    if current:
        chunks.append("".join(current))
    return [c for c in chunks if c.strip()]


def _retry_after(body: object) -> float:
    if not isinstance(body, dict):
        return 0.0
    params = body.get("parameters")
    if not isinstance(params, dict):
        return 0.0
    try:
        return float(params.get("retry_after") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _is_parse_error(description: str) -> bool:
    lowered = description.lower()
    return "parse entities" in lowered or "can't find end" in lowered or "unsupported start tag" in lowered


def _strip_tags(text: str) -> str:
    """Убрать HTML-теги для запасной отправки без ``parse_mode``."""
    import re

    return html.unescape(re.sub(r"</?[a-zA-Z][^>]*>", "", text))
def _px(value: float) -> str:
    if value >= 1000:
        return f"{value:,.1f}"
    if value >= 1:
        return f"{value:.4f}".rstrip("0").rstrip(".")
    return f"{value:.8f}".rstrip("0").rstrip(".")


def _money(value: float) -> str:
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.0f}k"
    return f"{value:.0f}"


def _qty(value: float) -> str:
    if value >= 100:
        return f"{value:,.0f}".replace(",", " ")
    if value >= 1:
        return f"{value:.2f}".rstrip("0").rstrip(".")
    return f"{value:.6f}".rstrip("0").rstrip(".")


def _money_signed(value: float) -> str:
    sign = "+" if value >= 0 else "−"
    return f"{sign}{_money(abs(value))}"


def _ratio(value: float) -> str:
    if value != value:
        return "n/a"
    if value in (float("inf"), float("-inf")):
        return "∞"
    return f"{value:.2f}"
