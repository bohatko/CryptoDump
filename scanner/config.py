"""Конфиг сканера: дефолты, загрузка JSON, горячая перезагрузка порогов."""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field, fields, is_dataclass, replace
from pathlib import Path
from typing import Any, get_type_hints

from .telegram import TelegramConfig

log = logging.getLogger(__name__)

WINDOW_SECONDS: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
}

WINDOW_LABELS: dict[int, str] = {secs: label for label, secs in WINDOW_SECONDS.items()}


@dataclass
class MarketConfig:
    inst_types: list[str] = field(default_factory=lambda: ["SWAP", "FUTURES"])
    quote_ccy: str = "USDT"
    symbols: list[str] = field(default_factory=list)  # пусто = весь рынок (top_n)
    # top_n = 0 → весь рынок. Поток trades дешёвый: 511 инструментов на одном
    # соединении дают ~400 сообщ/с и ~300 МБ/час (замерено), поэтому ограничивать
    # вселенную десятками инструментов незачем.
    top_n: int = 0
    # отсекать совсем мёртвые инструменты: оборот за 24ч в USDT.
    # 100k отсекает ~67 самых мёртвых из 511 (там бывает 6-20k в сутки).
    min_turnover_usdt: float = 100_000.0
    exclude_patterns: list[str] = field(default_factory=lambda: ["-USDC", "-USD-", "_UMC"])
    # Порт 443 вместо 8443: OKX присылает notice 64010 о выводе 8443 из эксплуатации.
    ws_url: str = "wss://ws.okx.com:443/ws/v5/public"
    # Свечи и часть рыночных каналов живут на отдельном business-эндпоинте:
    # на /ws/v5/public канал candle1m возвращает ошибку 60018 «doesn't exist».
    ws_business_url: str = "wss://ws.okx.com:443/ws/v5/business"
    rest_url: str = "https://www.okx.com"
    subscribe_chunk: int = 64
    # Каналы, подписываемые на ВСЕ инструменты. books здесь нет намеренно:
    # 400 уровней стоят ~28 МБ/час на инструмент (13 ГБ/час на весь рынок),
    # поэтому стакан подключается по требованию — см. depth.mode == "ondemand".
    ws_channels: list[str] = field(default_factory=lambda: ["trades", "candle1m"])
    # liquidation-orders публикуется OKX не для всех типов; ошибки подписки логируются
    liquidation_inst_types: list[str] = field(default_factory=lambda: ["SWAP"])


@dataclass
class DepthConfig:
    levels: int = 20
    min_notional_usdt: float = 1_000_000.0
    poll_interval_s: float = 5.0
    concurrency: int = 8
    stale_level_s: float = 10.0
    stale_after_s: float = 15.0
    require_fresh_book: bool = True
    require_full_depth: bool = True  # сумма считается по 20 уровням, а не по 5 из books5
    wall_multiplier: float = 4.0  # во сколько раз уровень больше среднего по бидам
    wall_min_notional_usdt: float = 300_000.0
    wall_side: str = "bid"  # стена ищется на бидах
    # лучший бид не считается стеной: на OKX он почти всегда держит основную массу
    # объёма top-N, и сигнатура срабатывала бы в каждом снимке
    wall_exclude_top_level: bool = True
    # сколько секунд стена должна простоять на одном месте, чтобы считаться настоящей.
    # Меряем временем, а не числом снимков: на WS стакан обновляется ~10 раз в секунду,
    # и «три опроса» превратились бы в 300 мс, которые проходит любой спуф.
    wall_persist_s: float = 10.0
    # источник стакана: "ws" — канал books (400 уровней, ~100 мс), "rest" — поллинг
    source: str = "ws"
    # как покрывать стакан на широкой вселенной:
    #   "all"      — подписать books на все инструменты (дорого: ~28 МБ/ч на инструмент)
    #   "ondemand" — books только для инструментов, где сработал дамп (рекомендуется)
    #   "top"      — books для первых book_slots по обороту (стакан всегда под рукой)
    mode: str = "ondemand"
    # сколько инструментов одновременно могут держать стакан в режимах ondemand/top.
    # 20 × 28 МБ/ч ≈ 560 МБ/ч — комфортный потолок трафика.
    book_slots: int = 20
    # сколько секунд держать стакан после последнего срабатывания (чтобы вторая
    # волна дампа и оценка сигнатур успели отработать на живых данных)
    book_ttl_s: float = 180.0
    # Каждая подписка/отписка — запрос к OKX, а их 480 в час на соединение. Поэтому
    # слот нельзя отобрать раньше book_min_hold_s (иначе две просадки отбирают его
    # друг у друга каждую секунду), а освобождённый стакан не выдаётся тому же
    # инструменту book_retry_s секунд — если только просадка не стала в 1.5 раза глубже.
    book_min_hold_s: float = 30.0
    book_retry_s: float = 600.0
    # запас глубины: сколько уровней хранить, чтобы при выедании верхних было что
    # подтянуть снизу. Считаем всегда ровно по levels.
    store_levels: int = 100


@dataclass
class CvdConfig:
    window_s: int = 300
    burst_s: int = 60
    reversal_ratio: float = 0.6
    taker_buy_ratio: float = 1.35
    # до какого окна (в секундах) максимум считается по сделкам, а не по 1m-свечам.
    # 1m и 5m считаются точно (погрешность свечей на них достигает 50-100%),
    # 30m+ остаются на свечах — там минута погрешности несущественна.
    precise_windows_until_s: int = 300


@dataclass
class LiquidationConfig:
    window_s: int = 300
    decay_ratio: float = 0.35
    min_notional_usdt: float = 200_000.0


@dataclass
class SignalsConfig:
    min_score: int = 2


@dataclass
class AlertsConfig:
    cooldown_s: float = 300.0
    sinks: list[str] = field(default_factory=lambda: ["console", "jsonl", "telegram"])
    jsonl_path: str = "alerts.jsonl"
    webhook_url: str | None = None
    telegram: TelegramConfig = field(default_factory=TelegramConfig)


@dataclass
class EngineConfig:
    eval_interval_s: float = 2.0
    reload_interval_s: float = 15.0
    bootstrap_candles: int = 125
    trade_stale_after_s: float = 30.0
    warmup_s: float = 60.0
    # персист состояния (CVD, ликвидации, кулдаун): без него после рестарта
    # сигнатуры молчат, пока не наберётся окно
    persist_state: bool = True
    state_path: str = "state.json"
    state_save_interval_s: float = 30.0
    # старше этого возраста снимок не восстанавливаем: бакеты уже не отражают поток
    state_max_age_s: float = 900.0
    log_level: str = "INFO"


@dataclass
class TradeConfig:
    """План сделки в алерте: вход, стоп, цели. Механический расчёт по уровням дампа."""

    enabled: bool = True
    # не догонять: выше entry × (1 + entry_slippage_pct%) входить уже поздно
    entry_slippage_pct: float = 0.3
    # стоп ставится под опору (стена покупателя или дно дампа) с этим отступом
    stop_buffer_pct: float = 0.3
    # стоп ближе этого выбьет обычный шум — отодвигаем
    min_stop_pct: float = 0.6
    # стоп дальше — сделка помечается как рискованная
    max_stop_pct: float = 4.0
    # цели — доли отката дампа от дна к максимуму окна (1.0 = возврат к максимуму)
    targets_retrace: list[float] = field(default_factory=lambda: [0.382, 0.618, 1.0])
    # сколько USDT готовы потерять на стопе; 0 — размер позиции не считать
    risk_usdt: float = 0.0
    # сколько секунд после алерта план считается актуальным
    valid_s: float = 900.0
    # ниже этого «прибыль/риск» до второй цели сделка помечается как слабая
    min_rr: float = 1.0


@dataclass
class PaperConfig:
    """Виртуальный счёт: каждая сделка по плану алерта на весь баланс, без реальных ордеров."""

    enabled: bool = True
    start_balance: float = 100.0
    leverage: float = 5.0
    # комиссия тейкера OKX за сторону, % от номинала (вход и выход по рынку)
    fee_pct: float = 0.05
    # проскальзывание при выходе по стопу и по времени: рыночный ордер в падающий стакан
    slippage_pct: float = 0.05
    # какую цель плана брать тейком: 0 — 38.2%, 1 — 61.8%, 2 — возврат к максимуму
    target_index: int = 1
    # сделка с прибылью/риском до тейка ниже этого не открывается (сигнал всё равно считается)
    min_rr: float = 1.0
    # после первой цели стоп переносится в безубыток (с учётом комиссий)
    breakeven_after_first: bool = True
    # откуп после дампа — идея на часы, не на дни: дольше держать — закрыть по рынку
    max_hold_s: float = 7200.0


@dataclass
class DashboardConfig:
    enabled: bool = True
    # По умолчанию только локальный доступ. На другой адрес дашборд поднимется лишь
    # с токеном: через него можно менять пороги и останавливать сканер.
    host: str = "127.0.0.1"
    port: int = 8787
    # имя переменной окружения с токеном доступа (переменная не задана — без токена)
    token_env: str = "DASHBOARD_TOKEN"
    # история алертов и эпизодов просадки (SQLite из стандартной библиотеки)
    history_path: str = "history.sqlite3"
    # эпизод просадки закрывается, если инструмент столько секунд не был в просадке
    episode_gap_s: float = 60.0
    # сколько последних записей лога держать для вкладки «Журнал»
    log_buffer: int = 1000


@dataclass
class Config:
    market: MarketConfig = field(default_factory=MarketConfig)
    windows: dict[str, float] = field(
        default_factory=lambda: {"1m": 1.5, "5m": 3.0, "30m": 6.0, "1h": 9.0, "2h": 12.0}
    )
    depth: DepthConfig = field(default_factory=DepthConfig)
    cvd: CvdConfig = field(default_factory=CvdConfig)
    liquidation: LiquidationConfig = field(default_factory=LiquidationConfig)
    signals: SignalsConfig = field(default_factory=SignalsConfig)
    alerts: AlertsConfig = field(default_factory=AlertsConfig)
    engine: EngineConfig = field(default_factory=EngineConfig)
    trade: TradeConfig = field(default_factory=TradeConfig)
    paper: PaperConfig = field(default_factory=PaperConfig)
    dashboard: DashboardConfig = field(default_factory=DashboardConfig)
    source_path: str | None = None
    _thresholds_cache: tuple[tuple[tuple[str, float], ...], list[tuple[int, float]]] | None = field(
        default=None, repr=False, compare=False
    )

    # ---------- API ----------

    def window_thresholds(self) -> list[tuple[int, float]]:
        """[(секунды, порог в %)] — только известные метки окон, отсортировано.

        Результат кэшируется по содержимому ``windows``: метод вызывается в цикле
        оценки, а предупреждения о мусорных порогах не должны спамить каждые 2 c.
        """
        cached = self._thresholds_cache
        if cached is not None and cached[0] == self._windows_key():
            return cached[1]

        result: list[tuple[int, float]] = []
        for label, pct in self.windows.items():
            secs = WINDOW_SECONDS.get(label)
            if secs is None:
                log.warning("неизвестная метка окна в конфиге: %r (ожидались %s)", label, list(WINDOW_SECONDS))
                continue
            if pct <= 0:
                log.warning("порог окна %s = %s, ожидалось положительное число — пропускаю", label, pct)
                continue
            result.append((secs, float(pct)))
        result.sort(key=lambda item: item[0])
        self._thresholds_cache = (self._windows_key(), result)
        return result

    def _windows_key(self) -> tuple[tuple[str, float], ...]:
        return tuple(sorted((str(k), float(v)) for k, v in self.windows.items()))

    def max_window_s(self) -> int:
        return max((secs for secs, _ in self.window_thresholds()), default=WINDOW_SECONDS["2h"])

    def restart_required_changes(self, other: "Config") -> list[str]:
        """Поля, которые нельзя применить без рестарта."""
        changed: list[str] = []
        if self.market != other.market:
            changed.append("market")
        if self.depth.levels != other.depth.levels or self.depth.poll_interval_s != other.depth.poll_interval_s:
            changed.append("depth.poll")
        # от режима зависит набор подписок и наличие планировщика стакана
        if self.depth.source != other.depth.source or self.depth.mode != other.depth.mode:
            changed.append("depth.mode")
        # сервер и база истории создаются один раз при старте; на лету — только episode_gap_s
        if replace(self.dashboard, episode_gap_s=0.0) != replace(other.dashboard, episode_gap_s=0.0):
            changed.append("dashboard")
        return changed


# ---------- загрузка ----------

def _build(cls: Any, data: dict[str, Any]) -> Any:
    """Собрать датакласс из dict, игнорируя неизвестные ключи.

    Вложенные датаклассы (например ``alerts.telegram``) собираются рекурсивно.
    """
    if not is_dataclass(cls):
        return data
    known = {f.name: f for f in fields(cls)}
    unknown = set(data) - set(known)
    if unknown:
        log.warning("%s: неизвестные ключи конфига игнорируются: %s", cls.__name__, sorted(unknown))

    # config.py объявлен с `from __future__ import annotations`, поэтому f.type — строка;
    # get_type_hints разворачивает её в реальный тип и позволяет увидеть вложенный dataclass
    try:
        hints = get_type_hints(cls)
    except Exception:  # noqa: BLE001 — не резолвится (редкий тип) → без вложенности
        hints = {}

    kwargs: dict[str, Any] = {}
    for key, value in data.items():
        if key not in known:
            continue
        if isinstance(value, dict):
            nested = _nested_dataclass(hints.get(key))
            if nested is not None:
                kwargs[key] = _build(nested, value)
                continue
        kwargs[key] = value
    return cls(**kwargs)


def _nested_dataclass(annotation: Any) -> Any | None:
    """Достать dataclass-тип из аннотации поля (``TelegramConfig`` или ``X | None``)."""
    if is_dataclass(annotation) and isinstance(annotation, type):
        return annotation
    for arg in getattr(annotation, "__args__", ()):
        if is_dataclass(arg) and isinstance(arg, type):
            return arg
    return None


def config_from_dict(data: dict[str, Any], source_path: str | None = None) -> Config:
    cfg = Config()
    market = data.get("market")
    if isinstance(market, dict):
        cfg.market = _build(MarketConfig, market)
    windows = data.get("windows")
    if isinstance(windows, dict):
        parsed: dict[str, float] = {}
        for label, pct in windows.items():
            try:
                parsed[str(label)] = float(pct)
            except (TypeError, ValueError):
                log.warning("windows.%s = %r не число — пропускаю", label, pct)
        if parsed:
            cfg.windows = parsed
    for name, cls in (
        ("depth", DepthConfig),
        ("cvd", CvdConfig),
        ("liquidation", LiquidationConfig),
        ("signals", SignalsConfig),
        ("alerts", AlertsConfig),
        ("engine", EngineConfig),
        ("trade", TradeConfig),
        ("paper", PaperConfig),
        ("dashboard", DashboardConfig),
    ):
        section = data.get(name)
        if isinstance(section, dict):
            setattr(cfg, name, _build(cls, section))
    cfg.source_path = source_path
    return cfg


def load_config(path: str | os.PathLike[str] | None) -> Config:
    if not path:
        return Config()
    file_path = Path(path)
    if not file_path.exists():
        log.warning("конфиг %s не найден — использую дефолты", file_path)
        return Config(source_path=str(file_path))
    with file_path.open("r", encoding="utf-8-sig") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"конфиг {file_path} должен быть JSON-объектом")
    return config_from_dict(data, source_path=str(file_path))


class ConfigWatcher:
    """Следит за mtime файла конфига и подсовывает свежие пороги на лету."""

    def __init__(self, path: str | os.PathLike[str] | None) -> None:
        self.path = Path(path) if path else None
        self._mtime = self._read_mtime()

    def _read_mtime(self) -> float:
        if not self.path:
            return 0.0
        try:
            return self.path.stat().st_mtime
        except OSError:
            return 0.0

    def poll(self) -> Config | None:
        """Вернуть новый конфиг, если файл изменился, иначе ``None``."""
        mtime = self._read_mtime()
        if mtime == 0.0 or mtime == self._mtime:
            return None
        self._mtime = mtime
        try:
            cfg = load_config(self.path)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            log.error("не удалось перезагрузить конфиг %s: %s — работаю на старом", self.path, exc)
            return None
        cfg.source_path = str(self.path)
        log.info("конфиг перезагружен из %s (mtime=%s, in %s)", self.path, time.ctime(mtime), time.time())
        return cfg

    def mark_seen(self) -> None:
        """Файл записан нами самими (дашборд) и уже применён — не перечитывать его."""
        self._mtime = self._read_mtime()


def merge_config(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    """Наложить частичную правку на конфиг: вложенные секции сливаются, остальное заменяется."""
    merged = dict(base)
    for key, value in patch.items():
        current = merged.get(key)
        if isinstance(value, dict) and isinstance(current, dict) and key != "windows":
            merged[key] = merge_config(current, value)
        else:
            # windows заменяется целиком: иначе удалённое окно нельзя было бы убрать
            merged[key] = value
    return merged


def read_config_file(path: str | os.PathLike[str]) -> dict[str, Any]:
    file_path = Path(path)
    if not file_path.exists():
        return {}
    with file_path.open("r", encoding="utf-8-sig") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"конфиг {file_path} должен быть JSON-объектом")
    return data


def write_config_file(path: str | os.PathLike[str], data: dict[str, Any]) -> None:
    """Атомарная запись (UTF-8 без BOM): сканер читает файл на лету и не должен увидеть половину."""
    file_path = Path(path)
    tmp = file_path.with_name(file_path.name + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, file_path)


def dump_config(cfg: Config) -> str:
    from dataclasses import asdict

    data = asdict(cfg)
    data.pop("source_path", None)
    data.pop("_thresholds_cache", None)
    return json.dumps(data, ensure_ascii=False, indent=2)
