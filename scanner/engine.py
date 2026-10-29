"""Оркестратор: собирает компоненты и крутит фоновые задачи.

Поток данных:

    REST bootstrap (universe.py) ──► InstrumentState × N + CandleBook.seed
    WS public:   trades, books*, liquidation-orders ─┐
    WS business: candle1m ───────────────────────────┴► WsRouter ──► InstrumentState
    DepthGuard (раз в 1 c): протух / порван seqId → переподписка books
                            (REST /market/books только при depth.source == "rest")

    eval  (каждые engine.eval_interval_s): evaluate_instrument() → AlertRouter.publish
    reload (engine.reload_interval_s):     ConfigWatcher → _apply_config
    state  (engine.state_save_interval_s): state.json

    * На каких инструментах подписан ``books``, решает ``depth.mode``:
      ``all`` — на всех; ``top`` — на ``book_slots`` самых оборотных, постоянно;
      ``ondemand`` (по умолчанию) — eval видит просадку без стакана (``DropSeen``) →
      ``DepthScheduler.want`` → слот + подписка books на ``book_ttl_s``.

Задачи независимы; падение любой из них (кроме отменённых) останавливает движок,
чтобы сканер не остался «живым без данных».
"""

from __future__ import annotations

import asyncio
import logging
import time

from .aio import sleep_or_stop
from .alerts import AlertRouter
from .config import Config, ConfigWatcher
from .depth_guard import DepthGuard
from .depth_scheduler import DepthScheduler
from .evaluate import DropSeen, evaluate_instrument
from .instrument import InstrumentState
from .market import OkxRest
from .models import Alert
from .okx_ws import OkxPublicStream, Subscription, liquidations, per_instrument, split_by_endpoint
from .state import load_state, save_state
from .universe import bootstrap_candles, build_universe
from .ws_router import WsRouter

log = logging.getLogger(__name__)

# сколько ждать первый snapshot после подписки, прежде чем сторож начнёт чинить слот
BOOK_GRACE_S = 5.0


class ScanStartupError(RuntimeError):
    """Сканер не смог собрать вселенную инструментов и стартовать."""


class Engine:
    def __init__(self, cfg: Config, *, dry_run: bool = False) -> None:
        self.cfg = cfg
        self.dry_run = dry_run
        self.states: dict[str, InstrumentState] = {}
        self.watcher = ConfigWatcher(cfg.source_path)
        self.alerts = AlertRouter(cfg.alerts, dry_run=dry_run)
        self.rest = OkxRest(cfg.market.rest_url)
        self._stop = asyncio.Event()
        # Планировщик стакана по требованию создаётся сразу, а поток подставляется
        # позже (см. _stream_loop): иначе его задача стартовала бы раньше соединений.
        self.depth_scheduler: DepthScheduler | None = (
            DepthScheduler(self.states, cfg) if cfg.depth.source == "ws" and cfg.depth.mode == "ondemand" else None
        )
        # режим top: books подписан сразу и навсегда, планировщик не участвует
        self._static_books: set[str] = set()
        self.depth = DepthGuard(self.states, cfg, self.rest, self._stop, has_book=self._guard_owns)
        self.router = WsRouter(self.states, cfg, on_book_gap=self.depth.schedule_resync)
        self._streams: list[OkxPublicStream] = []
        self._started_at = 0.0

    # ---------- кто владеет стаканом ----------

    def _every_book(self) -> bool:
        return self.cfg.depth.source != "ws" or self.cfg.depth.mode == "all"

    def _has_book(self, inst_id: str) -> bool:
        """Есть ли у инструмента подписанный стакан (иначе оценка вернёт ``DropSeen``)."""
        if self._every_book() or inst_id in self._static_books:
            return True
        return self.depth_scheduler is not None and self.depth_scheduler.has_book(inst_id)

    def _guard_owns(self, inst_id: str, *, now: float | None = None) -> bool:
        """Должен ли DepthGuard чинить этот стакан.

        Свежему слоту даём ``BOOK_GRACE_S`` на первый snapshot: до него стакан
        выглядит протухшим, и сторож переподписал бы его впустую.
        """
        if self._every_book() or inst_id in self._static_books:
            return True
        if self.depth_scheduler is None:
            return False
        since = self.depth_scheduler.subscribed_at(inst_id)
        now = time.time() if now is None else now
        return since is not None and now - since >= BOOK_GRACE_S

    # ---------- публичный запуск ----------

    async def run(self, *, duration_s: float | None = None) -> None:
        log.info(
            "старт: inst_types=%s quote=%s windows=%s",
            self.cfg.market.inst_types,
            self.cfg.market.quote_ccy,
            [f"{w}@{t}%" for w, t in (self.cfg.window_thresholds())],
        )
        await self.rest.start()
        self._started_at = time.time()
        try:
            await self._bootstrap()
            self._restore_state()
            tasks = [
                asyncio.create_task(self._stream_loop(), name="ws"),
                asyncio.create_task(self.depth.run(), name="depth"),
                asyncio.create_task(self._eval_loop(), name="eval"),
                asyncio.create_task(self._reload_loop(), name="reload"),
            ]
            if self.cfg.engine.persist_state:
                tasks.append(asyncio.create_task(self._state_loop(), name="state"))
            if self.depth_scheduler is not None:
                tasks.append(asyncio.create_task(self.depth_scheduler.run(), name="depth-scheduler"))
            if duration_s is not None:
                tasks.append(asyncio.create_task(self._duration_loop(duration_s), name="duration"))
            for task in tasks:
                task.add_done_callback(self._on_task_done)
            await self._stop.wait()
            log.info("останавливаюсь…")
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await self.alerts.close()
            await self.rest.close()
            # в потоке: файл может быть занят, а блокировать event loop на выходе не нужно
            await asyncio.to_thread(self._save_state)
            log.info("остановлено, алертов отправлено: %d", self.alerts.sent)

    def _on_task_done(self, task: asyncio.Task) -> None:
        """Упавшая критичная задача не должна оставлять движок «живым без данных»."""
        if task.cancelled() or self._stop.is_set():
            return
        exc = task.exception()
        if exc is not None:
            log.error("задача %s упала: %s", task.get_name(), exc, exc_info=exc)
            self._stop.set()

    def stop(self) -> None:
        self._stop.set()

    # ---------- бутстрап ----------

    async def _bootstrap(self) -> None:
        """Собрать вселенную из REST и подтянуть историю свечей."""
        universe = await build_universe(self.rest, self.cfg.market)
        if not universe:
            raise ScanStartupError(
                "не удалось получить список инструментов из REST OKX "
                f"({self.cfg.market.rest_url}) — проверьте сеть и market.quote_ccy"
            )

        for inst, turnover in universe:
            self.states[inst.inst_id] = InstrumentState(
                inst_id=inst.inst_id,
                inst_type=inst.inst_type,
                ct_val=inst.contract_value,
                turnover=turnover,
                max_window_s=self.cfg.max_window_s(),
                cvd_window_s=self.cfg.cvd.window_s,
                liq_window_s=self.cfg.liquidation.window_s,
                precise_windows_until_s=self.cfg.cvd.precise_windows_until_s,
            )

        log.info("инструментов в скане: %d", len(self.states))
        await self._bootstrap_candles()

    async def _bootstrap_candles(self) -> None:
        await bootstrap_candles(
            self.rest,
            list(self.states.values()),
            limit=self.cfg.engine.bootstrap_candles,
            concurrency=self.cfg.depth.concurrency,
        )

    # ---------- WS ----------

    def _subscriptions(self) -> list[Subscription]:
        channels = tuple(self.cfg.market.ws_channels)
        mode = self.cfg.depth.mode
        if self.cfg.depth.source == "ws" and mode == "all" and "books" not in channels:
            # стакан на WS требует именно канала books — добавляем сами
            channels = channels + ("books",)
        if not channels:
            raise ScanStartupError("market.ws_channels пуст: без trades/candle1m сканер не получит данных")

        # В режимах ondemand/top стакан подписывается не на всех: books стоит
        # ~28 МБ/ч на инструмент, на 500 инструментах это 13 ГБ/ч.
        subs = per_instrument(list(self.states), channels)
        if self.cfg.depth.source == "ws" and mode == "top":
            top = self._top_by_turnover(self.cfg.depth.book_slots)
            self._static_books = set(top)
            subs += [Subscription(channel="books", inst_id=iid) for iid in top]
            log.info("стакан сразу подключён для топ-%d по обороту", len(top))
        elif self.cfg.depth.source == "ws" and mode == "ondemand":
            log.info("стакан по требованию: books подключится под дампы (слотов %d)", self.cfg.depth.book_slots)

        # OKX публикует ``liquidation-orders`` не для всех типов инструментов,
        # поэтому набор типов задаётся отдельно (по умолчанию только SWAP)
        liq_types = [t for t in self.cfg.market.liquidation_inst_types if t in self.cfg.market.inst_types]
        if not liq_types:
            log.warning("liquidation-orders не подписан — сигнатура затухания ликвидаций будет пустой")
        return subs + liquidations(liq_types)

    def _top_by_turnover(self, n: int) -> list[str]:
        """Инструменты с наибольшим оборотом (для режима ``top``)."""
        ranked = sorted(self.states.values(), key=lambda s: s.turnover, reverse=True)
        return [s.inst_id for s in ranked[: max(1, n)]]

    def _public_stream(self) -> OkxPublicStream | None:
        """Поток public — через него планировщик управляет подписками на books."""
        for stream in self._streams:
            if stream.url == self.cfg.market.ws_url:
                return stream
        return None

    async def _stream_loop(self) -> None:
        public_subs, business_subs, unknown = split_by_endpoint(self._subscriptions())
        for sub in unknown:
            log.error("неизвестный канал %r — пропускаю (OKX отклонил бы всю пачку)", sub.channel)

        streams: list[OkxPublicStream] = []
        for url, subs in (
            (self.cfg.market.ws_url, public_subs),
            (self.cfg.market.ws_business_url, business_subs),
        ):
            if not subs:
                continue
            streams.append(
                OkxPublicStream(
                    url,
                    subs,
                    self.router.handle,
                    chunk_size=self.cfg.market.subscribe_chunk,
                    on_reconnect=self._on_reconnect,
                )
            )
        if not streams:
            raise ScanStartupError("нечего подписывать: все каналы отфильтрованы как неизвестные")

        log.info(
            "подписки: public=%d (%s), business=%d (%s)",
            len(public_subs),
            ",".join(sorted({s.channel for s in public_subs})) or "—",
            len(business_subs),
            ",".join(sorted({s.channel for s in business_subs})) or "—",
        )
        self._streams = streams
        self.depth.streams = streams

        public_stream = self._public_stream()
        if self.depth_scheduler is not None and public_stream is not None:
            # с этого момента планировщик может подписываться на books
            self.depth_scheduler.stream = public_stream

        await asyncio.gather(*(stream.run(self._stop) for stream in streams))

    async def _on_reconnect(self, _connected_at: float) -> None:
        """После разрыва история могла устареть — дозаправим свечи."""
        log.info("WS переподключён, обновляю свечи")
        await self._bootstrap_candles()

    # ---------- оценка ----------

    async def _eval_loop(self) -> None:
        while not self._stop.is_set():
            await sleep_or_stop(self.cfg.engine.eval_interval_s, self._stop)
            if self._stop.is_set():
                return
            try:
                await self._evaluate_all()
            except Exception:  # noqa: BLE001 — цикл не должен умирать
                log.exception("ошибка в цикле оценки")

    async def _evaluate_all(self, *, now: float | None = None) -> list[Alert]:
        now = time.time() if now is None else now
        # прогрев: без сделок и ликвидаций CVD/стакан пустые, сигнатуры дадут ложный ноль
        warmup_left = self.cfg.engine.warmup_s - (now - self._started_at)
        if warmup_left > 0:
            log.debug("прогрев: ещё %.0f c", warmup_left)
            return []

        # считаем синхронно (это дёшево), а отправку алертов делаем параллельно:
        # вебхук может тормозить до таймаута, и он не должен задерживать оценку
        # остальных инструментов
        alerts: list[Alert] = []
        scheduler = self.depth_scheduler
        for state in list(self.states.values()):
            try:
                # в режимах ondemand/top стакан есть не у всех — тогда оценка
                # вернёт DropSeen, и мы запросим слот вместо пропуска дампа
                result = evaluate_instrument(
                    state, self.cfg, now=now, depth_available=self._has_book(state.inst_id)
                )
            except Exception:  # noqa: BLE001 — один инструмент не должен ломать проход
                log.exception("ошибка оценки %s", state.inst_id)
                continue

            if isinstance(result, DropSeen):
                if scheduler is not None:
                    scheduler.want(state.inst_id, drop_pct=result.drop.drop_pct, now=now)
                else:
                    # режим top: инструмент вне топа, стакана для него не будет
                    log.debug("%s: просадка %.2f%% без стакана", state.inst_id, result.drop.drop_pct)
            elif isinstance(result, Alert):
                alerts.append(result)
                if scheduler is not None:
                    # инструмент активен — держим стакан живым, чтобы вторая волна
                    # дампа тоже отработала на актуальных данных
                    scheduler.want(state.inst_id, drop_pct=result.drop_pct, now=now)

        if alerts:
            await asyncio.gather(*(self._publish(alert) for alert in alerts), return_exceptions=True)
        return alerts

    async def _publish(self, alert: Alert) -> None:
        published = await self.alerts.publish(alert)
        if published and alert.sibling_windows:
            # остальные сработавшие окна того же дампа гасим, чтобы не спамить
            self.alerts.cooldown.suppress(alert.inst_id, alert.sibling_windows, now=alert.ts)

    # ---------- горячая перезагрузка ----------

    async def _reload_loop(self) -> None:
        interval = max(1.0, self.cfg.engine.reload_interval_s)
        while not self._stop.is_set():
            await sleep_or_stop(interval, self._stop)
            if self._stop.is_set():
                return
            fresh = self.watcher.poll()
            if fresh is not None:
                self._apply_config(fresh)

    def _apply_config(self, fresh: Config) -> None:
        restart = self.cfg.restart_required_changes(fresh)
        if restart:
            log.warning(
                "в конфиге изменены %s — это требует рестарта, оставляю старые значения",
                ", ".join(restart),
            )
            # поля, требующие рестарта, откатываем к текущим: смена вселенной или
            # интервала опроса на лету привела бы к рассинхрону подписок и данных
            fresh.market = self.cfg.market
            fresh.depth.levels = self.cfg.depth.levels
            fresh.depth.poll_interval_s = self.cfg.depth.poll_interval_s
            fresh.depth.source = self.cfg.depth.source
            fresh.depth.mode = self.cfg.depth.mode
        self.cfg = fresh
        # компоненты держат ссылку на конфиг — без этого они работали бы на старых порогах
        self.router.cfg = fresh
        self.depth.cfg = fresh
        if self.depth_scheduler is not None:
            self.depth_scheduler.cfg = fresh
        self.alerts.cooldown.cooldown_s = fresh.alerts.cooldown_s
        log.info("пороги обновлены: %s", [f"{w}@{t}%" for w, t in fresh.window_thresholds()])

    async def _duration_loop(self, duration_s: float) -> None:
        await sleep_or_stop(duration_s, self._stop)
        log.info("отработал %.0f c — выхожу", duration_s)
        self._stop.set()

    # ---------- персист состояния ----------

    def _restore_state(self) -> None:
        if not self.cfg.engine.persist_state:
            return
        load_state(
            self.cfg.engine.state_path,
            trades={iid: s.trades for iid, s in self.states.items()},
            liq={iid: s.liq for iid, s in self.states.items()},
            cooldown=self.alerts.cooldown,
            max_age_s=self.cfg.engine.state_max_age_s,
        )

    def _save_state(self) -> None:
        if not self.cfg.engine.persist_state:
            return
        save_state(
            self.cfg.engine.state_path,
            trades={iid: s.trades for iid, s in self.states.items()},
            liq={iid: s.liq for iid, s in self.states.items()},
            cooldown=self.alerts.cooldown,
        )

    async def _state_loop(self) -> None:
        # запись в отдельном потоке: файл небольшой, но цикл оценки ждать не должен
        interval = max(5.0, self.cfg.engine.state_save_interval_s)
        while not self._stop.is_set():
            await sleep_or_stop(interval, self._stop)
            if self._stop.is_set():
                return
            try:
                await asyncio.to_thread(self._save_state)
            except Exception:  # noqa: BLE001 — персист не должен ронять сканер
                log.exception("ошибка сохранения состояния")
