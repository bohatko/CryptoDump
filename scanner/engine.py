"""Оркестратор: собирает компоненты и крутит фоновые задачи.

Поток данных:

    REST bootstrap (universe.py) ──► InstrumentState × N + CandleBook.seed
    WS public:   trades, books*, liquidation-orders ─┐
    WS business: candle1m ───────────────────────────┴► WsRouter ──► InstrumentState
    DepthGuard (раз в 1 c): протух / порван seqId → переподписка books
                            (REST /market/books только при depth.source == "rest")

    eval  (каждые engine.eval_interval_s): evaluate_instrument() → AlertRouter.publish
                                           + DropMonitor (эпизоды просадки) → History (SQLite)
    reload (engine.reload_interval_s):     ConfigWatcher → _apply_config
    state  (engine.state_save_interval_s): state.json
    dashboard (dashboard.enabled):         веб-интерфейс на aiohttp, поднимается до бутстрапа

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
from dataclasses import replace
from typing import Callable

from .aio import sleep_or_stop
from .alerts import AlertRouter
from .config import Config, ConfigWatcher
from .depth_guard import DepthGuard
from .depth_scheduler import DepthScheduler
from .evaluate import STATUS_LOW_SCORE, STATUS_NO_BOOK, STATUS_THIN_BOOK, DropSeen, evaluate_instrument
from .history import History
from .instrument import InstrumentState
from .market import OkxRest
from .models import Alert, DropHit
from .monitor import DropMonitor
from .okx_ws import OkxPublicStream, Subscription, liquidations, per_instrument, split_by_endpoint
from .paper import PaperBook
from .state import load_state, state_payload, write_state
from .universe import bootstrap_candles, build_universe
from .ws_router import WsRouter

log = logging.getLogger(__name__)

# сколько ждать первый snapshot после подписки, прежде чем сторож начнёт чинить слот
BOOK_GRACE_S = 5.0
# OKX отдаёт сделку за ~0.1 c; с опозданием в секунды вход по цене алерта уже не тот
FEED_LAG_WARN_S = 3.0
FEED_LAG_LOG_EVERY_S = 300.0
# с такой очередью в канале ждать её рассасывания дольше, чем переподключиться
FEED_LAG_RECONNECT_S = 20.0
FEED_LAG_RECONNECT_EVERY_S = 120.0


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
        self.monitor = DropMonitor(gap_s=cfg.dashboard.episode_gap_s)
        # база открывается в run(), а не здесь: тесты создают Engine без файловой системы
        self.history: History | None = None
        self.paper = PaperBook(cfg)
        self._paper_task: asyncio.Task | None = None
        self.phase = "старт"
        self.last_eval_at = 0.0
        self.last_eval_ms = 0.0
        self.feed_lag_s: float | None = None
        self._lag_warned_at = 0.0
        self._lag_reconnect_at = 0.0
        # main.py пересоздаёт движок, если остановка запрошена как перезапуск (дашборд)
        self.restart_requested = False

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
        self._open_history()
        # дашборд поднимаем до бутстрапа: загрузка вселенной идёт десятки секунд,
        # и всё это время интерфейс должен отвечать
        dashboard = self._start_dashboard()
        try:
            self.phase = "загрузка инструментов"
            await self._bootstrap()
            self._restore_state()
            self.paper.load(self.history)
            self.replay_paper()
            self.phase = "работает"
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
            self.phase = "остановка"
            log.info("останавливаюсь…")
            for task in tasks:
                task.cancel()
            if self._paper_task is not None:
                self._paper_task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            self._stop.set()
            if dashboard is not None:
                dashboard.cancel()
                await asyncio.gather(dashboard, return_exceptions=True)
            await self.alerts.close()
            await self.rest.close()
            await self._save_state()
            self._close_history()
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

    def request_restart(self) -> None:
        """Остановиться так, чтобы main.py собрал движок заново с конфигом из файла."""
        self.restart_requested = True
        self._stop.set()

    @property
    def started_at(self) -> float:
        return self._started_at

    @property
    def streams(self) -> list[OkxPublicStream]:
        return list(self._streams)

    @property
    def static_books(self) -> set[str]:
        return set(self._static_books)

    def has_book(self, inst_id: str) -> bool:
        return self._has_book(inst_id)

    # ---------- дашборд и история ----------

    def replay_paper(self, *, reset: bool = False) -> bool:
        """Прогнать виртуальный счёт по истории алертов (``reset`` — начать с нуля по текущим правилам)."""
        if self._paper_task is not None and not self._paper_task.done():
            return False
        if reset:
            self.paper.reset()
        # без _on_task_done: сбой прогона (нет REST) не должен останавливать сканирование
        self._paper_task = asyncio.create_task(self.paper.replay(self.rest, self.states), name="paper-replay")
        return True

    def _start_dashboard(self) -> asyncio.Task | None:
        if not self.cfg.dashboard.enabled:
            return None
        from .dashboard import Dashboard  # aiohttp.web нужен только при включённом дашборде

        # без _on_task_done: сбой веб-сервера не должен останавливать сканирование
        return asyncio.create_task(Dashboard(self).run(self._stop), name="dashboard")

    def _open_history(self) -> None:
        if not self.cfg.dashboard.enabled or not self.cfg.dashboard.history_path:
            return
        try:
            self.history = History(self.cfg.dashboard.history_path)
            if "jsonl" in self.cfg.alerts.sinks:
                self.history.import_jsonl(self.cfg.alerts.jsonl_path)
        except Exception:  # noqa: BLE001 — без истории сканер работает, дашборд покажет пусто
            log.exception("не удалось открыть историю %s", self.cfg.dashboard.history_path)
            self.history = None

    def _close_history(self) -> None:
        if self.history is None:
            return
        self._record(lambda h: [h.add_episode(ep) for ep in self.monitor.drain()])
        self.history.close()
        self.history = None

    def _record(self, write: Callable[[History], object]) -> object:
        if self.history is None:
            return None
        try:
            return write(self.history)
        except Exception:  # noqa: BLE001 — сбой записи истории не должен ронять оценку
            log.exception("ошибка записи истории")
            return None

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
                tick_sz=inst.tick_sz,
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
        try:
            self.paper.update(self.states, now=now)
        except Exception:  # noqa: BLE001 — виртуальный счёт не должен мешать оценке
            log.exception("ошибка сопровождения виртуального счёта")
        # прогрев: без сделок и ликвидаций CVD/стакан пустые, сигнатуры дадут ложный ноль
        warmup_left = self.cfg.engine.warmup_s - (now - self._started_at)
        if warmup_left > 0:
            log.debug("прогрев: ещё %.0f c", warmup_left)
            return []

        # считаем синхронно (это дёшево), а отправку алертов делаем параллельно:
        # вебхук может тормозить до таймаута, и он не должен задерживать оценку
        # остальных инструментов
        started = time.perf_counter()
        alerts: list[Alert] = []
        scheduler = self.depth_scheduler
        for state in list(self.states.values()):
            try:
                # в режимах ondemand/top стакан есть не у всех — тогда оценка
                # вернёт DropSeen, и мы запросим слот вместо пропуска дампа
                result = evaluate_instrument(
                    state,
                    self.cfg,
                    now=now,
                    depth_available=self._has_book(state.inst_id),
                    observe=lambda drop, status, detail, iid=state.inst_id: self._observe(
                        iid, drop, status, detail, now=now
                    ),
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

        closed = self.monitor.sweep(now=now)
        if closed:
            self._record(lambda h: [h.add_episode(ep) for ep in closed])
        self._check_feed_lag(alerts, now=now)
        self.last_eval_at = now
        self.last_eval_ms = (time.perf_counter() - started) * 1000.0

        if alerts:
            await asyncio.gather(*(self._publish(alert) for alert in alerts), return_exceptions=True)
        return alerts

    def _check_feed_lag(self, alerts: list[Alert], *, now: float) -> None:
        lag = self.router.trade_lag.value(now=now)
        self.feed_lag_s = lag
        if self.depth_scheduler is not None:
            self.depth_scheduler.throttle(lag, now=now)
        if lag is None or lag < FEED_LAG_WARN_S:
            return
        if lag >= FEED_LAG_RECONNECT_S and now - self._lag_reconnect_at >= FEED_LAG_RECONNECT_EVERY_S:
            # Очередь уже в канале: урезанные стаканы её не отменяют, OKX дошлёт всё
            # опоздавшее. Новое соединение начинает с текущих данных.
            self._lag_reconnect_at = now
            for stream in self._streams:
                if any(sub.channel == "trades" for sub in stream.subscriptions):
                    stream.request_reconnect(f"сделки опаздывают на {lag:.0f} c, сбрасываю очередь")
            self.router.trade_lag.reset()
        for alert in alerts:
            if alert.plan is not None:
                alert.plan.warnings.append(f"данные биржи опаздывают на {lag:.0f} c — сверьте цену перед входом")
        if now - self._lag_warned_at >= FEED_LAG_LOG_EVERY_S:
            self._lag_warned_at = now
            log.warning(
                "сделки приходят с опозданием %.1f c (норма ~0.1 c): канал до OKX не успевает — "
                "медленный VPN/сеть; слоты стакана урезаются автоматически, иначе нужен канал быстрее",
                lag,
            )

    def _observe(self, inst_id: str, drop: DropHit, status: str, detail: str, *, now: float) -> None:
        scheduler = self.depth_scheduler
        if scheduler is not None and status == STATUS_NO_BOOK:
            thin = scheduler.thin(inst_id, now=now)
            if thin is not None:
                # стакан отпущен на паузу за тонкость — «ждёт стакан» в истории было бы неправдой
                status, detail = STATUS_THIN_BOOK, thin
        self.monitor.observe(inst_id, drop, status, detail, now=now)
        if scheduler is None or not scheduler.has_book(inst_id):
            return
        if status == STATUS_LOW_SCORE:
            # стакан живой и ликвидный, дамп продолжается, ждём сигнатур — слот продлеваем
            scheduler.want(inst_id, drop_pct=drop.drop_pct, now=now)
        elif status == STATUS_THIN_BOOK:
            # стакан полный и свежий (неготовый оценка сообщает как «ждёт стакан»), но тонкий
            scheduler.reject(inst_id, detail, now=now)

    async def _publish(self, alert: Alert) -> None:
        published = await self.alerts.publish(alert)
        if published:
            alert_id = self._record(lambda h: h.add_alert(alert))
            if isinstance(alert_id, int) and alert_id > 0:
                self.paper.on_alert(alert_id, alert.to_dict())
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

    def apply_config(self, fresh: Config) -> None:
        """Применить конфиг на лету (дашборд); поля «только с рестартом» останутся прежними."""
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
            fresh.dashboard = replace(self.cfg.dashboard, episode_gap_s=fresh.dashboard.episode_gap_s)
        self.cfg = fresh
        # компоненты держат ссылку на конфиг — без этого они работали бы на старых порогах
        self.router.cfg = fresh
        self.paper.cfg = fresh
        self.depth.cfg = fresh
        if self.depth_scheduler is not None:
            self.depth_scheduler.cfg = fresh
        self.alerts.cooldown.cooldown_s = fresh.alerts.cooldown_s
        self.monitor.gap_s = fresh.dashboard.episode_gap_s
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

    async def _save_state(self) -> None:
        if not self.cfg.engine.persist_state:
            return
        now = time.time()
        payload = state_payload(
            trades={iid: s.trades for iid, s in self.states.items()},
            liq={iid: s.liq for iid, s in self.states.items()},
            cooldown=self.alerts.cooldown,
            now=now,
        )
        # в поток — только запись файла: снимок выше собран в event loop, пока данные не меняются
        await asyncio.to_thread(write_state, self.cfg.engine.state_path, payload, now=now)

    async def _state_loop(self) -> None:
        # запись в отдельном потоке: файл небольшой, но цикл оценки ждать не должен
        interval = max(5.0, self.cfg.engine.state_save_interval_s)
        while not self._stop.is_set():
            await sleep_or_stop(interval, self._stop)
            if self._stop.is_set():
                return
            try:
                await self._save_state()
            except Exception:  # noqa: BLE001 — персист не должен ронять сканер
                log.exception("ошибка сохранения состояния")
