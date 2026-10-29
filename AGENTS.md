# AGENTS.md — что нужно знать агенту до первой правки

CryptoDump — асинхронный сканер откупов после дампа на фьючерсах OKX (SWAP + FUTURES,
USDT). Слушает публичный WS, считает просадку по окнам 1m…2h, проверяет стакан и
поток агрессора, шлёт алерт в консоль / JSONL / вебхук / Telegram. Python 3.11+,
единственная runtime-зависимость — `aiohttp`. Подробности расчётов — `PLAN.md`,
запуск для человека — `README.md`.

## Команды

Запускать из корня репозитория (пакет не устанавливается через pip).

```bash
python -m venv .venv && .venv\Scripts\activate      # Windows; на Linux: source .venv/bin/activate
pip install -r requirements-dev.txt                 # aiohttp + pytest + ruff

pytest                                              # все тесты, без сети, < 1 c
ruff check                                          # линтер (E, F, W, I; длина строки 120)

python -m scanner.main --print-config               # итоговый конфиг
python -m scanner.diag -s BTC,ETH --seconds 30      # живой поток: что доходит до детекции
python -m scanner.main --dry-run -s BTC,ETH         # сканер без Telegram/вебхука; дашборд на :8787
python -m scanner.main --dry-run -s BTC,ETH --port 8788   # второй экземпляр рядом с рабочим

python tools/liquidity_probe.py --rounds 5          # кто проходит фильтр 1M USDT на 20 уровнях (старое правило, без band_pct)
python tools/channel_cost.py --seconds 60           # трафик trades/books5/books на живом рынке
python tools/backtest.py --hours 24 --top 10 --out tools/out/backtest_24h.csv
                                                    # бэктест по 1m-свечам (приближение: без стакана и CVD)
python tools/backtest.py --hours 6 -s BTC,ETH,SOL   # пробный прогон на трёх инструментах
```

`tools/` — исследовательские скрипты с сетью, вне тестов; сканер их не импортирует.
Перед сменой `depth.*` порогов или `book_slots` перепроверьте ими цифры из `PLAN.md` §5.5.
`backtest.py` — **приближение только по свечам 1m**: стакан (стена покупателя), поток
агрессора (CVD) и ликвидации в историю не попадают, OKX их не отдаёт. Сигнал там — одна
просадка по порогу окна без подтверждения сигнатурами, зато пороги и правила `paper`
берутся из конфига, поэтому цифры сравнимы с вкладкой «Счёт». Вариант B — сигналы, где
свеча дампа закрылась зелёной (грубый заменитель подтверждения).

Перед сдачей работы: `ruff check` и `pytest` зелёные. CI (`.github/workflows/ci.yml`)
гоняет то же самое на 3.11 и 3.12.

Проверка на живых данных без ожидания дампа — временный конфиг с порогами 0.01%:
`{"windows": {"1m": 0.01, "5m": 0.01}, "engine": {"persist_state": false}}` и
`python -m scanner.diag -c <файл> -s BTC,ETH --seconds 25`. В выводе должно быть
`стакан подключён по требованию` и `уровней=20 полный=True источник=ws`.

## Карта модулей (`scanner/`)

Поток данных:

```
REST (universe.py) ──► InstrumentState × N ◄── WsRouter ◄── OkxPublicStream × 2 (public, business)
                              │                                   ▲
       eval каждые 2 c        ▼                                   │ subscribe / unsubscribe / resubscribe books
   evaluate_instrument() ─► Alert ─► AlertRouter ─► sinks         │
              │  │                 └──► History (SQLite)          │
              │  └─ observe(статус) ─► DropMonitor ─► History     │
              └─ DropSeen (просадка, стакана нет) ─► DepthScheduler ──┘
                                                     DepthGuard (ресинк seqId / протухших) ──┘

Dashboard (aiohttp.web, :8787) читает Engine / History / LogBuffer, пишет конфиг → Engine.apply_config
```

| Модуль | Ответственность |
|---|---|
| `engine.py` | оркестратор: создаёт компоненты, запускает задачи (ws, depth, depth-scheduler, eval, reload, state, duration), решает, у кого есть стакан (`_has_book`, `_guard_owns`), горячая перезагрузка (`_apply_config`) |
| `instrument.py` | `InstrumentState` — всё состояние одного инструмента: `book`, `candles`, `trades`, `drops`, `liq`, `last_trade`, `turnover`, `price()` |
| `universe.py` | `build_universe` → `[(Instrument, оборот_24ч)]` (фильтр quote / `exclude_patterns` / `min_turnover_usdt` / `top_n`), `bootstrap_candles` |
| `ws_router.py` | `WsRouter`: таблица «канал → обработчик» (`trades`, `candle1m`, `books`, `books5`, `liquidation-orders`); разрыв `seqId` → колбэк `on_book_gap` |
| `depth_scheduler.py` | `DepthScheduler` (режим `ondemand`): слоты `books` под дампы, TTL `book_ttl_s`, вытеснение самой слабой просадки не раньше `book_min_hold_s`, пауза `book_retry_s` для отпущенных (`cooling()`); тонкий стакан — `reject()`: слот отпускается сразу, пауза `book_thin_retry_s` без исключений |
| `depth_guard.py` | `DepthGuard`: переподписка `books` при разрыве цепочки или протухании (backoff 5 → 120 c на инструмент, сброс после 60 c здоровья); REST-поллинг только при `depth.source == "rest"` |
| `evaluate.py` | `evaluate_instrument()` — чистая функция: просадка → фильтр ликвидности → сигнатуры → `Alert` (с `plan`) / `DropSeen` / `None` |
| `plan.py` | `build_plan()` — чистая функция: план лонга по алерту (вход = ask, стоп под стеной/дном, цели по откату дампа, R:R, размер от `trade.risk_usdt`, округление до `tickSz`) |
| `paper.py` | виртуальный счёт: `open_trade` / `step` (сделка по бару low/high/close: стоп раньше тейка, безубыток после 1-й цели, выход по времени, ликвидация), `PaperBook` (весь баланс в одну позицию, реинвест, `replay` прошлых алертов по свечам `history-candles`, `reconstruct_plan` для алертов без плана, `summary` — матожидание) |
| `detection.py` | три сигнатуры (`bid_wall`, `cvd_fade`, `liq_decay`), `passed_liquidity`, `score_ok` |
| `orderbook.py` | `OrderBook`: REST-снимок, WS snapshot + инкременты по `seqId`, `summary()`, поиск и подтверждение стены |
| `windows.py` | `TradeWindow` (секундные бакеты: CVD, точный high), `CandleBook`, `DropWindows`, `LiquidationTracker` |
| `okx_ws.py` | `OkxPublicStream`: желаемые подписки vs отправленные на соединении, `flush` раз в 0.4 c в бюджете 400/ч, пауза и повтор на `60014`, сторож молчащего канала, ping, реконнект с backoff; `budget()`; `split_by_endpoint` |
| `market.py` | REST-клиент OKX (instruments, tickers, books, candles), ретраи 429/5xx |
| `parsers.py` | сырые ответы OKX → модели, перевод контрактов в монеты (`ctVal`) |
| `models.py` | датаклассы `Instrument`, `Trade`, `Candle`, `Level`, `BookSummary`, `DropHit`, `Signals`, `TradePlan`, `Alert` (+ текст, `reasons()` и `to_dict`) |
| `config.py` | датаклассы конфига и дефолты, загрузка JSON, `ConfigWatcher`, `restart_required_changes` |
| `alerts.py` / `telegram.py` | `AlertRouter` + `Cooldown`, синки; Telegram Bot API (HTML, нарезка, 429/5xx, лимитер) |
| `state.py` | `state.json`: бакеты трейдов, ликвидации, кулдаун (атомарная запись, TTL 15 мин) |
| `env.py` | загрузка `.env` без зависимостей (переменные процесса приоритетнее) |
| `aio.py` | `sleep_or_stop` — ожидание с выходом по `asyncio.Event` |
| `dashboard.py` | веб-дашборд: `Dashboard(engine).run(stop)`, JSON API `/api/*`, middleware защиты, `apply_patch` (правка конфига), `validate_patch`, управление (`/api/control/*`) |
| `web/index.html` | фронтенд дашборда: один файл, vanilla JS, опрос API раз в 3 c; всё из данных экранируется `esc()` |
| `history.py` | `History`: SQLite (таблицы `alerts`, `episodes`), фильтры, `stats`, импорт `alerts.jsonl` в пустую базу |
| `monitor.py` | `DropMonitor` / `DropEpisode`: эпизод просадки от первого попадания до `episode_gap_s` тишины |
| `logbuffer.py` | `LogBuffer` — кольцевой буфер лога; `install()` один раз на процесс |
| `diag.py`, `main.py`, `__main__.py` | диагностика и CLI; `main.py` крутит цикл перезапуска движка (`Engine.restart_requested`) |

Тесты — `tests/test_<модуль>.py`, общие фабрики — `tests/conftest.py`
(`make_candles`, `make_book_summary`, фикстура `now`).

## Режимы стакана (`depth.mode`) — главное, что легко сломать

Канал `books` стоит ~28 МБ/ч на инструмент (на 511 перпетуалах ~13 ГБ/ч), `trades` на
всём рынке — ~300 МБ/ч. Поэтому по умолчанию (`top_n = 0`) сканируется весь рынок, а
стакан подключается выборочно:

- **`ondemand`** (по умолчанию): eval находит просадку у инструмента без стакана →
  `evaluate_instrument` возвращает `DropSeen` → `DepthScheduler.want()` → на следующем
  тике слот и `subscribe(books)`. Слот живёт `book_ttl_s` (180 c) после последнего
  `want()`; слотов `book_slots` (20); при нехватке вытесняется самая слабая просадка.
- **`top`**: `books` подписан сразу на `book_slots` самых оборотных
  (`Engine._static_books`); планировщика нет; у остальных `DropSeen` только логируется.
- **`all`**: `books` на всех (только для маленькой вселенной, `-s BTC,ETH`).
- `depth.source == "rest"`: стакан опрашивается REST у всех, `mode` не влияет.

`DepthGuard` чинит только стаканы, которыми кто-то владеет (`Engine._guard_owns`):
статические из `top` и слоты планировщика старше `BOOK_GRACE_S` (5 c, ждём первый snapshot).
Иначе сторож вернул бы `books` инструменту без слота в обход планировщика.

**Бюджет подписок OKX: 480 запросов `subscribe`/`unsubscribe` в час на соединение.**
Сверх — `60014 Requests too frequent`, а после затяжного шторма OKX молча перестаёт слать
данные (соединение живо, сделок нет — сканер слепнет). Ресинк стакана = 2 запроса
(повторный `subscribe` snapshot не даёт). Поэтому:

- `OkxPublicStream.subscribe/unsubscribe/resubscribe` ничего не шлют, а меняют желаемое
  состояние; отправляет только `flush` (раз в 0.4 c, один запрос-пачка, ≤ 400/ч). Не
  добавляйте прямых `send_json` с `op` подписки в обход `_send`.
- `DepthGuard` — экспоненциальный backoff на инструмент; `DepthScheduler` — `book_min_hold_s`
  и `book_retry_s`. Любой новый источник переподписок должен иметь свой предел частоты.
- Тонкий стакан (полный и свежий, но ниже `min_notional_usdt`) → `Engine._observe` →
  `DepthScheduler.reject`: слот отпускается на ближайшем тике и `book_thin_retry_s` не
  выдаётся, даже если дамп углубился. Раньше такие монеты гоняли подписку по кругу каждые
  `book_ttl_s` и выедали бюджет. Поэтому неготовый стакан (нет snapshot, протух, неполный)
  `evaluate_instrument` сообщает как «ждёт стакан» + `DropSeen`, а не «тонкий» — иначе
  ликвидную монету отправили бы на паузу до первого snapshot.
- Расход виден в `/api/status` → `streams[].budget` и в шапке дашборда.

**Пропускная способность канала.** Через медленный VPN (~150 КБ/с) 20 стаканов `books`
забивают соединение, и сделки приходят с опозданием 4–20 c — цены в алертах устаревают.
`WsRouter.trade_lag` (медиана «сейчас − ts самой свежей сделки пачки» за 30 c) →
`Engine._check_feed_lag`: предупреждение в плане при ≥ 3 c, лог раз в 5 мин и
`DepthScheduler.throttle` — лимит слотов × 2/3 (не чаще 30 c, не меньше 2), +1 раз в 120 c
при задержке < 1 c. Лимит сейчас — `DepthScheduler.limit()`, а не `book_slots` напрямую.
При ≥ 20 c очередь уже в канале и сама не рассосётся (наблюдали рост до 69 c после урезания)
→ `stream.request_reconnect()` у потока с `trades`, не чаще раза в 2 мин.

## Дашборд — что легко сломать

- **Безопасность.** Через API можно менять пороги и остановить сканер. Middleware `_guard`:
  на loopback проверяет `Host` (DNS rebinding), у изменяющих запросов требует
  `Content-Type: application/json` и свой `Origin` (CSRF с чужой страницы), при заданном
  токене (`dashboard.token_env`) — `Authorization: Bearer`. На не-loopback адресе без токена
  сервер не стартует. Новые изменяющие ручки — только `POST`/`PUT` за этим middleware.
- **Правка конфига накладывается на файл, а не на `engine.cfg`.** В действующем конфиге есть
  CLI-переопределения (`-s`, `--no-telegram`), и запись `dump_config()` в файл навсегда
  зашила бы их и все дефолты. Поэтому `apply_patch`: `read_config_file` → `merge_config` →
  `write_config_file` → `watcher.mark_seen()` → `engine.apply_config`. «Нужен перезапуск»
  сообщается только для полей, которые правка реально трогает (`_touches`), иначе `-s`
  выглядел бы как изменённый `market`.
- **`windows` в `merge_config` заменяется целиком** — иначе выключить окно было бы нельзя.
- **Причины отсева — через `observe`** в `evaluate_instrument` (статусы `STATUS_*`). Функция
  остаётся чистой: колбэк необязательный, тесты без него не меняются.
- **`History` открывается в `Engine.run()`, не в `__init__`**: тесты создают `Engine` без
  файловой системы. Ошибки записи истории глотаются в `_record` — оценка важнее.
- **Задача дашборда не под `_on_task_done`**: падение веб-сервера (занят порт) не должно
  останавливать сканирование. Дашборд стартует до `_bootstrap`, чтобы отвечать во время загрузки.
- **Перезапуск** = `Engine.request_restart()` → `main.py` заново читает конфиг из файла и
  применяет CLI-переопределения. Python-модули при этом не перезагружаются: после правки кода
  нужен перезапуск процесса.
- `diag.py` выключает дашборд: он работает рядом с рабочим сканером, порт и база заняты.

## Виртуальный счёт (`paper.py`, вкладка «Счёт»)

- Одна функция `step` для живого счёта (бар = сделки с `checked_to` из `TradeWindow.range_since`,
  каждые 2 c в `_evaluate_all`) и для прогона истории (бар = минутная свеча). Не заводите
  отдельную логику для одного из режимов — результаты перестанут быть сравнимыми.
- Порядок сделок на счёте важен (реинвест), поэтому живой `on_alert` игнорируется, пока
  `replay` не закончил (`ready=False`) — прогон сам подберёт этот алерт из истории.
  Занятость и баланс считаются на момент алерта (`_busy(at)`, `balance(at)`).
- Свеча, в которую попал алерт, при прогоне пропускается: её high мог быть до входа.
- Сделки — в таблице `paper_trades` (payload = `PaperTrade.to_dict()`); «Пересчитать с нуля»
  (`/api/control/paper-reset` → `Engine.replay_paper(reset=True)`) чистит таблицу и прогоняет
  все алерты по текущим правилам `paper.*`.

## Инварианты (каждый оплачен багом — не ломать)

1. **`sz` у OKX — в контрактах.** Любой номинал = `px × sz × ctVal`. Касается стакана,
   сделок и ликвидаций. Без этого фильтр 1M USDT врёт в 1/ctVal раз (у BTC — в 100).
2. **WS-инкремент нельзя накладывать на REST-снимок.** Разрыв `prevSeqId ≠ seqId` или
   отсутствие `seqId` → `apply_ws_update` возвращает `False` → переподписка `books`
   (она даёт чистый snapshot). Не чинить через REST.
3. **Свежесть инкрементального стакана — по `book.updated_at`, не по `level.ts`.** Стоячая
   стена не получает апдейтов, и фильтр по уровню выкинул бы её ровно на `wall_persist_s`.
4. **Хранить глубже, чем считать:** `store_levels` (400, максимум OKX) в памяти, срез до
   `levels` (20) только в `summary()`. Фильтр ликвидности — по полосе ±`band_pct`% от середины
   спреда (`BookSummary.liquidity_notional`), а не по 20 уровням: у плотных стаканов (HYPE, PEPE)
   20 уровней — сотые доли процента от цены, у редких (CASHCAT) — проценты и дальние заявки.
   Стена и bid/ask в алерте по-прежнему по `levels`.
5. **Стена:** лучший бид не стена; база — среднее (не медиана) уровней ниже лучшего бида;
   подтверждение по времени `wall_persist_s`, а не по числу снимков (WS даёт ~10 снимков/с).
   Отсчёт идёт внутри `summary()`, поэтому его надо вызывать регулярно (это делает eval).
6. **Окна 1m/5m — по сделкам, 30m+ — по свечам** (`cvd.precise_windows_until_s`). Свеча,
   пересекающая границу окна, завышала максимум почти вдвое.
7. **Свечи только на `/ws/v5/business`**, остальное на `/ws/v5/public`, порт 443. Неизвестный
   канал в пачке `subscribe` OKX отклоняет целиком — `split_by_endpoint` их отсекает.
8. **`unsubscribe` убирает подписку из `stream.subscriptions` даже без соединения** — иначе
   после реконнекта `books` вернётся инструменту без слота.
9. **Секреты только из окружения/`.env`.** В конфиге — имена переменных (`token_env`,
   `chat_id_env`). Токен не логировать.
10. **Запросы подписки — только через `flush` и в бюджете** (см. выше). Шторм переподписок
    уже однажды ослепил сканер на всём рынке.
11. **План сделки — только лонг, уровни из дампа** (`plan.py`): стоп ниже входа, цели выше
    входа, иначе `build_plan` возвращает `None` и алерт уходит без плана. Цены — вниз до
    `tickSz`, размер в контрактах = монеты / `ctVal`.

## Как вносить изменения

- **Новое поле конфига:** датакласс в `config.py` с дефолтом и комментарием → то же
  значение в `scanner/config.example.json` (тест `test_example_config_matches_defaults`
  сверяет пример с дефолтами) → если поле нельзя менять на лету, добавить его в
  `Config.restart_required_changes` и откат в `Engine._apply_config`.
- **Новый компонент с конфигом:** держит ссылку `self.cfg`; добавить переприсваивание в
  `Engine._apply_config`, иначе горячая перезагрузка его не затронет.
- **Новый WS-канал:** обработчик в `WsRouter._handlers` и имя в `PUBLIC_CHANNELS` /
 `BUSINESS_CHANNELS` в `okx_ws.py`.
- **Новый порог в дашборде:** строка в `FIELDS` в `web/index.html`; если поле «только с
 рестартом» — ещё `isRestart()` там же и `RESTART_KEYS` / `_touches` в `dashboard.py`.
- **Новая ручка API:** метод `Dashboard`, маршрут в `build_app`, ответ через `_json()`
 (он вычищает `inf`/`nan`, которые ломают `JSON.parse`), тест в `tests/test_dashboard.py`
 через `aiohttp.test_utils.TestServer` внутри `asyncio.run`.
- **Время:** функции с логикой принимают `now: float | None = None` — тесты передают
  фиксированное `now` (`1_700_000_000.0`) вместо `time.time()`. Сохранять этот стиль.
- **Тесты без сети.** Потоки и REST подменяются фейками с нужными методами (см.
  `tests/test_depth_guard.py`, `tests/test_universe.py`). Async-код — через `asyncio.run`
  внутри обычного теста (pytest-asyncio не используется).
- **Стиль:** комментарии, докстринги и логи — по-русски; комментарий объясняет «почему»,
  а не пересказывает код. Широкие `except Exception` только в фоновых циклах и с
  `# noqa: BLE001 — <почему>`.
- **Кодировка:** UTF-8 без BOM. В Windows PowerShell 5 `Set-Content -Encoding utf8`
  дописывает BOM — для правки файлов используйте инструменты редактора или Python.

## Известные ограничения и долги

- Одно WS-соединение на эндпоинт: при очень широкой вселенной может упереться в лимит
  подписок OKX (M2 в `PLAN.md`).
- `liquidation-orders` OKX публикует не полностью и только для SWAP — сигнатура `liq_decay`
  подтверждающая. У квартальных фьючерсов её не бывает.
- Нет replay записанного потока и калибровки порогов на истории (M1/M3).
- Прежние `multi_probe.py` и `stability.py` заменены флагом `--rounds` у
  `tools/liquidity_probe.py`; `tools/channel_cost.py` переписан заново (исходник был утерян).
