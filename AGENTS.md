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
python -m scanner.main --dry-run -s BTC,ETH         # сканер без Telegram/вебхука

python tools/liquidity_probe.py --rounds 5          # кто проходит фильтр 1M USDT и насколько стабильно
python tools/channel_cost.py --seconds 60           # трафик trades/books5/books на живом рынке
```

`tools/` — исследовательские скрипты с сетью, вне тестов; сканер их не импортирует.
Перед сменой `depth.*` порогов или `book_slots` перепроверьте ими цифры из `PLAN.md` §5.5.

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
              │                                                   │
              └─ DropSeen (просадка, стакана нет) ─► DepthScheduler ──┘
                                                     DepthGuard (ресинк seqId / протухших) ──┘
```

| Модуль | Ответственность |
|---|---|
| `engine.py` | оркестратор: создаёт компоненты, запускает задачи (ws, depth, depth-scheduler, eval, reload, state, duration), решает, у кого есть стакан (`_has_book`, `_guard_owns`), горячая перезагрузка (`_apply_config`) |
| `instrument.py` | `InstrumentState` — всё состояние одного инструмента: `book`, `candles`, `trades`, `drops`, `liq`, `last_trade`, `turnover`, `price()` |
| `universe.py` | `build_universe` → `[(Instrument, оборот_24ч)]` (фильтр quote / `exclude_patterns` / `min_turnover_usdt` / `top_n`), `bootstrap_candles` |
| `ws_router.py` | `WsRouter`: таблица «канал → обработчик» (`trades`, `candle1m`, `books`, `books5`, `liquidation-orders`); разрыв `seqId` → колбэк `on_book_gap` |
| `depth_scheduler.py` | `DepthScheduler` (режим `ondemand`): слоты `books` под дампы, TTL `book_ttl_s`, вытеснение самой слабой просадки |
| `depth_guard.py` | `DepthGuard`: переподписка `books` при разрыве цепочки или протухании (кулдаун 5 c); REST-поллинг только при `depth.source == "rest"` |
| `evaluate.py` | `evaluate_instrument()` — чистая функция: просадка → фильтр ликвидности → сигнатуры → `Alert` / `DropSeen` / `None` |
| `detection.py` | три сигнатуры (`bid_wall`, `cvd_fade`, `liq_decay`), `passed_liquidity`, `score_ok` |
| `orderbook.py` | `OrderBook`: REST-снимок, WS snapshot + инкременты по `seqId`, `summary()`, поиск и подтверждение стены |
| `windows.py` | `TradeWindow` (секундные бакеты: CVD, точный high), `CandleBook`, `DropWindows`, `LiquidationTracker` |
| `okx_ws.py` | `OkxPublicStream`: подписки чанками, ping, реконнект с backoff, `subscribe` / `unsubscribe` / `resubscribe` на лету; `split_by_endpoint` |
| `market.py` | REST-клиент OKX (instruments, tickers, books, candles), ретраи 429/5xx |
| `parsers.py` | сырые ответы OKX → модели, перевод контрактов в монеты (`ctVal`) |
| `models.py` | датаклассы `Instrument`, `Trade`, `Candle`, `Level`, `BookSummary`, `DropHit`, `Signals`, `Alert` (+ текст и `to_dict`) |
| `config.py` | датаклассы конфига и дефолты, загрузка JSON, `ConfigWatcher`, `restart_required_changes` |
| `alerts.py` / `telegram.py` | `AlertRouter` + `Cooldown`, синки; Telegram Bot API (HTML, нарезка, 429/5xx, лимитер) |
| `state.py` | `state.json`: бакеты трейдов, ликвидации, кулдаун (атомарная запись, TTL 15 мин) |
| `env.py` | загрузка `.env` без зависимостей (переменные процесса приоритетнее) |
| `aio.py` | `sleep_or_stop` — ожидание с выходом по `asyncio.Event` |
| `diag.py`, `main.py`, `__main__.py` | диагностика и CLI |

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

## Инварианты (каждый оплачен багом — не ломать)

1. **`sz` у OKX — в контрактах.** Любой номинал = `px × sz × ctVal`. Касается стакана,
   сделок и ликвидаций. Без этого фильтр 1M USDT врёт в 1/ctVal раз (у BTC — в 100).
2. **WS-инкремент нельзя накладывать на REST-снимок.** Разрыв `prevSeqId ≠ seqId` или
   отсутствие `seqId` → `apply_ws_update` возвращает `False` → переподписка `books`
   (она даёт чистый snapshot). Не чинить через REST.
3. **Свежесть инкрементального стакана — по `book.updated_at`, не по `level.ts`.** Стоячая
   стена не получает апдейтов, и фильтр по уровню выкинул бы её ровно на `wall_persist_s`.
4. **Хранить глубже, чем считать:** `store_levels` (100) в памяти, срез до `levels` (20)
   только в `summary()`.
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

## Как вносить изменения

- **Новое поле конфига:** датакласс в `config.py` с дефолтом и комментарием → то же
  значение в `scanner/config.example.json` (тест `test_example_config_matches_defaults`
  сверяет пример с дефолтами) → если поле нельзя менять на лету, добавить его в
  `Config.restart_required_changes` и откат в `Engine._apply_config`.
- **Новый компонент с конфигом:** держит ссылку `self.cfg`; добавить переприсваивание в
  `Engine._apply_config`, иначе горячая перезагрузка его не затронет.
- **Новый WS-канал:** обработчик в `WsRouter._handlers` и имя в `PUBLIC_CHANNELS` /
  `BUSINESS_CHANNELS` в `okx_ws.py`.
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
- Слот выдан, но стакан ещё не прошёл фильтр (`authoritative=False` до первого snapshot) →
  оценка вернёт `None`, `want()` не вызовется, и слот проживёт только `book_ttl_s` с момента
  выдачи. На практике snapshot приходит за < 1 c.
- `liquidation-orders` OKX публикует не полностью и только для SWAP — сигнатура `liq_decay`
  подтверждающая. У квартальных фьючерсов её не бывает.
- Нет replay записанного потока и калибровки порогов на истории (M1/M3).
- Прежние `multi_probe.py` и `stability.py` заменены флагом `--rounds` у
  `tools/liquidity_probe.py`; `tools/channel_cost.py` переписан заново (исходник был утерян).
