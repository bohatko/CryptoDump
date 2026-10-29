# CryptoDump

Сканер откупов после дампа на фьючерсах OKX (перпетуалы + квартальные фьючерсы).
Подробности расчётов — в [PLAN.md](PLAN.md), карта кода и правила для разработки (и для
AI-агентов) — в [AGENTS.md](AGENTS.md).

## Быстрый старт

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
pip install -r scanner/requirements.txt   # для разработки: pip install -r requirements-dev.txt

cp .env.example .env            # и впишите TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID

python -m scanner.main --telegram-test                     # проверка бота и чата
python -m scanner.main --print-config                      # посмотреть итоговый конфиг
pytest                                                     # тесты расчётов без сети
ruff check                                                 # линтер
python -m scanner.diag -s BTC,ETH --seconds 30             # что реально доходит до детекции
python -m scanner.main -c scanner/config.example.json
python -m scanner.main -c scanner/config.example.json -s BTC,ETH,SOL --no-telegram
python -m scanner.main --duration 120 --log-level DEBUG    # две минуты и выход
```

Запуск без аргументов использует дефолты из `scanner/config.py` и `.env` из корня проекта.

Запускать из корня проекта, чтобы `python -m scanner.main` нашёл пакет.

## Уведомления в Telegram

1. Создайте бота у [@BotFather](https://t.me/BotFather) и получите токен.
2. Добавьте бота в чат/канал и **дайте ему право писать** (для канала — администратором).
3. Скопируйте `.env.example` в `.env` и заполните:

```
TELEGRAM_BOT_TOKEN=123456789:AA...
TELEGRAM_CHAT_ID=-1002592005579
```

4. Проверьте связку: `python -m scanner.main --telegram-test`.

Как это работает:

- токен читается из окружения по имени переменной (`alerts.telegram.token_env`), а не из
  JSON-конфига — секреты не попадают в репозиторий и логи; `.env` уже в `.gitignore`;
- `.env` ищется в текущей папке, затем в корне проекта; переменные процесса имеют приоритет;
- сообщение содержит инструмент, окно, процент просадки, цену и максимум окна, стакан
  (bid/ask, имбаланс, стена), CVD с taker-ratio и динамику ликвидаций;
- на чат действует кулдаун 5 минут (`alerts.cooldown_s`), поэтому один и тот же дамп не
  приходит повторно;
- несколько адресатов — через запятую в `TELEGRAM_CHAT_ID` или в `alerts.telegram.chat_ids`;
- при 429 бот ждёт столько, сколько просит Telegram, при 5xx — повторяет с backoff;
  если разметка не разобралась, сообщение уходит тем же текстом без форматирования;
- `--dry-run` и `--no-telegram` отключают отправку (удобно для отладки порогов).

## Структура

```
PLAN.md                   подробный план (источники, формулы, риски, roadmap)
AGENTS.md                 карта кода, инварианты, правила изменений
scanner/
  main.py                 CLI (python -m scanner.main)
  __main__.py             python -m scanner
  engine.py               оркестратор: компоненты, фоновые задачи, горячая перезагрузка
  instrument.py           InstrumentState — всё состояние одного инструмента
  universe.py             вселенная из REST (оборот, фильтры) + история свечей
  ws_router.py            разбор WS-сообщений по каналам в InstrumentState
  depth_scheduler.py      стакан по требованию: слоты books под дампы, TTL, вытеснение
  depth_guard.py          ресинк стакана (разрыв seqId, протухание), REST-резерв
  evaluate.py             оценка инструмента: просадка → ликвидность → сигнатуры → алерт
  detection.py            три сигнатуры откупа + скоринг
  config.py               конфиг, дефолты, горячая перезагрузка порогов
  models.py               Instrument / Trade / Candle / BookSummary / Alert
  market.py               REST: инструменты, обороты, стакан, свечи
  parsers.py              разбор ответов OKX + ctVal (контракты → монеты)
  okx_ws.py               публичный WS: подписки на лету, ping/pong, реконнект
  orderbook.py            стакан: инкременты WS по seqId, ctVal, стена + устойчивость
  windows.py              TradeWindow (CVD + точный high), CandleBook, DropWindows
  state.py                персист состояния (CVD, ликвидации, кулдаун)
  alerts.py               кулдаун + синки console/jsonl/webhook/telegram
  telegram.py             Telegram Bot API: формат, ретраи, лимитер
  env.py                  загрузка .env без внешних зависимостей
  aio.py                  sleep_or_stop
  diag.py                 диагностика потока данных
  config.example.json     конфиг с дефолтными порогами (сверяется тестом)
tests/                    pytest, без сети: python -m pytest
tools/
  liquidity_probe.py      сколько инструментов проходит фильтр ликвидности (--rounds N — стабильность)
  channel_cost.py         живой замер трафика каналов trades / books5 / books
pyproject.toml            настройки pytest и ruff
requirements-dev.txt      зависимости для разработки (pytest, ruff)
.github/workflows/ci.yml  ruff + pytest на Python 3.11 и 3.12
.env.example              шаблон переменных окружения (токен бота, chat id)
```

## Что важно знать

- По умолчанию сканируется **весь рынок** (`market.top_n = 0`, минус инструменты с оборотом
  меньше `market.min_turnover_usdt`): сделки и свечи дешёвые (~300 МБ/час на 511 перпетуалах).
- Стакан берётся из WS-канала `books` (400 уровней, инкременты ~100 мс, без логина), но он
  дорогой (~28 МБ/час на инструмент), поэтому подключается **по требованию**
  (`depth.mode: "ondemand"`): когда у инструмента сработало окно просадки, ему выделяется
  один из `depth.book_slots` слотов на `depth.book_ttl_s` секунд. Альтернативы — `"top"`
  (стакан сразу у самых оборотных) и `"all"` (у всех, только для короткого списка `-s`).
  `books5` (5 уровней) фильтр на 20 уровней не проходит; REST `/market/books` — резерв
  при `depth.source: "rest"`.
- **`sz` у OKX — в контрактах, а не в монетах.** Размер уровня и сделки умножается на
  `ctVal` инструмента из `/public/instruments`, иначе фильтр ликвидности и CVD считались бы
  в контрактах (у BTC-USDT-SWAP один контракт = 0.01 BTC, ошибка в 100 раз).
- Свечи живут на отдельном эндпоинте `wss://ws.okx.com:443/ws/v5/business` (на публичном
  `candle1m` возвращает ошибку 60018). Порт 8443 выводится OKX из эксплуатации — используется 443.
- Стена на бидах считается по среднему уровню ниже лучшего бида и должна простоять
  `wall_persist_s` (10 с): на живом стакане верхний уровень держит почти весь объём,
  а медиана занижена пылью.
- Окна 1m и 5m считаются по сделкам (секундные бакеты), а не по 1m-свечам: свеча,
  пересекающая границу окна, завышала максимум почти вдвое по времени.
- `liquidation-orders` у OKX неполный и публикуется только для SWAP — используется как
  подтверждающий сигнал, не триггер; квартальные фьючерсы опираются на стену и CVD.
- Свежесть стакана в WS-режиме проверяется по всему стакану, а не по каждому уровню:
  биржа присылает только изменения, и неподвижная стена иначе считалась бы мёртвой
  ровно через `stale_level_s` — в момент подтверждения `wall_persist_s`.
- Товарных фьючерсов у OKX нет: `SWAP` + `FUTURES` (включая `quarter`) — это крипто-деривативы.
- Пороги окон меняются на лету: правьте `windows` в конфиге, движок перечитает по mtime.
  Смена `market.*`, `depth.levels`, интервала опроса, `depth.mode` и `depth.source` требует
  рестарта (в лог пишется предупреждение, старые значения сохраняются).
