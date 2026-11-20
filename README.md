# PolyBot

Бумажный, по желанию боевой бот для крипто-окон Polymarket Up/Down.
Стратегия снята с кошелька [mo-money](https://polymarket.com/profile/0x32ed2e546b187ca15e2841edc82b22c713cf8ec3).
Разбор с цифрами: [docs/STRATEGY_ANALYSIS.md](docs/STRATEGY_ANALYSIS.md).

Бот ставит лестницу пост-онли заявок на покупку от 1¢ до 98¢, обе стороны вместе не дороже 99¢,
пары дешевле $1 меняет на $1, остаток забирает после официального итога. Продаж нет.
По умолчанию только BTC, окна 5 и 15 минут.

Старт всегда в бумажном режиме. Боевой режим не включается без ключей и проверки геоблока.

## Переменные

Имена прежние. Новые не обязательны.

```
TRADING_MODE=paper          # paper | live
DRY_RUN=false               # true принудительно оставляет бумажный режим
BANKROLL_USD=2750           # стартовый бумажный банк
STRATEGY_PRESET=mo_money    # lockin и hybrid больше не отдельные стратегии
ENABLED_ASSETS=BTC
ENABLED_TIMEFRAMES=5m,15m
ORDER_CLIP_USD=16
MAX_WINDOW_RISK_USD=140
MAX_TOTAL_EXPOSURE_USD=420
MAX_DAILY_LOSS_USD=160
MAX_CONSECUTIVE_LOSSES=10
MIN_MERGE_SHARES=1
ONCHAIN_GAS_USD=0           # вычитается на live merge/redeem, если нет релеера
DB_PATH=/data/polybot.db
DASHBOARD_PIN=
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
```

Боевые ключи те же: `POLYMARKET_PRIVATE_KEY`, `POLYMARKET_WALLET_ADDRESS`,
ключи релеера и билдера. Без релеера газ задаётся `ONCHAIN_GAS_USD`, иначе в учёте он 0.

`STRATEGY_PRESET=lockin` или `hybrid` больше не переключает логику: оба значения
пишут предупреждение и работают как mo-money. Отдельных движков под ними не было.

Бумажный капитал и позиции пишутся в SQLite и поднимаются после рестарта.
Чистый старт — удалить файл базы.

## Запуск

Railway: Dockerfile, healthcheck `/api/health`, том на `/data`.
Регион `eu-west1`. Процесс слушает `PORT`.

Локально:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m unittest discover -s tests
python main.py
```

Панель на телефоне: Safari → URL сервиса → PIN → «На экран Домой».
Инструкция: [GUIDE_IPHONE_RU.md](GUIDE_IPHONE_RU.md).

## Учёт

Капитал = свободный USDC + залог в заявках + оценка позиций по лучшему биду.
Реализованный PnL включает комиссию и газ один раз. Нереализованный — остаток
до капитала. Мейкерская комиссия 0. Тейкерская, если понадобится:

`shares × feeRate × price × (1 − price)`, округление half-up до 5 знаков.
До 20.11.2026 23:59 UTC ставка крипты 0.07, затем BTC 5m/15m — 0.05, остальная крипта — 0.01.
Если Gamma отдала ставку рынка, берётся она.
