# PolyBot — Пошаговая инструкция для iPhone / iPad

Бот работает 24/7 в облаке. Управление полностью с телефона: веб-панель + Telegram.

## 1. Создай Telegram-бота (1 минута)

1. Открой [@BotFather](https://t.me/BotFather)
2. `/newbot` → придумай имя и username
3. Скопируй **токен** (вид `123456:ABC-DEF...`)
4. Напиши своему боту любое сообщение
5. Открой [@userinfobot](https://t.me/userinfobot) → скопируй свой **Id** (число)

## 2. Деплой на Railway

1. Зайди на [railway.app](https://railway.app) (войти через GitHub)
2. **New Project → Deploy from GitHub repo** → выбери `polymarket`
3. **Region: eu-west1 (Amsterdam)** — обязательно
4. **Variables** → добавь:

```
TRADING_MODE=paper
DASHBOARD_PIN=123456
TELEGRAM_BOT_TOKEN=токен_от_BotFather
TELEGRAM_CHAT_ID=твой_числовой_id
BANKROLL_USD=2750
STRATEGY=copy_wallet
COPY_WALLET=0x32ed2e546b187ca15e2841edc82b22c713cf8ec3
COPY_SIZE_MODE=exact
DB_PATH=/data/polybot.db
```

5. **Settings → Volumes** → Add Volume → Mount Path: `/data`
6. Дождись зелёного деплоя (1–2 минуты)

## 3. Открой панель на iPhone

1. В Railway → Settings → Domains → Generate Domain
2. Скопируй URL (типа `https://xxx.up.railway.app`)
3. Открой в **Safari**
4. Введи PIN
5. Нажми «Поделиться» → **На экран «Домой»**

Готово — иконка как у приложения.

## 4. Проверь Telegram

Напиши боту:
- `/status` — состояние
- `/pnl` — PnL
- `/pause` / `/resume`
- `/kill` — аварийный стоп

## 5. Что бот делает и режим paper → live

Сейчас бот в **paper** (виртуальные деньги на живом стакане) и копирует кошелёк
`0x32ed…8ec3` один к одному: каждая его покупка/продажа, MERGE, SPLIT и REDEEM
повторяется на демо-счёте тем же числом штук. Карточка «Копирование кошелька» в
панели показывает очередь, задержку и счётчики. Если в копиях появляется
`not enough cash` — увеличь `BANKROLL_USD` (это учебные деньги).

Копирование в live не включается by design. Боевой режим есть только у
`STRATEGY=maker_ladder` — лестницы заявок от 1¢ до 98¢, которая меняет пару
дешевле $1 на доллар.

**Не включай live**, пока бумажные окна не покажут понятный результат и лимиты не будут привычными.

Когда будешь готов:
1. Создай **отдельный** кошелёк только под бота
2. Положи на него pUSD на Polygon
3. В Railway Variables добавь:
   - `POLYMARKET_PRIVATE_KEY`
   - `POLYMARKET_WALLET_ADDRESS`
4. Поставь `TRADING_MODE=live`
5. Начни с маленького банка ($200–300)

## Безопасность

- Никогда не коммить `.env` и приватные ключи
- Live только с отдельного кошелька
- Перед live проверь geoblock: `https://polymarket.com/api/geoblock`
- 50% прибыли выводи на холодный кошелёк

## Альтернатива: VPS

Если Railway неудобен:
```bash
sudo bash deploy/install_vps.sh
```
Потом открой `http://IP:8080` в Safari.
