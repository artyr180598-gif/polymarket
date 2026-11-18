#!/usr/bin/env bash
# =============================================================================
# Однокомандная установка PolyBot TWAP Hybrid на Ubuntu 22.04 / 24.04 VPS
# Подходит для запуска с iPhone / iPad через приложение Termius
# Использование: sudo bash deploy/install_vps.sh
# =============================================================================
set -euo pipefail

APP_DIR="/opt/polybot"
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "=== [1/5] Установка системных пакетов Python 3.12 и venv ==="
apt-get update -qq
apt-get install -y -qq python3 python3-venv python3-pip ufw curl

echo "=== [2/5] Копирование файлов бота в ${APP_DIR} ==="
mkdir -p "${APP_DIR}/data"
cp -r "${SRC_DIR}/bot" "${SRC_DIR}/main.py" "${SRC_DIR}/requirements.txt" "${SRC_DIR}/.env.example" "${APP_DIR}/"

if [ ! -f "${APP_DIR}/.env" ]; then
  cp "${APP_DIR}/.env.example" "${APP_DIR}/.env"
  RAND_PIN="$(tr -dc '0-9' </dev/urandom | head -c 6)"
  sed -i "s/^DASHBOARD_PIN=.*/DASHBOARD_PIN=${RAND_PIN}/" "${APP_DIR}/.env"
  chmod 600 "${APP_DIR}/.env"
  echo "Создан файл ${APP_DIR}/.env (сгенерирован PIN панели: ${RAND_PIN})"
else
  echo "Сохранён существующий файл ${APP_DIR}/.env"
fi

echo "=== [3/5] Создание виртуального окружения и установка polymarket-client ==="
python3 -m venv "${APP_DIR}/.venv"
"${APP_DIR}/.venv/bin/pip" install -q --upgrade pip
"${APP_DIR}/.venv/bin/pip" install -q -r "${APP_DIR}/requirements.txt"

echo "=== [4/5] Проверка геоблокировки IP сервера Polymarket ==="
curl -s https://polymarket.com/api/geoblock || true
echo ""

echo "=== [5/5] Настройка автозапуска 24/7 (systemd) ==="
cat >/etc/systemd/system/polybot.service <<EOF
[Unit]
Description=PolyBot TWAP Hybrid (Polymarket Trading Bot)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${APP_DIR}
EnvironmentFile=${APP_DIR}/.env
ExecStart=${APP_DIR}/.venv/bin/python ${APP_DIR}/main.py
Restart=always
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable polybot
systemctl restart polybot

SERVER_IP="$(curl -s -4 ifconfig.me || echo '<IP_СЕРВЕРА>')"
CUR_PIN="$(grep '^DASHBOARD_PIN=' "${APP_DIR}/.env" | cut -d= -f2)"

echo ""
echo "================================================================="
echo " PolyBot успешно установлен и работает 24/7 в режиме PAPER!"
echo " Панель для iPhone/iPad (Safari): http://${SERVER_IP}:8080"
echo " Ваш секретный PIN панели:        ${CUR_PIN}"
echo " Редактировать настройки / ключи: nano ${APP_DIR}/.env"
echo " Перезапуск после правок:         systemctl restart polybot"
echo " Смотреть живые логи:             journalctl -u polybot -f"
echo "================================================================="
