#!/usr/bin/env bash
# Установка Pump.fun Dev Scanner одной командой на чистую Ubuntu (запуск от root):
#   curl -fsSL https://raw.githubusercontent.com/jjkm6zsyw8-cmd/Tg-bot/claude/telegram-bot-spec-fbcq7j/scripts/install.sh | bash
set -euo pipefail
trap 'echo; echo "❌ Ошибка в строке $LINENO. Пришлите этот текст в чат (без токенов)."' ERR

REPO="https://github.com/jjkm6zsyw8-cmd/Tg-bot.git"
BRANCH="claude/telegram-bot-spec-fbcq7j"
DIR="/opt/tg-bot"

ask() {  # ask <переменная> <вопрос> — читает с клавиатуры даже при curl | bash
  local var="$1" prompt="$2" value=""
  while [ -z "$value" ]; do
    read -r -p "$prompt: " value < /dev/tty
  done
  printf -v "$var" '%s' "$value"
}

echo "=== 1/5 Обновляю систему и ставлю Docker ==="
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq && apt-get install -y -qq git curl ca-certificates >/dev/null
if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sh >/dev/null
fi

echo "=== 2/5 Скачиваю бота ==="
if [ -d "$DIR/.git" ]; then
  git -C "$DIR" fetch -q origin "$BRANCH" && git -C "$DIR" checkout -q "$BRANCH" && git -C "$DIR" pull -q
else
  git clone -q -b "$BRANCH" "$REPO" "$DIR"
fi
cd "$DIR"

echo "=== 3/5 Настройки (вставьте значения и нажмите Enter) ==="
if [ -f .env ]; then
  echo "Файл .env уже есть — оставляю как есть. Чтобы ввести заново: rm $DIR/.env и запустите установку снова."
else
  ask BOT_TOKEN "Токен бота от @BotFather (нажмите на токен в сообщении BotFather — скопируется только он)"
  BOT_TOKEN="${BOT_TOKEN%% *}"   # отрезаем приклеившееся время сообщения
  ask ALLOWED "Ваш Telegram user_id (от @userinfobot)"
  ask CHAT "Id группы (начинается с -100)"
  case "$CHAT" in -*) ;; *) CHAT="-$CHAT" ;; esac   # минус часто теряется при копировании
  ask RPC "RPC URL из Helius (https://mainnet.helius-rpc.com/?api-key=...)"
  HELIUS_KEY=""
  case "$RPC" in *api-key=*) HELIUS_KEY="${RPC##*api-key=}" ;; esac
  PG_PASS="$(od -An -tx1 -N16 /dev/urandom | tr -d ' \n')"
  cat > .env <<EOF
BOT_TOKEN=$BOT_TOKEN
ALLOWED_USER_IDS=$ALLOWED
ALERT_CHAT_ID=$CHAT
POSTGRES_PASSWORD=$PG_PASS
PUMPPORTAL_WS=wss://pumpportal.fun/api/data
PUMPPORTAL_API_KEY=
HELIUS_API_KEY=$HELIUS_KEY
RPC_URL=$RPC
REDIRECT_BASE=https://example.com
LOG_LEVEL=INFO
EOF
  chmod 600 .env
fi

echo "=== 4/5 Облегчённые настройки для недорогого VPS ==="
sed -i 's/shared_buffers=1GB/shared_buffers=256MB/' docker-compose.yml
sed -i 's/raw_trades_days: 30/raw_trades_days: 7/' config/config.yaml
sed -i 's/use_direct_pump_link: false/use_direct_pump_link: true/' config/config.yaml

mkdir -p logs backups && chown -R 10001:10001 logs   # контейнеры бота работают под uid 10001

echo "=== 5/5 Запускаю (первая сборка 2–5 минут) ==="
docker compose up -d --build db backup
docker compose up -d --build --force-recreate ingest analytics bot

echo
docker compose ps
echo
echo "Готово. Напишите в группе /start, затем /health."
echo "Логи: cd $DIR && docker compose logs -f bot"
