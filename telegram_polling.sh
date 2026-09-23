#!/usr/bin/env bash

set -u

API_IP="149.154.167.220"
TOKEN="${TELEGRAM_BOT_TOKEN:-}"
LOG_FILE="${TELEGRAM_POLLING_LOG:-/opt/telegram-polling/logs/telegramApp.log}"
STATE_FILE="${TELEGRAM_POLLING_STATE:-/opt/telegram-polling/telegram_polling.offset}"

if [[ -z "$TOKEN" ]]; then
    printf 'TELEGRAM_BOT_TOKEN is not configured.\n' >&2
    exit 1
fi

mkdir -p "$(dirname "$LOG_FILE")"
touch "$LOG_FILE"
offset=0
if [[ -f "$STATE_FILE" ]]; then
    offset=$(cat "$STATE_FILE")
fi

log() {
    local level="$1"
    local message="$2"
    local data="${3:-}"
    {
        printf '[%s] [%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$level" "$message"
        [[ -n "$data" ]] && printf '%s\n' "$data"
        printf '%s\n' '--------------------------------------------------------------------------------'
    } >> "$LOG_FILE"
}

log INFO "Telegram polling started" "{\"offset\":$offset}"

while true; do
    response=$(curl --noproxy '*' --resolve "api.telegram.org:443:$API_IP" \
        --silent --show-error --max-time 40 \
        --data-urlencode "offset=$offset" \
        --data-urlencode 'timeout=25' \
        --data-urlencode 'allowed_updates=["message","edited_message","channel_post","edited_channel_post","my_chat_member"]' \
        "https://api.telegram.org/bot${TOKEN}/getUpdates" 2>&1)
    curl_status=$?

    if [[ $curl_status -ne 0 ]]; then
        log ERROR "Telegram cURL error" "$(printf '%s' "$response" | jq -Rs .)"
        sleep 3
        continue
    fi

    if [[ "$(printf '%s' "$response" | jq -r '.ok // false' 2>/dev/null)" != "true" ]]; then
        log ERROR "Telegram API error" "$(printf '%s' "$response" | jq -c . 2>/dev/null || printf '%s' "$response")"
        sleep 3
        continue
    fi

    while IFS= read -r update; do
        [[ -z "$update" ]] && continue
        update_id=$(printf '%s' "$update" | jq -r '.update_id // 0')
        if [[ "$update_id" =~ ^[0-9]+$ ]] && (( update_id > 0 )); then
            offset=$((update_id + 1))
            printf '%s' "$offset" > "$STATE_FILE"
        fi

        log INFO "Получено обновление Telegram" "$update"

        message=$(printf '%s' "$update" | jq -c '(.message // .edited_message // .channel_post // .edited_channel_post) // empty')
        command=$(printf '%s' "$message" | jq -r '.text // ""' 2>/dev/null | awk '{print $1}' | sed 's#^/##; s/@.*//' | tr '[:upper:]' '[:lower:]')
        if [[ "$command" == "getthemes" ]]; then
            context=$(printf '%s' "$message" | jq -c '{
                chat_id: (.chat.id // null),
                chat_title: (.chat.title // .chat.username // null),
                chat_type: (.chat.type // null),
                message_thread_id: (.message_thread_id // null),
                topic_name: (.forum_topic_created.name // null),
                from_id: (.from.id // null),
                from_username: (.from.username // null),
                text: (.text // "")
            }')
            log INFO "Получена команда /getthemes" "$context"
        fi
    done < <(printf '%s' "$response" | jq -c '.result[]?')
done
