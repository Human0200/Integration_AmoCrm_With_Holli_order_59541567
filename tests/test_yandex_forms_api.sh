#!/bin/sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
TOKENS_FILE="$ROOT_DIR/tokens.json"
TOKENS_BACKUP="/tmp/yandex_forms_tokens.json.backup"

if [ -f "$TOKENS_FILE" ]; then
  cp "$TOKENS_FILE" "$TOKENS_BACKUP"
fi

cleanup() {
  if [ -f "$TOKENS_BACKUP" ]; then
    cp "$TOKENS_BACKUP" "$TOKENS_FILE"
    rm -f "$TOKENS_BACKUP"
  else
    rm -f "$TOKENS_FILE"
  fi
}

trap cleanup EXIT

cat > "$TOKENS_FILE" <<EOF
{"time": $(date +%s), "access_token": "dummy", "refresh_token": "dummy"}
EOF

run_case() {
  QUERY_JSON="$1"
  METHOD="$2"
  php -r '
$query = json_decode(base64_decode($argv[1]), true) ?: [];
parse_str(http_build_query($query), $_GET);
$_POST = [];
$_REQUEST = $_GET;
$_SERVER["REQUEST_METHOD"] = $argv[2];
$_SERVER["HTTP_HOST"] = "localhost";
ob_start();
register_shutdown_function(static function () {
    $body = ob_get_contents();
    $payload = [
        "status" => http_response_code(),
        "body" => $body,
    ];
    while (ob_get_level() > 0) {
        ob_end_clean();
    }
    echo json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
});
include getcwd() . "/yandex_forms_api.php";
' "$(printf '%s' "$QUERY_JSON" | base64)" "$METHOD"
}

UNKNOWN=$(run_case '{"action":"ping","lead_id":123}' GET)
MISSING=$(run_case '{"action":"forms"}' GET)

printf '%s' "$UNKNOWN" | grep -q '"status":400'
printf '%s' "$UNKNOWN" | grep -q 'Неизвестное действие'
printf '%s' "$MISSING" | grep -q '"status":400'
printf '%s' "$MISSING" | grep -q 'Не передан lead_id'

echo "OK"
