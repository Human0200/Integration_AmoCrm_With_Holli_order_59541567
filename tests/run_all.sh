#!/bin/sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

php tests/test_add_student_helpers.php
php tests/test_index_helpers.php
php tests/test_yandex_forms_helpers.php
php tests/test_telegram_helpers.php
php tests/test_telegram_topics_helpers.php
php tests/test_zoom_helpers.php
php tests/test_zoom_salesbot_helpers.php
sh tests/test_yandex_forms_api.sh

echo "ALL TESTS PASSED"
