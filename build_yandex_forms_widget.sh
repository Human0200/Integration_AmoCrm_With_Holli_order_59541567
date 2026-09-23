#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
WIDGET_DIR="$ROOT_DIR/widget/yandex_forms_widget"
OUTPUT_ZIP="$ROOT_DIR/yandex_forms_widget.zip"

if [[ ! -d "$WIDGET_DIR" ]]; then
  echo "Widget directory not found: $WIDGET_DIR" >&2
  exit 1
fi

rm -f "$OUTPUT_ZIP"
cd "$WIDGET_DIR"
zip -qr "$OUTPUT_ZIP" . -x './widget.zip'

echo "Built: $OUTPUT_ZIP"
