#!/usr/bin/env bash
# Build PhotoCraft Web using Trunk + pack_wasm.py (parallel 6 MiB .wasm.part* chunks + progress bar).
set -euo pipefail
# shellcheck source=../env.sh
. "$(dirname "${BASH_SOURCE[0]}")/../env.sh"
HERE="$ROOT/packaging/web"

if [[ "${1:-}" != "--skip-build" ]]; then
  command -v trunk >/dev/null || { echo "error: trunk not found (cargo install trunk --locked)" >&2; exit 1; }
  rm -rf "$ROOT/dist/web"
  (cd "$ROOT/apps/photocraft-web" && trunk build --release)
fi

APP="$ROOT/dist/web"
[[ -f "$APP/index.html" ]] || { echo "error: $APP/index.html missing; run without --skip-build" >&2; exit 1; }
if grep -Eq '(src|href)="/[^/]' "$APP/index.html"; then
  echo "error: $APP/index.html has root-absolute URLs; it would break when served from a sub-path" >&2
  exit 1
fi

MAX_FILE_BYTES=$((10#${PHOTOCRAFT_WEB_MAX_FILE_BYTES:-25165824})) # 24 MiB
NAME="photocraft-web-$VERSION"
WORK="$CARGO_TARGET_DIR/web-package"
rm -rf "$WORK"
mkdir -p "$WORK/$NAME/app"
cp -R "$APP/." "$WORK/$NAME/"
cp "$HERE/_headers" "$HERE/.htaccess" "$WORK/$NAME/"
cp "$HERE/README.md" "$WORK/$NAME/HOSTING.md"
copy_docs "$WORK/$NAME"

size() { wc -c <"$1" | tr -d ' '; }
largest=0
while IFS= read -r file; do
  bytes=$(size "$file")
  [[ "$bytes" -le "$largest" ]] || largest=$bytes
  if [[ "$bytes" -gt "$MAX_FILE_BYTES" ]]; then
    echo "error: ${file#"$WORK/$NAME/"} is $bytes bytes, over the $MAX_FILE_BYTES-byte limit" >&2
    exit 1
  fi
done < <(find "$WORK/$NAME" -type f)
echo "largest file in the site: $largest bytes (limit $MAX_FILE_BYTES)"

rm -rf "$ROOT/dist/$NAME" "$DIST/$NAME.zip"
mkdir -p "$ROOT/dist"
cp -R "$WORK/$NAME" "$ROOT/dist/$NAME"
echo "staged directory $ROOT/dist/$NAME"

if command -v zip >/dev/null 2>&1; then
  (cd "$WORK" && zip -qr9 "$DIST/$NAME.zip" "$NAME")
  echo "wrote $DIST/$NAME.zip"
  ls -lh "$DIST/$NAME.zip"
fi
