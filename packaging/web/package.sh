#!/usr/bin/env bash
# Build the browser version and stage/zip it:
#   $ROOT/dist/photocraft-web-<version>/  and  $DIST/photocraft-web-<version>.zip
#
# Usage: packaging/web/package.sh [--skip-build]
set -euo pipefail
# shellcheck source=../env.sh
. "$(dirname "${BASH_SOURCE[0]}")/../env.sh"
HERE="$ROOT/packaging/web"

if [[ "${1:-}" != "--skip-build" ]]; then
  command -v trunk >/dev/null || { echo "error: trunk not found (cargo install trunk --locked)" >&2; exit 1; }
  (cd "$ROOT/apps/photocraft-web" && trunk build --release)
  (cd "$ROOT/apps/photocraft-web-boot" && trunk build --release)
fi

APP="$ROOT/dist/web"       # the app: index.html, wasm-bindgen glue, snippets/, one .wasm
BOOT="$ROOT/dist/web-boot" # the page that ships, and the module that starts the app from parts
for site in "$APP" "$BOOT"; do
  [[ -f "$site/index.html" ]] || { echo "error: $site/index.html missing; run without --skip-build" >&2; exit 1; }
  if grep -Eq '(src|href)="/[^/]' "$site/index.html"; then
    echo "error: $site/index.html has root-absolute URLs; it would break when served from a sub-path" >&2
    exit 1
  fi
done

MAX_FILE_BYTES=$((10#${PHOTOCRAFT_WEB_MAX_FILE_BYTES:-25165824})) # 24 MiB
PART_BYTES=$((10#${PHOTOCRAFT_WEB_PART_BYTES:-10485760}))         # 10 MiB chunks for smooth Cloudflare upload
if [[ "$PART_BYTES" -le 0 ]] || [[ "$PART_BYTES" -gt "$MAX_FILE_BYTES" ]]; then
  echo "error: a part of $PART_BYTES bytes doesn't fit the $MAX_FILE_BYTES-byte file limit" >&2
  exit 1
fi

WASM=""
for wasm in "$APP"/*_bg.wasm "$APP"/*.wasm; do
  [[ -f "$wasm" ]] || continue
  if [[ -n "$WASM" && "$WASM" != "$wasm" ]]; then
    echo "error: more than one .wasm in $APP; remove it and run without --skip-build" >&2
    exit 1
  fi
  WASM="$wasm"
done
[[ -n "$WASM" ]] || { echo "error: no .wasm in $APP" >&2; exit 1; }
STEM="$(basename "${WASM%_bg.wasm}" .wasm)"

# Trunk v0.21+ hashes the JS glue by its own content hash rather than the .wasm content hash.
GLUE=""
for js in "$APP"/photocraft-web*.js "$APP"/*.js; do
  [[ -f "$js" ]] || continue
  if [[ -n "$GLUE" && "$GLUE" != "$js" ]]; then
    echo "error: more than one JS glue file in $APP ($GLUE vs $js)" >&2
    exit 1
  fi
  GLUE="$js"
done
[[ -n "$GLUE" ]] || { echo "error: JS glue missing in $APP" >&2; exit 1; }

NAME="photocraft-web-$VERSION"
WORK="$CARGO_TARGET_DIR/web-package"
rm -rf "$WORK"
mkdir -p "$WORK/$NAME/app"
cp -R "$BOOT/." "$WORK/$NAME/"
if [[ -d "$APP/icons" ]]; then
  cp -R "$APP/icons" "$WORK/$NAME/"
fi
# The boot module imports the app's glue from app/photocraft-web.js, which in turn imports
# wasm-bindgen JS snippets relative to itself (./snippets/... -> app/snippets/...).
cp "$GLUE" "$WORK/$NAME/app/photocraft-web.js"
if [[ -d "$APP/snippets" ]]; then
  cp -R "$APP/snippets" "$WORK/$NAME/app/snippets"
  cp -R "$APP/snippets" "$WORK/$NAME/snippets"
fi

gzip -9 -n -c "$WASM" | (cd "$WORK/$NAME/app" && split -b "$PART_BYTES" - "$STEM.part-")
{
  echo "photocraft-parts 1"
  for part in "$WORK/$NAME/app/$STEM".part-*; do basename "$part"; done
} >"$WORK/$NAME/app/parts.txt"

size() { wc -c <"$1" | tr -d ' '; }
wasm_bytes=$(size "$WASM")
gzip_bytes=$(cat "$WORK/$NAME/app/$STEM".part-* | wc -c | tr -d ' ')
part_count=$(($(wc -l <"$WORK/$NAME/app/parts.txt") - 1))
echo "$(basename "$WASM"): $wasm_bytes bytes ($((wasm_bytes / 1048576)) MiB), shipped as $gzip_bytes bytes of gzip in $part_count part(s) of up to $PART_BYTES"

cp "$HERE/_headers" "$HERE/.htaccess" "$WORK/$NAME/"
cp "$HERE/README.md" "$WORK/$NAME/HOSTING.md"
copy_docs "$WORK/$NAME"

if [[ -n "${PHOTOCRAFT_WEB_FONTS_DIR:-}" ]]; then
  FONTS_MANIFEST="$PHOTOCRAFT_WEB_FONTS_DIR/fonts/manifest.txt"
  [[ -f "$FONTS_MANIFEST" ]] || { echo "error: $FONTS_MANIFEST missing (PHOTOCRAFT_WEB_FONTS_DIR)" >&2; exit 1; }
  mkdir -p "$WORK/$NAME/fonts"
  cp "$FONTS_MANIFEST" "$WORK/$NAME/fonts/manifest.txt"
  awk -F' [|] ' '!/^[[:space:]]*(#|$)/ { print $3; if ($6 != "") print $6 }' "$FONTS_MANIFEST" | sort -u | while IFS= read -r f; do
    case "$f" in
      "" | /* | *..* | *\\* | *://*) echo "error: $FONTS_MANIFEST: '$f' is not a relative path inside the folder" >&2; exit 1 ;;
    esac
    mkdir -p "$WORK/$NAME/$(dirname "$f")"
    cp "$PHOTOCRAFT_WEB_FONTS_DIR/$f" "$WORK/$NAME/$f"
  done
fi

largest=0
while IFS= read -r file; do
  bytes=$(size "$file")
  [[ "$bytes" -le "$largest" ]] || largest=$bytes
  if [[ "$bytes" -gt "$MAX_FILE_BYTES" ]]; then
    echo "error: ${file#"$WORK/$NAME/"} is $bytes bytes, over the $MAX_FILE_BYTES-byte limit (Cloudflare's per-file cap is 25 MiB)" >&2
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
