#!/bin/bash
if [ -z "$1" ]; then
  echo "manca il parametro che dice quale file copiare"
  exit 1
fi

SRC="$1"
DEST="test2025-12-01"

if [ -f "$SRC" ]; then
  echo "Copio $SRC in $DEST"
  cp "$SRC" "$DEST/ToiDea.xml"
else
  echo "$SRC non esiste"
  exit 1
fi
