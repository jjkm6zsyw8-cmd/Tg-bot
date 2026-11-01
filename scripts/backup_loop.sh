#!/bin/sh
# Ежедневный бэкап базы, хранится 14 последних копий.
set -eu
while true; do
  f="/backups/scanner_$(date -u +%Y%m%d_%H%M).dump"
  if pg_dump -h db -U scanner -Fc scanner -f "$f.tmp"; then
    mv "$f.tmp" "$f"
    echo "backup ok: $f"
  else
    echo "backup FAILED" >&2
    rm -f "$f.tmp"
  fi
  ls -1t /backups/scanner_*.dump 2>/dev/null | tail -n +15 | xargs -r rm -f
  sleep 86400
done
