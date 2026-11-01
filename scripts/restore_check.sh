#!/bin/sh
# Ежемесячная проверка восстановления: последний бэкап разворачивается во временную базу
# и сверяется число строк основных таблиц. Запуск: docker compose run --rm backup /scripts/restore_check.sh
set -eu
latest=$(ls -1t /backups/scanner_*.dump | head -n 1)
echo "проверяю $latest"
psql -h db -U scanner -d postgres -c "DROP DATABASE IF EXISTS scanner_restore_check" >/dev/null
psql -h db -U scanner -d postgres -c "CREATE DATABASE scanner_restore_check" >/dev/null
pg_restore -h db -U scanner -d scanner_restore_check "$latest"
for t in tokens trades clusters cluster_links launches alerts user_marks; do
  n=$(psql -h db -U scanner -d scanner_restore_check -tAc "SELECT count(*) FROM $t")
  echo "$t: $n"
done
psql -h db -U scanner -d postgres -c "DROP DATABASE scanner_restore_check" >/dev/null
echo "восстановление OK"
