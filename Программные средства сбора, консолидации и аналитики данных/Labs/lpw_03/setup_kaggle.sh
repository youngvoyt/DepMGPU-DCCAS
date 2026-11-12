#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "$0")"
token="${1:-$HOME/.kaggle/kaggle.json}"
if [[ ! -f "$token" ]]; then
  echo 'Укажите личный kaggle.json: bash setup_kaggle.sh /путь/kaggle.json' >&2
  exit 1
fi
airflow_uid="${AIRFLOW_UID:-$(id -u)}"
if [[ -f .env ]]; then
  configured_uid=$(awk -F= '$1 == "AIRFLOW_UID" {print $2}' .env | tail -n 1)
  airflow_uid="${configured_uid:-$airflow_uid}"
fi
[[ "$airflow_uid" =~ ^[0-9]+$ ]] || { echo 'Некорректный AIRFLOW_UID' >&2; exit 1; }
mkdir -p dags/.kaggle/logs dags/data superset screenshots
install -m 600 -- "$token" dags/.kaggle/kaggle.json
chmod 700 dags/.kaggle
chmod 775 dags/data superset
if [[ "$(id -u)" != "$airflow_uid" ]]; then
  sudo chown -R "$airflow_uid":0 dags/.kaggle dags/data
fi
echo 'Личный токен настроен. Его содержимое не выводится и не включается в Git.'
