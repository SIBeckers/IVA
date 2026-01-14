
#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="postgis"
DB_NAME="impacted_values"
WRITER_USER="iva_job"
DDL_PATH_IN_CONTAINER="/ddl/ddl.sql"
WAIT_SECS=60

# Load .env (APP_PGPASSWORD, PGPASSWORD)
if [[ -f ".env" ]]; then
  # shellcheck disable=SC2046
  export $(tr -d '\r' < .env | grep -E '^(APP_PGPASSWORD|PGPASSWORD)=' | xargs)
fi

: "${APP_PGPASSWORD:?APP_PGPASSWORD must be set in .env}"
: "${PGPASSWORD:?PGPASSWORD must be set in .env}"

echo -n "Waiting for ${CONTAINER_NAME} to accept connections"
for i in $(seq 1 $WAIT_SECS); do
  if podman exec "${CONTAINER_NAME}" pg_isready -U "${WRITER_USER}" -d "${DB_NAME}" >/dev/null 2>&1; then
    echo " ... ready."
    break
  fi
  echo -n "."
  sleep 1
  if [[ $i -eq $WAIT_SECS ]]; then
    echo; echo "ERROR: ${CONTAINER_NAME} not ready in ${WAIT_SECS}s"; exit 1
  fi
done

echo "Applying DDL..."
podman exec -i "${CONTAINER_NAME}" psql -U "${WRITER_USER}" -d "${DB_NAME}" \
  -v app_pass="${APP_PGPASSWORD}" \
  -v job_pass="${PGPASSWORD}" \
  -f "${DDL_PATH_IN_CONTAINER}"
echo "DDL applied."
