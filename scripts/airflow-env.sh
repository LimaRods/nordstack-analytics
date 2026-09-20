# Sets the environment every Airflow command in this repo needs.
# Usage, from anywhere inside the repo:   source scripts/airflow-env.sh
#
# Without AIRFLOW_HOME set to an absolute path, Airflow falls back to ~/airflow and reads
# whatever config happens to live there. The .env value (./airflow) only resolves from the
# repo root, so it is not enough on its own.

_repo_root="$PWD"
while [ ! -d "$_repo_root/airflow/dags" ] && [ "$_repo_root" != "/" ]; do
    _repo_root="$(dirname "$_repo_root")"
done

if [ "$_repo_root" = "/" ]; then
    echo "airflow-env.sh: run this from inside the repo" >&2
else
    export AIRFLOW_HOME="$_repo_root/airflow"

    # macOS only. Without these, gunicorn workers segfault in a loop on boot:
    # setproctitle and os_log are not fork-safe on recent macOS.
    export OS_ACTIVITY_MODE=disable
    export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

    # SMTP_* and ALERT_EMAIL_TO are read from the process environment by the
    # notification callbacks, so they have to be exported, not just present in .env.
    if [ -f "$_repo_root/.env" ]; then
        set -a
        . "$_repo_root/.env"
        set +a
        export AIRFLOW_HOME="$_repo_root/airflow"   # .env sets a relative value; win over it
    fi

    echo "AIRFLOW_HOME=$AIRFLOW_HOME"
fi

unset _repo_root
