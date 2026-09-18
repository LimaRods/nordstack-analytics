"""Which dbt target a DAG run builds into.

Priority: the run's `dbt_target` param, then DBT_TARGET, then `dev`.

That order makes production a deliberate act. A scheduled run, a local test and a plain
trigger all land on the environment's default; reaching `prod` means selecting it on the
trigger form or setting DBT_TARGET in the deployment. `prod` is never a fallback.
"""

from __future__ import annotations

import os

VALID_TARGETS = ("dev", "prod")


def environment_default() -> str:
    """The target for runs that do not choose one. Never `prod` unless deployment says so."""
    target = os.environ.get("DBT_TARGET", "dev")
    return target if target in VALID_TARGETS else "dev"


def resolve_dbt_target(context: dict) -> str:
    """Resolve the target for one DAG run from its Airflow context.

    `params` first, then the run conf directly, which covers callers that pass conf
    without a matching param (the CLI's `-c`, the REST API).
    """
    params = context.get("params") or {}
    if isinstance(params, dict) and params.get("dbt_target") in VALID_TARGETS:
        return str(params["dbt_target"])

    conf = getattr(context.get("dag_run"), "conf", None) or {}
    if isinstance(conf, dict) and conf.get("dbt_target") in VALID_TARGETS:
        return str(conf["dbt_target"])

    return environment_default()
