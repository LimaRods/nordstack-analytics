"""Which dbt target a DAG run builds into.

Priority: the run's `dbt_target` param, then DBT_TARGET, then `dev`. `prod` is never a
fallback -- it has to be selected.
"""

from __future__ import annotations

import os

VALID_TARGETS = ("dev", "prod")


def environment_default() -> str:
    """The target for runs that do not choose one."""
    target = os.environ.get("DBT_TARGET", "dev")
    return target if target in VALID_TARGETS else "dev"


def resolve_dbt_target(context: dict) -> str:
    """Resolve the target for one DAG run from its Airflow context."""
    params = context.get("params") or {}
    if isinstance(params, dict) and params.get("dbt_target") in VALID_TARGETS:
        return str(params["dbt_target"])

    # Covers callers that pass conf without a matching param: the CLI's -c, the API.
    conf = getattr(context.get("dag_run"), "conf", None) or {}
    if isinstance(conf, dict) and conf.get("dbt_target") in VALID_TARGETS:
        return str(conf["dbt_target"])

    return environment_default()
