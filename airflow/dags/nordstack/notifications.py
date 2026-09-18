"""Email notifications for the pipeline.

Sending goes through smtplib with credentials from the environment, so the whole
notification path stays in version control rather than in airflow.cfg. A missing
mailbox is logged, never raised -- see _send.
"""

from __future__ import annotations

import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from nordstack.config import resolve_dbt_target

log = logging.getLogger(__name__)


def _recipients() -> list[str]:
    raw = os.environ.get("ALERT_EMAIL_TO", "")
    return [address.strip() for address in raw.split(",") if address.strip()]


def _send(subject: str, html: str) -> None:
    """Send one message. Never raises."""
    user = os.environ.get("SMTP_USER")
    password = os.environ.get("SMTP_PASSWORD")
    recipients = _recipients()

    # A missing mailbox is a configuration problem, not a data one: failing here would
    # mask the pipeline error the email was reporting.
    if not (user and password and recipients):
        log.warning(
            "Email not sent -- SMTP_USER, SMTP_PASSWORD or ALERT_EMAIL_TO is unset. "
            "Subject was: %s",
            subject,
        )
        return

    message = MIMEMultipart()
    message["From"] = user
    message["To"] = ", ".join(recipients)
    message["Subject"] = subject
    message.attach(MIMEText(html, "html"))

    host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    port = int(os.environ.get("SMTP_PORT", "587"))

    try:
        with smtplib.SMTP(host, port, timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(user, password)
            smtp.sendmail(user, recipients, message.as_string())
        log.info("Notification sent to %s", ", ".join(recipients))
    except Exception:
        # Swallowed deliberately, and only here: a failure callback that raises would
        # hide the original failure. Logged in full so it stays diagnosable.
        log.exception("Failed to send notification: %s", subject)


def _context_rows(context: dict) -> str:
    dag_run = context.get("dag_run")
    task_instance = context.get("task_instance")
    rows = {
        "DAG": context.get("dag").dag_id if context.get("dag") else "unknown",
        "Run": getattr(dag_run, "run_id", "unknown"),
        "Logical date": str(context.get("logical_date") or context.get("execution_date")),
        "Target": resolve_dbt_target(context),
    }
    if task_instance is not None:
        rows["Failed task"] = task_instance.task_id
        rows["Try"] = f"{task_instance.try_number} of {task_instance.max_tries + 1}"
        if getattr(task_instance, "log_url", None):
            rows["Logs"] = f'<a href="{task_instance.log_url}">open in Airflow</a>'
    return "".join(
        f"<tr><td style='padding:4px 12px 4px 0'><b>{k}</b></td>"
        f"<td style='padding:4px 0'>{v}</td></tr>"
        for k, v in rows.items()
    )


def notify_success(context: dict) -> None:
    """DAG-level on_success_callback."""
    dag_id = context.get("dag").dag_id if context.get("dag") else "nordstack"
    _send(
        subject=f"[Airflow] {dag_id} succeeded",
        html=(
            "<h3 style='color:#1a7f37;margin:0 0 8px'>Pipeline succeeded</h3>"
            "<p style='margin:0 0 12px'>MySQL &rarr; dlt &rarr; PostgreSQL raw &rarr; dbt. "
            "All models built and all tests passed.</p>"
            f"<table style='font-family:system-ui;font-size:14px'>{_context_rows(context)}</table>"
        ),
    )


def notify_failure(context: dict) -> None:
    """DAG-level on_failure_callback.

    Fires once per failed DAG run, after retries are exhausted -- not once per failed
    attempt. A transient error that the retry policy absorbs produces no email, which
    is the point: an alert that fires on recoverable errors trains people to ignore it.
    """
    dag_id = context.get("dag").dag_id if context.get("dag") else "nordstack"
    exception = context.get("exception")
    detail = (
        f"<pre style='background:#f6f8fa;padding:10px;border-radius:6px;"
        f"white-space:pre-wrap'>{exception}</pre>"
        if exception
        else ""
    )
    _send(
        subject=f"[Airflow] {dag_id} FAILED",
        html=(
            "<h3 style='color:#cf222e;margin:0 0 8px'>Pipeline failed</h3>"
            "<p style='margin:0 0 12px'>Retries were exhausted. Downstream models were "
            "not published.</p>"
            f"<table style='font-family:system-ui;font-size:14px'>{_context_rows(context)}</table>"
            f"{detail}"
        ),
    )


def notify_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """DAG-level sla_miss_callback.

    Separate from failure: an SLA miss means the run is LATE, not broken. On a
    five-minute schedule, lateness is the first symptom of a pipeline that will start
    overlapping with itself.
    """
    _send(
        subject=f"[Airflow] {dag.dag_id} missed its SLA",
        html=(
            "<h3 style='color:#9a6700;margin:0 0 8px'>SLA missed</h3>"
            "<p>The run is still going or finished late. It is not failing -- but on a "
            "5-minute schedule, sustained lateness leads to overlapping runs.</p>"
            f"<p><b>Tasks:</b> {task_list}</p>"
        ),
    )
