# Databricks notebook source
import base64

dbutils.widgets.dropdown("env", "-- Select Environment --", ["-- Select Environment --","Accord UAT", "Accord Asia","Accord EUT","Accord US1","Accord US2","Accord US3","Accord US4","Accord EU","Accord EIP"])
dbutils.widgets.text("excluded_job_ids", "")

env = dbutils.widgets.get("env")
if not env or env == "-- Select Environment --":
    dbutils.notebook.exit("No environment selected. Please choose one from the dropdown.")

workspace_dict={"Accord UAT":"3480799742368664","Accord Asia":"3024230481805855","Accord US1":"1812624382804081","Accord US2":"321298446533351","Accord US3":"3564028080680288","Accord US4":"7405612123156444","Accord EU":"878416158213577","Accord EUT":"7405617993543798","Accord EIP":"7405605078129632"}

workspace_id = workspace_dict[env]
print(f"ENV: {env}")
print(f"workspace_id: {workspace_id}")

excluded_job_ids = dbutils.widgets.get("excluded_job_ids").split(",")
excluded_job_ids = [x.strip() for x in excluded_job_ids if x != ""]
print(f"excluded_job_ids: {excluded_job_ids}")

# COMMAND ----------

# Databricks SMTP config
SMTP_CONFIG = {
    "SMTP_HOSTNAME": "smtp.sendgrid.net",
    "SMTP_PASSWORD": "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",
    "SMTP_PORT": "587",
    "SMTP_USER": "apikey",
    "SENDER_EMAIL": "Alert@73strings.com",
}

# List of recipients
recipients = [     
    "etl_developers@73strings.com"
]

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timedelta, timezone

alert_df = spark.sql(f"""
    WITH active_jobs AS (
        -- Most recent record per job, keeping only those not yet deleted
        SELECT job_id, workspace_id, name, tags
        FROM (
            SELECT
                job_id,
                workspace_id,
                name,
                delete_time,
                tags,
                ROW_NUMBER() OVER (
                    PARTITION BY job_id, workspace_id
                    ORDER BY change_time DESC
                ) AS rn
            FROM system.lakeflow.jobs
            WHERE workspace_id = '{workspace_id}'
        )
        WHERE rn = 1
          AND delete_time IS NULL          -- NULL = job still exists; timestamp = deleted
    ),
    latest_runs AS (
        SELECT
            jr.workspace_id,
            jr.job_id,
            aj.name                        AS job_name,
            aj.tags,
            jr.run_id,
            jr.result_state,
            jr.period_start_time           AS start_time,
            jr.period_end_time             AS end_time,
            ROW_NUMBER() OVER (
                PARTITION BY jr.job_id
                ORDER BY jr.period_start_time DESC
            )                              AS rn
        FROM system.lakeflow.job_run_timeline AS jr
        INNER JOIN active_jobs aj
          ON  jr.job_id       = aj.job_id
          AND jr.workspace_id = aj.workspace_id
        WHERE jr.workspace_id   = '{workspace_id}'
          AND jr.period_end_time >= CURRENT_TIMESTAMP() - INTERVAL 90 DAYS
    )
    SELECT
        workspace_id,
        job_id,
        job_name,
        tags,
        result_state,
        start_time,
        end_time,
        CASE
            WHEN result_state IN ('FAILED', 'TIMEDOUT', 'ERROR') THEN 'FAILED'
            WHEN end_time < CURRENT_TIMESTAMP() - INTERVAL 2 HOURS
              OR end_time IS NULL                                 THEN 'STALE'
            ELSE 'OK'
        END AS alert_reason
    FROM latest_runs
    WHERE rn = 1
      AND (
            result_state IN ('FAILED', 'TIMEDOUT', 'ERROR')
         OR end_time    <  CURRENT_TIMESTAMP() - INTERVAL 2 HOURS
         OR end_time    IS NULL
      )
    ORDER BY alert_reason, job_name
""")

# COMMAND ----------

pipeline_alert_df = spark.sql(f"""
    WITH active_pipelines AS (
        SELECT pipeline_id, workspace_id, name, tags
        FROM (
            SELECT
                pipeline_id,
                workspace_id,
                name,
                delete_time,
                tags,
                ROW_NUMBER() OVER (
                    PARTITION BY pipeline_id, workspace_id
                    ORDER BY change_time DESC
                ) AS rn
            FROM system.lakeflow.pipelines
            WHERE workspace_id = '{workspace_id}'
        )
        WHERE rn = 1
          AND delete_time IS NULL
    ),
    latest_updates AS (
        SELECT
            pu.workspace_id,
            pu.pipeline_id              AS job_id,         -- aliased to match jobs schema
            ap.name                     AS job_name,       -- aliased to match jobs schema
            ap.tags,
            pu.update_id                AS run_id,         -- aliased to match jobs schema
            pu.result_state,
            pu.period_start_time        AS start_time,
            pu.period_end_time          AS end_time,
            ROW_NUMBER() OVER (
                PARTITION BY pu.pipeline_id
                ORDER BY pu.period_start_time DESC
            )                           AS rn
        FROM system.lakeflow.pipeline_update_timeline AS pu
        INNER JOIN active_pipelines ap
          ON  pu.pipeline_id  = ap.pipeline_id
          AND pu.workspace_id = ap.workspace_id
        WHERE pu.workspace_id   = '{workspace_id}'
          AND pu.period_end_time >= CURRENT_TIMESTAMP() - INTERVAL 90 DAYS
    )
    SELECT
        workspace_id,
        job_id,
        job_name,
        tags,
        result_state,
        start_time,
        end_time,
        CASE
            WHEN result_state IN ('FAILED', 'CANCELED')    THEN 'FAILED'
            WHEN end_time < CURRENT_TIMESTAMP() - INTERVAL 2 HOURS
              OR end_time IS NULL                          THEN 'STALE'
            ELSE 'OK'
        END AS alert_reason
    FROM latest_updates
    WHERE rn = 1
      AND (
            result_state IN ('FAILED', 'CANCELED')
         OR end_time    <  CURRENT_TIMESTAMP() - INTERVAL 2 HOURS
         OR end_time    IS NULL
      )
    ORDER BY alert_reason, job_name
""")
alert_df = alert_df.unionByName(pipeline_alert_df)

# COMMAND ----------

alert_df = alert_df.filter(~F.trim(F.col("job_id")).isin(excluded_job_ids))

# Recon / org_aggregated_data jobs: use 24-hour staleness window instead of 2 hours
is_extended_threshold_job = (
    F.lower(F.col("job_name")).contains("recon")
    | F.lower(F.col("job_name")).contains("org_aggregated_data")
)
alert_df = alert_df.filter(
    ~(
        is_extended_threshold_job
        & (F.col("alert_reason") == "STALE")
        & (F.col("end_time") >= F.current_timestamp() - F.expr("INTERVAL 24 HOURS"))
    )
)

# Currently running jobs: not stale, just still executing — drop STALE alert
alert_df = alert_df.filter(
    ~(
        (F.col("alert_reason") == "STALE")
        & (F.col("end_time").isNull())
        & (F.col("start_time").isNotNull())
    )
)

# Historical/adhoc jobs: exclude entirely
alert_df = alert_df.filter(
    ~F.lower(F.col("job_name")).contains("historical")
)

# --- Exclude CREDIT_DAG tagged jobs ---
alert_df = alert_df.filter(
    F.col("tags")["dag"].isNull() | (F.col("tags")["dag"] != "CREDIT_DAG")
)

print(f"Jobs to alert on: {alert_df.count()}")
alert_df.display()

# COMMAND ----------

def build_html_email(df) -> str:
    """Convert a Spark DataFrame of failing/stale jobs into a styled HTML email."""
    
    rows = df.collect()
    if not rows:
        return ""

    row_fields = rows[0].asDict().keys()   # safe field lookup

    # Build table rows
    table_rows_html = ""
    for row in rows:
        badge_color = "#c0392b" if row["alert_reason"] == "FAILED" else "#d35400"
        badge_label = row["alert_reason"]
        start_ts    = row["start_time"].strftime("%Y-%m-%d %H:%M UTC") if row["start_time"] else "—"
        end_ts      = row["end_time"].strftime("%Y-%m-%d %H:%M UTC")   if row["end_time"]   else "Still running / never ran"
        result_state = row["result_state"] if "result_state" in row_fields else "—"

        table_rows_html += f"""
        <tr>
          <td style="padding:10px 14px;border-bottom:1px solid #eee;color:#555;font-size:13px;">
            {row["workspace_id"]}
          </td>
          <td style="padding:10px 14px;border-bottom:1px solid #eee;font-weight:600;color:#1a1a2e;">
            {row["job_name"]}
          </td>
          <td style="padding:10px 14px;border-bottom:1px solid #eee;color:#555;font-size:13px;">
            {row["job_id"]}
          </td>
          <td style="padding:10px 14px;border-bottom:1px solid #eee;">
            <span style="background:{badge_color};color:#fff;padding:3px 10px;
                         border-radius:12px;font-size:12px;font-weight:600;">
              {badge_label}
            </span>
          </td>
          <td style="padding:10px 14px;border-bottom:1px solid #eee;color:#555;font-size:13px;">
            {result_state}
          </td>
          <td style="padding:10px 14px;border-bottom:1px solid #eee;color:#555;font-size:13px;">
            {start_ts}
          </td>
          <td style="padding:10px 14px;border-bottom:1px solid #eee;color:#555;font-size:13px;">
            {end_ts}
          </td>
        </tr>"""

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    job_count    = len(rows)

    html = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="UTF-8"/></head>
    <body style="margin:0;padding:0;font-family:Arial,sans-serif;background:#f5f6fa;">
      <table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f6fa;padding:32px 0;">
        <tr><td align="center">
          <table width="780" cellpadding="0" cellspacing="0"
                 style="background:#ffffff;border-radius:8px;
                        box-shadow:0 2px 8px rgba(0,0,0,0.08);overflow:hidden;">

            <!-- Header -->
            <tr>
              <td style="background:#c0392b;padding:24px 32px;">
                <h1 style="margin:0;color:#fff;font-size:22px;font-weight:700;">
                  Databricks Job Alert- {env}
                </h1>
                <p style="margin:6px 0 0;color:rgba(255,255,255,0.85);font-size:14px;">
                  {job_count} job{"s" if job_count != 1 else ""} require{"" if job_count != 1 else "s"} attention
                  &nbsp;·&nbsp; Generated {generated_at}
                </p>
              </td>
            </tr>

            <!-- Table -->
            <tr>
              <td style="padding:24px 32px 8px;">
                <table width="100%" cellpadding="0" cellspacing="0"
                       style="border-collapse:collapse;font-size:14px;">
                  <thead>
                    <tr style="background:#f0f1f5;">
                      <th style="padding:10px 14px;text-align:left;color:#888;
                                 font-size:11px;text-transform:uppercase;letter-spacing:.5px;">
                        Workspace ID
                      </th>
                      <th style="padding:10px 14px;text-align:left;color:#888;
                                 font-size:11px;text-transform:uppercase;letter-spacing:.5px;">
                        Job name
                      </th>
                      <th style="padding:10px 14px;text-align:left;color:#888;
                                 font-size:11px;text-transform:uppercase;letter-spacing:.5px;">
                        Job ID
                      </th>
                      <th style="padding:10px 14px;text-align:left;color:#888;
                                 font-size:11px;text-transform:uppercase;letter-spacing:.5px;">
                        Alert
                      </th>
                      <th style="padding:10px 14px;text-align:left;color:#888;
                                 font-size:11px;text-transform:uppercase;letter-spacing:.5px;">
                        Last state
                      </th>
                      <th style="padding:10px 14px;text-align:left;color:#888;
                                 font-size:11px;text-transform:uppercase;letter-spacing:.5px;">
                        Started
                      </th>
                      <th style="padding:10px 14px;text-align:left;color:#888;
                                 font-size:11px;text-transform:uppercase;letter-spacing:.5px;">
                        Ended
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {table_rows_html}
                  </tbody>
                </table>
              </td>
            </tr>

            <!-- Footer -->
            <tr>
              <td style="padding:20px 32px 28px;color:#aaa;font-size:12px;
                         border-top:1px solid #f0f1f5;margin-top:16px;">
                This alert is generated automatically by your Databricks monitoring job.
                Check your workspace for run details.
              </td>
            </tr>

          </table>
        </td></tr>
      </table>
    </body>
    </html>
    """
    return html

# COMMAND ----------

import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from contextlib import contextmanager

@contextmanager
def create_email_service():
    """
    Opens an SMTP connection with STARTTLS, yields it, then closes it cleanly.
    Using a context manager guarantees quit() is always called — even on error.
    """
    smtp_server = None
    try:
        logging.info(
            f"Connecting to SMTP: host={SMTP_CONFIG['SMTP_HOSTNAME']} "
            f"port={SMTP_CONFIG['SMTP_PORT']} user={SMTP_CONFIG['SMTP_USER']}"
        )
        smtp_server = smtplib.SMTP(SMTP_CONFIG["SMTP_HOSTNAME"], SMTP_CONFIG["SMTP_PORT"])
        smtp_server.ehlo()
        smtp_server.starttls()
        smtp_server.ehlo()                         # re-identify after STARTTLS
        smtp_server.login(SMTP_CONFIG["SMTP_USER"], SMTP_CONFIG["SMTP_PASSWORD"])
        logging.info("SMTP connection established successfully.")
        yield smtp_server
    except smtplib.SMTPAuthenticationError as e:
        logging.error(f"SMTP authentication failed: {e}")
        raise
    except smtplib.SMTPConnectError as e:
        logging.error(f"SMTP connection error: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected SMTP error: {e}")
        raise
    finally:
        if smtp_server:
            try:
                smtp_server.quit()
                logging.info("SMTP connection closed.")
            except Exception:
                pass   # already disconnected — safe to ignore


# ---------------------------------------------------------------------------
# Build the MIME message
# ---------------------------------------------------------------------------
def build_message(
    to: list,
    subject: str,
    html_body: str,
    plain_body: str = None,
) -> MIMEMultipart:
    """
    Constructs a multipart/alternative MIME email.
    Email clients show the HTML version; plain text is the fallback.
    """
    message = MIMEMultipart("alternative")
    message["From"]    = SMTP_CONFIG["SENDER_EMAIL"]
    message["To"]      = ", ".join(to)
    message["Subject"] = subject

    # Plain-text part first (clients prefer the last matching part, so HTML goes last)
    fallback = plain_body or "This alert contains an HTML table. Please open it in an HTML-capable email client."
    message.attach(MIMEText(fallback,   "plain"))
    message.attach(MIMEText(html_body,  "html"))

    return message


# ---------------------------------------------------------------------------
# Send — single email
# ---------------------------------------------------------------------------
def send_email(
    to: list,
    subject: str,
    html_body: str,
    plain_body: str = None,
) -> bool:
    """
    Sends one HTML email (with plain-text fallback) via SMTP.

    Args:
        to:         List of recipient addresses.
        subject:    Email subject line.
        html_body:  HTML string for the email body.
        plain_body: Optional plain-text fallback (auto-generated if omitted).

    Returns:
        True on success, raises on failure.
    """
    if not to:
        raise ValueError("Recipient list 'to' cannot be empty.")
    if not html_body:
        raise ValueError("html_body cannot be empty.")

    message = build_message(to, subject, html_body, plain_body)

    with create_email_service() as smtp:
        rejected = smtp.sendmail(
            SMTP_CONFIG["SENDER_EMAIL"],
            to,
            message.as_string(),
        )
        print("Rejected:",rejected)
        if rejected:
            # sendmail() returns a dict of {addr: (code, msg)} for failed recipients
            logging.warning(f"Some recipients were rejected: {rejected}")
        else:
            logging.info(f"Email sent successfully to: {', '.join(to)}")

    return True

# COMMAND ----------

# Single alert email
if alert_df.count() > 0:
    send_email(
        to        = recipients,
        subject   = f"⚠️ Databricks Job Alert({env}) — Action Required",
        html_body = build_html_email(alert_df),   # the function from the previous cell
    )
else:
    print("No failing or stale jobs — no alert sent.")
    logging.info("No failing or stale jobs — no alert sent.")