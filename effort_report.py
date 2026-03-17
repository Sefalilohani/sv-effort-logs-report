import os
import time
import requests
from datetime import datetime, timezone, timedelta

# ── CONFIG ─────────────────────────────────────────────────────

_raw_token = os.environ["SLACK_BOT_TOKEN"]
SLACK_TOKEN = "xoxb" + _raw_token[4:31] + "bFqMGfkmHBzvLRtU1It2ptnt"

REDASH_API_KEY = "hRWTnaq2Qgk274jXYszPW3U9Cqzyjt20WbcARosl"
REDASH_BASE = "https://redash.springworks.in"

OPS_CHANNEL_ID = "C0AGRE19V6U"  # testing-sefali

REPORT_TYPE = os.environ.get("REPORT_TYPE", "9am")
THREAD_FILE = "effort_thread_ts.txt"

IST = timezone(timedelta(hours=5, minutes=30))

REDASH_URL = (
    "https://redash.springworks.in/queries/1493"
    "?p_Client_name=%5B%22SELECT%20id%20FROM%20company%20WHERE%20id%20not%20in%20%2828%2C%2089%2C%2074%29%20and%20deleted_at%20is%20NULL%22%5D"
    "&p_Custom_Check_Type=%5B%22adverse_media_check%22%2C%22corporate_affiliation_check%22%2C%22directorship_check%22%2C%22do_not_use_education_check%22%2C%22economic_default_check%22%2C%22employment_details%22%2C%22face_match%22%2C%22facis_level_3%22%2C%22form_16%22%2C%22form_26as%22%2C%22gap_review%22%2C%22general_service_administration_check%22%2C%22medical_test_electrocardiogram%22%2C%22medical_test_package_a%22%2C%22medical_test_package_e%22%2C%22medical_test_package_f%22%2C%22medical_test_pulmonary_function_test%22%2C%22medical_test_ultrasound_abdomen%22%2C%22office_of_foreign_assets_control_ofac_check%22%2C%22oig_exclusions%22%2C%22other%22%2C%22overlap_check%22%2C%22personal_reference_check%22%2C%22personal_reference_check_2%22%2C%22police_clearance_certificate%22%2C%22political_affiliation_check%22%2C%22resume_review%22%2C%22right_to_work_india%22%2C%22social_media_check%22%2C%22social_media_lite%22%2C%22universal_account_number_check%22%5D"
    "&p_Min_Days_Since_Effort=5"
    "&p_Net_TAT=%5B%22%278-10%28Yellow%29%27%22%2C%22%2711-14%28Red%29%27%22%2C%22%2714%2B%28Black%29%27%22%5D"
    "&p_Quick_Check=%5B%22all%22%5D"
    "&p_result_limit=4000"
    "#2553"
)

# ── HELPERS ────────────────────────────────────────────────────

def ordinal(n):
    if 11 <= n <= 13:
        return f"{n}th"
    return f"{n}{['th','st','nd','rd','th'][min(n % 10, 4)]}"

def fmt_date(dt):
    return f"{ordinal(dt.day)} {dt.strftime('%B %Y')}"

# ── FETCH REDASH DATA ─────────────────────────────────────────

def _delete_temp_query(query_id, headers):
    try:
        requests.delete(
            f"{REDASH_BASE}/api/queries/{query_id}",
            headers=headers,
            timeout=10
        )
        print(f"Temporary query {query_id} deleted")
    except Exception as e:
        print(f"Warning: Could not delete temp query {query_id}: {e}")


def fetch_redash(sql):
    headers = {
        "Authorization": f"Key {REDASH_API_KEY}",
        "Content-Type": "application/json"
    }

    # Step 1: Create temporary query
    create_url = f"{REDASH_BASE}/api/queries"
    create_payload = {
        "name": "temp_effort_report",
        "query": sql,
        "data_source_id": 5,
        "options": {"apply_auto_limit": False}
    }
    print("Creating temporary Redash query...")
    r = requests.post(create_url, headers=headers, json=create_payload, timeout=30)
    print(f"Create query status: {r.status_code}")
    r.raise_for_status()
    query_id = r.json()["id"]
    print(f"Temporary query created: id={query_id}")

    # Step 2: Execute the query
    exec_url = f"{REDASH_BASE}/api/queries/{query_id}/results"
    exec_payload = {"max_age": 0}

    print("Executing query...")
    r2 = requests.post(exec_url, headers=headers, json=exec_payload, timeout=30)
    print(f"POST status: {r2.status_code}")
    if r2.status_code not in (200, 201):
        print(f"Response: {r2.text[:500]}")
    r2.raise_for_status()
    resp = r2.json()

    # Immediate result
    if "query_result" in resp:
        rows = resp["query_result"]["data"]["rows"]
        print(f"Got immediate result: {len(rows)} rows")
        _delete_temp_query(query_id, headers)
        return rows

    # Job queued — poll
    job_id = resp.get("job", {}).get("id", "unknown")
    print(f"Query job queued (id={job_id}), polling for result...")
    poll_payload = {"max_age": 60}

    for attempt in range(20):
        time.sleep(3)
        print(f"  Poll attempt {attempt + 1}/20...")
        r3 = requests.post(exec_url, headers=headers, json=poll_payload, timeout=30)
        if r3.status_code not in (200, 201):
            print(f"  Poll status {r3.status_code}: {r3.text[:200]}")
            continue
        resp3 = r3.json()
        if "query_result" in resp3:
            rows = resp3["query_result"]["data"]["rows"]
            print(f"  Got result: {len(rows)} rows")
            _delete_temp_query(query_id, headers)
            return rows
        new_job = resp3.get("job", {})
        print(f"  Still running, job status={new_job.get('status')}")

    _delete_temp_query(query_id, headers)
    raise Exception("Timed out waiting for Redash query result after 60 seconds")


# ── SQL QUERIES ───────────────────────────────────────────────

PIVOT_SQL = (
    "SELECT "
    "  comp.company_type AS 'Client Priority', "
    "  COALESCE(cct.display_name, cn.name) AS 'Check Type', "
    "  COUNT(DISTINCT cd.check_id) AS 'Count', "
    "  MIN(el.last_effort_date) AS 'From Date' "
    "FROM company_candidate_mapping ccm "
    "INNER JOIN candidates c ON c.id = ccm.candidate_id AND c.deleted_at IS NULL "
    "INNER JOIN company comp ON comp.id = ccm.company_id AND comp.deleted_at IS NULL "
    "INNER JOIN (SELECT DISTINCT company_id_fk FROM payments_company_packages WHERE deleted_at IS NULL) pcp ON pcp.company_id_fk = comp.id "
    "INNER JOIN ( "
    "  SELECT candidate_id_fk AS candidate_id, id AS check_id, (SELECT id FROM check_names WHERE name='identity') AS check_name_id, NULL AS custom_check_type_fk FROM candidates_ids WHERE deleted_at IS NULL AND status IN (0,4,9) "
    "  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='address'), NULL FROM candidates_address WHERE deleted_at IS NULL AND status IN (0,4,9) "
    "  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='court'), NULL FROM candidate_history WHERE deleted_at IS NULL AND status IN (0,4,9) "
    "  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='education'), NULL FROM candidates_education WHERE deleted_at IS NULL AND status IN (0,4,9) "
    "  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='employment'), NULL FROM candidates_employment WHERE deleted_at IS NULL AND status IN (0,4,9) "
    "  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='reference'), NULL FROM candidates_refcheck WHERE deleted_at IS NULL AND status IN (0,4,9) "
    "  UNION ALL SELECT cc.candidate_id_fk, cc.id, (SELECT id FROM check_names WHERE name='custom'), cc.custom_check_type_fk FROM custom_check cc WHERE cc.deleted_at IS NULL AND cc.status IN (0,4,9) "
    "  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='credit'), NULL FROM candidates_creditcheck WHERE deleted_at IS NULL AND status IN (0,4,9) "
    "  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='drug'), NULL FROM candidate_drugs WHERE deleted_at IS NULL AND status IN (0,4,9) "
    "  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='world'), NULL FROM candidates_worldcheck WHERE deleted_at IS NULL AND status IN (0,4,9) "
    ") cd ON cd.candidate_id = ccm.candidate_id "
    "INNER JOIN check_names cn ON cn.id = cd.check_name_id "
    "LEFT JOIN custom_check_types cct ON cct.id = cd.custom_check_type_fk "
    "LEFT JOIN candidate_checks_tat_v2 tat ON tat.candidate_id = ccm.candidate_id AND tat.check_id = cd.check_id "
    "LEFT JOIN ( "
    "  SELECT cecl.candidate_id_fk, cecl.check_id_fk, cecl.check_name_id_fk, "
    "    MAX(cecl.created_at) AS last_effort_date, "
    "    DATEDIFF(NOW(), MAX(cecl.created_at)) AS days_since_last_effort, "
    "    SUBSTRING_INDEX(GROUP_CONCAT(ccem.mode ORDER BY cecl.created_at DESC),',',1) AS last_effort_mode "
    "  FROM candidates_check_effort_logs cecl "
    "  LEFT JOIN candidates_check_effort_modes ccem ON ccem.id = cecl.effort_mode_id_fk "
    "  WHERE cecl.deleted_at IS NULL "
    "  GROUP BY cecl.candidate_id_fk, cecl.check_id_fk, cecl.check_name_id_fk "
    ") el ON el.candidate_id_fk = ccm.candidate_id AND el.check_id_fk = cd.check_id AND el.check_name_id_fk = cd.check_name_id "
    "LEFT JOIN payments_company_insuff_funds pcif ON pcif.candidate_id_fk = ccm.candidate_id AND pcif.check_id_fk = cd.check_id AND pcif.status = 'OPEN' AND pcif.deleted_at IS NULL "
    "WHERE ccm.deleted_at IS NULL "
    "  AND ccm.company_id NOT IN (28, 878) "
    "  AND ccm.status NOT IN (9,10,11,12) "
    "  AND pcif.id IS NULL "
    "  AND (el.last_effort_mode IS NULL OR el.last_effort_mode != 'Insuff_Raised') "
    "  AND (el.days_since_last_effort IS NULL OR el.days_since_last_effort >= 5) "
    "  AND CASE "
    "    WHEN FLOOR(tat.actual_net_tat_net) BETWEEN 8 AND 10 THEN '8-10(Yellow)' "
    "    WHEN FLOOR(tat.actual_net_tat_net) BETWEEN 11 AND 14 THEN '11-14(Red)' "
    "    WHEN FLOOR(tat.actual_net_tat_net) > 14 THEN '14+(Black)' "
    "    ELSE NULL "
    "  END IS NOT NULL "
    "GROUP BY comp.company_type, COALESCE(cct.display_name, cn.name) "
    "ORDER BY comp.company_type, COALESCE(cct.display_name, cn.name)"
)

FROM_DATE_SQL = (
    "SELECT MIN(el.last_effort_date) AS 'From Date' "
    "FROM company_candidate_mapping ccm "
    "INNER JOIN candidates c ON c.id = ccm.candidate_id AND c.deleted_at IS NULL "
    "INNER JOIN company comp ON comp.id = ccm.company_id AND comp.deleted_at IS NULL "
    "INNER JOIN (SELECT DISTINCT company_id_fk FROM payments_company_packages WHERE deleted_at IS NULL) pcp ON pcp.company_id_fk = comp.id "
    "INNER JOIN ( "
    "  SELECT candidate_id_fk AS candidate_id, id AS check_id, (SELECT id FROM check_names WHERE name='identity') AS check_name_id FROM candidates_ids WHERE deleted_at IS NULL AND status IN (0,4,9) "
    "  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='address') FROM candidates_address WHERE deleted_at IS NULL AND status IN (0,4,9) "
    "  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='court') FROM candidate_history WHERE deleted_at IS NULL AND status IN (0,4,9) "
    "  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='education') FROM candidates_education WHERE deleted_at IS NULL AND status IN (0,4,9) "
    "  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='employment') FROM candidates_employment WHERE deleted_at IS NULL AND status IN (0,4,9) "
    "  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='reference') FROM candidates_refcheck WHERE deleted_at IS NULL AND status IN (0,4,9) "
    "  UNION ALL SELECT cc.candidate_id_fk, cc.id, (SELECT id FROM check_names WHERE name='custom') FROM custom_check cc WHERE cc.deleted_at IS NULL AND cc.status IN (0,4,9) "
    "  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='credit') FROM candidates_creditcheck WHERE deleted_at IS NULL AND status IN (0,4,9) "
    "  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='drug') FROM candidate_drugs WHERE deleted_at IS NULL AND status IN (0,4,9) "
    "  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='world') FROM candidates_worldcheck WHERE deleted_at IS NULL AND status IN (0,4,9) "
    ") cd ON cd.candidate_id = ccm.candidate_id "
    "LEFT JOIN candidate_checks_tat_v2 tat ON tat.candidate_id = ccm.candidate_id AND tat.check_id = cd.check_id "
    "LEFT JOIN ( "
    "  SELECT cecl.candidate_id_fk, cecl.check_id_fk, cecl.check_name_id_fk, "
    "    MAX(cecl.created_at) AS last_effort_date, "
    "    DATEDIFF(NOW(), MAX(cecl.created_at)) AS days_since_last_effort, "
    "    SUBSTRING_INDEX(GROUP_CONCAT(ccem.mode ORDER BY cecl.created_at DESC),',',1) AS last_effort_mode "
    "  FROM candidates_check_effort_logs cecl "
    "  LEFT JOIN candidates_check_effort_modes ccem ON ccem.id = cecl.effort_mode_id_fk "
    "  WHERE cecl.deleted_at IS NULL "
    "  GROUP BY cecl.candidate_id_fk, cecl.check_id_fk, cecl.check_name_id_fk "
    ") el ON el.candidate_id_fk = ccm.candidate_id AND el.check_id_fk = cd.check_id AND el.check_name_id_fk = cd.check_name_id "
    "LEFT JOIN payments_company_insuff_funds pcif ON pcif.candidate_id_fk = ccm.candidate_id AND pcif.check_id_fk = cd.check_id AND pcif.status = 'OPEN' AND pcif.deleted_at IS NULL "
    "WHERE ccm.deleted_at IS NULL "
    "  AND ccm.company_id NOT IN (28, 878) "
    "  AND ccm.status NOT IN (9,10,11,12) "
    "  AND pcif.id IS NULL "
    "  AND (el.last_effort_mode IS NULL OR el.last_effort_mode != 'Insuff_Raised') "
    "  AND (el.days_since_last_effort IS NULL OR el.days_since_last_effort >= 5) "
    "  AND CASE "
    "    WHEN FLOOR(tat.actual_net_tat_net) BETWEEN 8 AND 10 THEN '8-10(Yellow)' "
    "    WHEN FLOOR(tat.actual_net_tat_net) BETWEEN 11 AND 14 THEN '11-14(Red)' "
    "    WHEN FLOOR(tat.actual_net_tat_net) > 14 THEN '14+(Black)' "
    "    ELSE NULL "
    "  END IS NOT NULL"
)

# ── POST TO SLACK ──────────────────────────────────────────────

def post_slack(text, thread_ts=None):
    payload = {"channel": OPS_CHANNEL_ID, "text": text}
    if thread_ts:
        payload["thread_ts"] = thread_ts
    r = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {SLACK_TOKEN}", "Content-Type": "application/json"},
        json=payload
    )
    r.raise_for_status()
    resp = r.json()
    if not resp.get("ok"):
        raise Exception(f"Slack API error: {resp.get('error')}")
    return resp["ts"]

# ── FIND 9:30AM THREAD ─────────────────────────────────────────

def find_9am_thread_ts():
    if os.path.exists(THREAD_FILE):
        with open(THREAD_FILE) as f:
            ts = f.read().strip()
            if ts:
                print(f"Found thread ts from file: {ts}")
                return ts

    now = datetime.now(IST)
    today_start = datetime(now.year, now.month, now.day, 0, 0, 0, tzinfo=IST).timestamp()

    r = requests.get(
        "https://slack.com/api/conversations.history",
        headers={"Authorization": f"Bearer {SLACK_TOKEN}"},
        params={"channel": OPS_CHANNEL_ID, "oldest": str(today_start), "limit": 50}
    )
    data = r.json()
    if not data.get("ok"):
        raise Exception(f"Slack history error: {data.get('error')}")

    for msg in data.get("messages", []):
        if "Effort Log Report" in msg.get("text", ""):
            ts = msg["ts"]
            print(f"Found today's effort report thread: {ts}")
            with open(THREAD_FILE, "w") as f:
                f.write(ts)
            return ts

    raise Exception("Could not find today's 9:30 AM effort report thread.")

# ── BUILD PIVOT TABLE ──────────────────────────────────────────

def build_pivot_table(rows):
    check_types = sorted(set(r["Check Type"] for r in rows))
    priorities = sorted(set(r["Client Priority"] for r in rows))

    data = {}
    for row in rows:
        data[(row["Client Priority"], row["Check Type"])] = row["Count"]

    priority_totals = {p: sum(data.get((p, ct), 0) for ct in check_types) for p in priorities}
    col_totals = {ct: sum(data.get((p, ct), 0) for p in priorities) for ct in check_types}
    grand_total = sum(col_totals.values())

    p_width = 16
    ct_width = max(9, max(len(ct) for ct in check_types) + 2)

    header = f"{'Client Priority':<{p_width}}" + "".join(f"{ct:>{ct_width}}" for ct in check_types) + f"{'Total':>8}"
    separator = "-" * len(header)

    lines = ["```", header, separator]
    for p in priorities:
        line = f"{p:<{p_width}}"
        for ct in check_types:
            val = data.get((p, ct), 0)
            line += f"{val if val else '-':>{ct_width}}"
        line += f"{priority_totals[p]:>8}"
        lines.append(line)

    lines.append(separator)
    totals_line = f"{'Total':<{p_width}}" + "".join(f"{col_totals[ct]:>{ct_width}}" for ct in check_types) + f"{grand_total:>8}"
    lines.append(totals_line)
    lines.append("```")

    return "\n".join(lines), grand_total

# ── BUILD REPORT ───────────────────────────────────────────────

def build_report(pivot_rows, from_date_rows, report_type):
    now = datetime.now(IST)
    today_str = fmt_date(now)

    from_date_raw = from_date_rows[0].get("From Date") if from_date_rows else None
    if from_date_raw:
        from_dt = datetime.strptime(str(from_date_raw)[:19], "%Y-%m-%dT%H:%M:%S")
        from_date_str = fmt_date(from_dt)
    else:
        from_date_str = "N/A"

    table_text, grand_total = build_pivot_table(pivot_rows)

    if report_type == "9am":
        heading = f"📋 *Effort Log Report — Missing or Outdated as of {today_str}*"
    else:
        heading = f"📋 *Updated Effort Log Report — Missing or Outdated as of {today_str}*"

    text = (
        f"{heading}\n"
        f"*Min. 5 Days Since Last Effort | NET TAT — 7+ days*\n"
        f"*From Date: {from_date_str}*\n\n"
        f"{table_text}\n\n"
        f"*Total Checks: {grand_total}*\n"
        f"📊 <{REDASH_URL}|View Full Report on Redash>\n\n"
        f"<!subteam^S04K9859L64> Please review and update effort logs for all pending checks at the earliest."
    )
    return text

# ── MAIN ───────────────────────────────────────────────────────

def run_report():
    print(f"Report type: {REPORT_TYPE}")

    print("Fetching pivot data...")
    pivot_rows = fetch_redash(PIVOT_SQL)

    print("Fetching from date...")
    from_date_rows = fetch_redash(FROM_DATE_SQL)

    message = build_report(pivot_rows, from_date_rows, REPORT_TYPE)

    if REPORT_TYPE == "9am":
        print("Posting new Slack message (9:30 AM effort report)")
        ts = post_slack(message)
        with open(THREAD_FILE, "w") as f:
            f.write(ts)
        print(f"Posted. Thread ts: {ts}")
    else:
        print(f"Replying in thread (4 PM effort report)")
        try:
            ts = find_9am_thread_ts()
            post_slack(message, ts)
            print(f"Replied in thread: {ts}")
        except Exception as e:
            print(f"Warning: Could not find 9:30 AM thread ({e}). Posting as new message.")
            ts = post_slack(message)
            print(f"Posted as new message. ts: {ts}")


if __name__ == "__main__":
    run_report()
