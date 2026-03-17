import os
import time
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

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

def fetch_redash():
    headers = {
        "Authorization": f"Key {REDASH_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "parameters": {
            "Quick_Check": ["all"],
            "Client_name": ["SELECT id FROM company WHERE id not in (28, 89, 74) and deleted_at is NULL"],
            "Custom_Check_Type": [
                "adverse_media_check", "corporate_affiliation_check", "directorship_check",
                "do_not_use_education_check", "economic_default_check", "employment_details",
                "face_match", "facis_level_3", "form_16", "form_26as", "gap_review",
                "general_service_administration_check", "medical_test_electrocardiogram",
                "medical_test_package_a", "medical_test_package_e", "medical_test_package_f",
                "medical_test_pulmonary_function_test", "medical_test_ultrasound_abdomen",
                "office_of_foreign_assets_control_ofac_check", "oig_exclusions", "other",
                "overlap_check", "personal_reference_check", "personal_reference_check_2",
                "police_clearance_certificate", "political_affiliation_check", "resume_review",
                "right_to_work_india", "social_media_check", "social_media_lite",
                "universal_account_number_check"
            ],
            "Min_Days_Since_Effort": 5,
            "Net_TAT": ["'8-10(Yellow)'", "'11-14(Red)'", "'14+(Black)'"],
            "result_limit": 4000
        },
        "max_age": 0
    }

    url = f"{REDASH_BASE}/api/queries/1493/results"

    print("Triggering Redash query 1493...")
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    print(f"POST status: {r.status_code}")
    if r.status_code not in (200, 201):
        print(f"Response: {r.text[:500]}")
    r.raise_for_status()
    resp = r.json()

    if "query_result" in resp:
        rows = resp["query_result"]["data"]["rows"]
        print(f"Got immediate result: {len(rows)} rows")
        return rows

    job_id = resp.get("job", {}).get("id", "unknown")
    print(f"Query job queued (id={job_id}), polling for result...")
    poll_payload = {**payload, "max_age": 60}

    for attempt in range(20):
        time.sleep(3)
        print(f"  Poll attempt {attempt + 1}/20...")
        r2 = requests.post(url, headers=headers, json=poll_payload, timeout=30)
        if r2.status_code not in (200, 201):
            print(f"  Poll status {r2.status_code}: {r2.text[:200]}")
            continue
        resp2 = r2.json()
        if "query_result" in resp2:
            rows = resp2["query_result"]["data"]["rows"]
            print(f"  Got result: {len(rows)} rows")
            return rows
        new_job = resp2.get("job", {})
        print(f"  Still running, job status={new_job.get('status')}")

    raise Exception("Timed out waiting for Redash query result after 60 seconds")

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
    # Aggregate raw rows into pivot: Client Priority x Check Type
    pivot = defaultdict(lambda: defaultdict(int))
    from_dates = []

    for row in rows:
        priority = row.get("Client Priority") or "Unknown"
        check_type = row.get("Check Type") or "Unknown"
        pivot[priority][check_type] += 1

        raw_date = row.get("Last Effort Log Date")
        if raw_date:
            try:
                dt = datetime.strptime(str(raw_date)[:19], "%Y-%m-%dT%H:%M:%S")
                from_dates.append(dt)
            except ValueError:
                pass

    from_date = min(from_dates) if from_dates else None

    priorities = sorted(pivot.keys())
    check_types = sorted(set(ct for p in pivot.values() for ct in p.keys()))

    priority_totals = {p: sum(pivot[p].values()) for p in priorities}
    col_totals = {ct: sum(pivot[p].get(ct, 0) for p in priorities) for ct in check_types}
    grand_total = sum(priority_totals.values())

    p_width = 16
    ct_width = max(9, max(len(ct) for ct in check_types) + 2)

    header = f"{'Client Priority':<{p_width}}" + "".join(f"{ct:>{ct_width}}" for ct in check_types) + f"{'Total':>8}"
    separator = "-" * len(header)

    lines = ["```", header, separator]
    for p in priorities:
        line = f"{p:<{p_width}}"
        for ct in check_types:
            val = pivot[p].get(ct, 0)
            line += f"{val if val else '-':>{ct_width}}"
        line += f"{priority_totals[p]:>8}"
        lines.append(line)

    lines.append(separator)
    totals_line = f"{'Total':<{p_width}}" + "".join(f"{col_totals[ct]:>{ct_width}}" for ct in check_types) + f"{grand_total:>8}"
    lines.append(totals_line)
    lines.append("```")

    return "\n".join(lines), grand_total, from_date

# ── BUILD REPORT ───────────────────────────────────────────────

def build_report(rows, report_type):
    now = datetime.now(IST)
    today_str = fmt_date(now)

    table_text, grand_total, from_date = build_pivot_table(rows)

    from_date_str = fmt_date(from_date) if from_date else "N/A"

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

    print("Fetching Redash data...")
    rows = fetch_redash()
    print(f"Got {len(rows)} rows")

    message = build_report(rows, REPORT_TYPE)

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
