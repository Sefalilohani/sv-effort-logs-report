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

    # Immediate cached result
    if "query_result" in resp:
        rows = resp["query_result"]["data"]["rows"]
        print(f"Got immediate result: {len(rows)} rows")
        _delete_temp_query(query_id, headers)
        return rows

    # Job queued — poll by re-POSTing with max_age=60
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

PIVOT_SQL = """
SELECT 
  comp.company_type AS `Client Priority`,
  COALESCE(cct.display_name, cn.name) AS `Check Type`,
  COUNT(DISTINCT cd.check_id) AS `Count`,
  MIN(el.last_effort_date) AS `From Date`
FROM company_candidate_mapping ccm
INNER JOIN candidates c ON c.id = ccm.candidate_id AND c.deleted_at IS NULL
INNER JOIN company comp ON comp.id = ccm.company_id AND comp.deleted_at IS NULL
INNER JOIN (SELECT DISTINCT company_id_fk FROM payments_company_packages WHERE deleted_at IS NULL) pcp ON pcp.company_id_fk = comp.id
INNER JOIN (
  SELECT candidate_id_fk AS candidate_id, id AS check_id, (SELECT id FROM check_names WHERE name='identity') AS check_name_id, NULL AS custom_check_type_fk FROM candidates_ids WHERE deleted_at IS NULL AND status IN (0,4,9)
  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='address'), NULL FROM candidates_address WHERE deleted_at IS NULL AND status IN (0,4,9)
  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='court'), NULL FROM candidate_history WHERE deleted_at IS NULL AND status IN (0,4,9)
  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='education'), NULL FROM candidates_education WHERE deleted_at IS NULL AND status IN (0,4,9)
  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='employment'), NULL FROM candidates_employment WHERE deleted_at IS NULL AND status IN (0,4,9)
  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='reference'), NULL FROM candidates_refcheck WHERE deleted_at IS NULL AND status IN (0,4,9)
  UNION ALL SELECT cc.candidate_id_fk, cc.id, (SELECT id FROM check_names WHERE name='custom'), cc.custom_check_type_fk FROM custom_check cc WHERE cc.deleted_at IS NULL AND cc.status IN (0,4,9)
  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='credit'), NULL FROM candidates_creditcheck WHERE deleted_at IS NULL AND status IN (0,4,9)
  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='drug'), NULL FROM candidate_drugs WHERE deleted_at IS NULL AND status IN (0,4,9)
  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='world'), NULL FROM candidates_worldcheck WHERE deleted_at IS NULL AND status IN (0,4,9)
) cd ON cd.candidate_id = ccm.candidate_id
INNER JOIN check_names cn ON cn.id = cd.check_name_id
LEFT JOIN custom_check_types cct ON cct.id = cd.custom_check_type_fk
LEFT JOIN candidate_checks_tat_v2 tat ON tat.candidate_id = ccm.candidate_id AND tat.check_id = cd.check_id
LEFT JOIN (
  SELECT cecl.candidate_id_fk, cecl.check_id_fk, cecl.check_name_id_fk,
    MAX(cecl.created_at) AS last_effort_date,
    DATEDIFF(NOW(), MAX(cecl.created_at)) AS days_since_last_effort,
    SUBSTRING_INDEX(GROUP_CONCAT(ccem.mode ORDER BY cecl.created_at DESC),',',1) AS last_effort_mode
  FROM candidates_check_effort_logs cecl
  LEFT JOIN candidates_check_effort_modes ccem ON ccem.id = cecl.effort_mode_id_fk
  WHERE cecl.deleted_at IS NULL
  GROUP BY cecl.candidate_id_fk, cecl.check_id_fk, cecl.check_name_id_fk
) el ON el.candidate_id_fk = ccm.candidate_id AND el.check_id_fk = cd.check_id AND el.check_name_id_fk = cd.check_name_id
LEFT JOIN payments_company_insuff_funds pcif ON pcif.candidate_id_fk = ccm.candidate_id AND pcif.check_id_fk = cd.check_id AND pcif.status = 'OPEN' AND pcif.deleted_at IS NULL
WHERE ccm.deleted_at IS NULL
  AND ccm.company_id NOT IN (28, 878)
  AND ccm.status NOT IN (9,10,11,12)
  AND pcif.id IS NULL
  AND (el.last_effort_mode IS NULL OR el.last_effort_mode != 'Insuff_Raised')
  AND (el.days_since_last_effort IS NULL OR el.days_since_last_effort >= 5)
  AND CASE
    WHEN FLOOR(tat.actual_net_tat_net) BETWEEN 8 AND 10 THEN '8-10(Yellow)'
    WHEN FLOOR(tat.actual_net_tat_net) BETWEEN 11 AND 14 THEN '11-14(Red)'
    WHEN FLOOR(tat.actual_net_tat_net) > 14 THEN '14+(Black)'
    ELSE NULL
  END IS NOT NULL
GROUP BY comp.company_type, COALESCE(cct.display_name, cn.name)
ORDER BY comp.company_type, COALESCE(cct.display_name, cn.name)
"""

FROM_DATE_SQL = """
SELECT MIN(el.last_effort_date) AS `From Date`
FROM company_candidate_mapping ccm
INNER JOIN candidates c ON c.id = ccm.candidate_id AND c.deleted_at IS NULL
INNER JOIN company comp ON comp.id = ccm.company_id AND comp.deleted_at IS NULL
INNER JOIN (SELECT DISTINCT company_id_fk FROM payments_company_packages WHERE deleted_at IS NULL) pcp ON pcp.company_id_fk = comp.id
INNER JOIN (
  SELECT candidate_id_fk AS candidate_id, id AS check_id, (SELECT id FROM check_names WHERE name='identity') AS check_name_id FROM candidates_ids WHERE deleted_at IS NULL AND status IN (0,4,9)
  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='address') FROM candidates_address WHERE deleted_at IS NULL AND status IN (0,4,9)
  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='court') FROM candidate_history WHERE deleted_at IS NULL AND status IN (0,4,9)
  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='education') FROM candidates_education WHERE deleted_at IS NULL AND status IN (0,4,9)
  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='employment') FROM candidates_employment WHERE deleted_at IS NULL AND status IN (0,4,9)
  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='reference') FROM candidates_refcheck WHERE deleted_at IS NULL AND status IN (0,4,9)
  UNION ALL SELECT cc.candidate_id_fk, cc.id, (SELECT id FROM check_names WHERE name='custom') FROM custom_check cc WHERE cc.deleted_at IS NULL AND cc.status IN (0,4,9)
  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='credit') FROM candidates_creditcheck WHERE deleted_at IS NULL AND status IN (0,4,9)
  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='drug') FROM candidate_drugs WHERE deleted_at IS NULL AND status IN (0,4,9)
  UNION ALL SELECT candidate_id_fk, id, (SELECT id FROM check_names WHERE name='world') FROM candidates_worldcheck WHERE deleted_at IS NULL AND status IN (0,4,9)
) cd ON cd.candidate_id = ccm.candidate_id
LEFT JOIN candidate_checks_tat_v2 tat ON tat.candidate_id = ccm.candidate_id AND tat.check_id = cd.check_id
LEFT JOIN (
  SELECT cecl.candidate_id_fk, cecl.check_id_fk, cecl.check_name_id_fk,
    MAX(cecl.created_at) AS last_effort_date,
    DATEDIFF(NOW(), MAX(cecl.created_at)) AS days_since_last_effort,
    SUBSTRING_INDEX(GROUP_CONCAT(ccem.mode ORDER BY cecl.created_at DESC),',',1) AS last_effort_mode
  FROM candidates_check_effort_logs cecl
  LEFT JOIN candidates_check_effort_modes ccem ON ccem.id = cecl.effort_mode_id_fk
  WHERE cecl.deleted_at IS NULL
  GROUP BY cecl.candidate_id_fk, cecl.check_id_fk, cecl.check_name_id_fk
) el ON el.candidate_id_fk = ccm.candidate_id AND el.check_id_fk = cd.check_id AND el.check_name_id_fk = cd.check_name_id
LEFT JOIN payments_company_insuff_funds pcif ON pcif.candidate_id_fk = ccm.candidate_id AND pcif.check_id_fk = cd.check_id AND pcif.status = 'OPEN' AND pcif.deleted_at IS NULL
WHERE ccm.deleted_at IS NULL
  AND ccm.company_id NOT IN (28, 878)
  AND ccm.status NOT IN (9,10,11,12)
  AND pcif.id IS NULL
  AND (el.last_effort_mode IS NULL OR el.last_effort_mode != 'Insuff_Raised')
  AND (el.days_since_last_effort IS NULL OR el.days_since_last_effort >= 5)
  AND CASE
    WHEN FLOOR(tat.actual_net_tat_net) BETWEEN 8 AND 10 THEN '8-10(Yellow)'
    WHEN FLOOR(tat.actual_net_tat_net)
