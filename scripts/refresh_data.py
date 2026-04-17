#!/usr/bin/env python3
"""Refresh Helio Install pipeline data from Zoho CRM.

Runs via GitHub Actions on a cron schedule. Exchanges the stored refresh
token for an access token, pulls the Installs module + user map from Zoho
CRM, and writes the result to ../data.json at the repo root. The dashboard
(index.html) fetches data.json on load and renders the table, chart, and
cards client-side.

Required env vars (GitHub Secrets):
  ZOHO_CLIENT_ID
  ZOHO_CLIENT_SECRET
  ZOHO_REFRESH_TOKEN
  ZOHO_DC (optional, default 'com'; also 'eu', 'in', 'com.au', 'jp')

No external dependencies — uses only stdlib, so the workflow doesn't need
a pip install step.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

DC = os.environ.get("ZOHO_DC", "com").strip()
ACCOUNTS_HOST = f"accounts.zoho.{DC}"
API_HOST = f"www.zohoapis.{DC}"

# Stages shown on the dashboard. Any other stage (e.g. Closed/Cancelled)
# is filtered out when building data.json.
ACTIVE_STAGES = [
    "Sales Ops Review",
    "Project Intake",
    "Site Survey",
    "Engineering",
    "Plan Review",
    "Interconnection",
    "Permitting",
    "Procurement & Scheduling",
    "Active Installation",
    "Inspection",
    "Witness Test / PTO",
    "Energized",
    "On Hold",
]
ACTIVE_STAGES_SET = set(ACTIVE_STAGES)

FIELDS = (
    "Project_ID,Name,Project_Stage,Sales_Representative,"
    "Project_Owner,Date_of_Stage_Change"
)

# Stage-change tracking was enabled in Zoho on 2026-04-16. Records that
# haven't moved stages since then have Date_of_Stage_Change = null. We
# treat those as "last changed on the launch date" so they show a
# consistent, daily-incrementing floor until they actually move stages
# and pick up a real timestamp. Midnight ET on the launch date.
STAGE_TRACKING_LAUNCH_TS = "2026-04-16T00:00:00-04:00"

CANVAS_ID = "5264387000040853100"  # layout ID used for the Zoho "open" link


def _http_json(req: urllib.request.Request) -> dict:
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            if resp.status == 204:
                return {}
            raw = resp.read()
            if not raw:
                return {}
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"HTTP {e.code} {e.reason} for {req.full_url}", file=sys.stderr)
        print(f"Response body: {body}", file=sys.stderr)
        raise


def get_access_token() -> str:
    required = ("ZOHO_CLIENT_ID", "ZOHO_CLIENT_SECRET", "ZOHO_REFRESH_TOKEN")
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise SystemExit(f"Missing env vars: {', '.join(missing)}")

    body = urllib.parse.urlencode({
        "refresh_token": os.environ["ZOHO_REFRESH_TOKEN"],
        "client_id": os.environ["ZOHO_CLIENT_ID"],
        "client_secret": os.environ["ZOHO_CLIENT_SECRET"],
        "grant_type": "refresh_token",
    }).encode()

    req = urllib.request.Request(
        f"https://{ACCOUNTS_HOST}/oauth/v2/token",
        data=body,
        method="POST",
    )
    payload = _http_json(req)
    token = payload.get("access_token")
    if not token:
        raise SystemExit(f"Zoho OAuth failed: {payload}")
    return token


def zoho_get(path: str, token: str, params: dict | None = None) -> dict:
    url = f"https://{API_HOST}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Zoho-oauthtoken {token}"},
    )
    return _http_json(req)


def fetch_installs(token: str) -> list[dict]:
    rows: list[dict] = []
    page = 1
    while True:
        resp = zoho_get("/crm/v7/Installs", token, {
            "fields": FIELDS,
            "page": page,
            "per_page": 200,
        })
        batch = resp.get("data") or []
        rows.extend(batch)
        info = resp.get("info") or {}
        if not info.get("more_records"):
            break
        page += 1
        if page > 50:  # hard safety cap
            break
    return rows


def _days_since(iso_str: str | None) -> int:
    if not iso_str:
        return 0
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    except ValueError:
        return 0
    return max(0, (datetime.now(timezone.utc) - dt).days)


def _format_label(dt: datetime) -> str:
    try:
        from zoneinfo import ZoneInfo
        local = dt.astimezone(ZoneInfo("America/New_York"))
        return local.strftime("%a %b %-d, %Y · %-I:%M %p ET")
    except Exception:
        return dt.strftime("%a %b %d, %Y · %H:%M UTC")


def _proj_id_sort_key(p: dict) -> int:
    try:
        return int(p["project_id"].split("-")[-1])
    except Exception:
        return 0


def build_projects(rows: list[dict]) -> list[dict]:
    out: list[dict] = []
    for r in rows:
        stage = r.get("Project_Stage") or ""
        if stage not in ACTIVE_STAGES_SET:
            continue

        # Resolve owner: prefer Zoho system Owner (lookup), fall back to the
        # Project_Owner text field if the system owner is empty.
        owner_name = ""
        owner_obj = r.get("Owner")
        if isinstance(owner_obj, dict):
            owner_name = (owner_obj.get("name") or "").strip()
        if not owner_name:
            owner_name = (r.get("Project_Owner") or "").strip()

        # Pick the best "entered current stage" timestamp:
        #   1. Real Date_of_Stage_Change (populated once a stage actually changes)
        #   2. If stage is "Sales Ops Review" (pipeline entry) and no stage
        #      change has been recorded, the project has never moved — use
        #      Created_Time, which is effectively when it entered this stage.
        #   3. Otherwise the project has been in its current stage since
        #      before stage tracking was enabled — use the launch date floor.
        stage_change_ts = r.get("Date_of_Stage_Change")
        if not stage_change_ts:
            if stage == "Sales Ops Review":
                stage_change_ts = r.get("Created_Time") or STAGE_TRACKING_LAUNCH_TS
            else:
                stage_change_ts = STAGE_TRACKING_LAUNCH_TS

        out.append({
            "project_id": (r.get("Project_ID") or "").strip(),
            "customer": (r.get("Name") or "").strip(),
            "stage": stage,
            "days_in_stage": _days_since(stage_change_ts),
            "rep": (r.get("Sales_Representative") or "").strip(),
            "owner": owner_name,
            "zoho_record_id": r.get("id") or "",
        })
    out.sort(key=_proj_id_sort_key, reverse=True)
    return out


def _apply_filter(projects: list[dict], flt: dict) -> list[dict]:
    """Apply a view filter to the project list. Within a field values are
    OR-ed (owner in ['A','B']); between fields they are AND-ed. An empty
    or missing filter field is treated as 'no restriction on this field'."""
    owners = set(flt.get("owners") or [])
    reps = set(flt.get("reps") or [])
    out = []
    for p in projects:
        if owners and p.get("owner") not in owners:
            continue
        if reps and p.get("rep") not in reps:
            continue
        out.append(p)
    return out


def _write_payload(path: Path, projects: list[dict], now: datetime,
                   view_label: str | None = None) -> None:
    payload = {
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "generated_at_label": _format_label(now),
        "canvas_id": CANVAS_ID,
        "project_count": len(projects),
        "projects": projects,
    }
    if view_label is not None:
        payload["view_label"] = view_label
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    token = get_access_token()
    raw = fetch_installs(token)
    projects = build_projects(raw)

    now = datetime.now(timezone.utc)
    repo_root = Path(__file__).resolve().parent.parent

    # Always write the full data.json (admin / default view).
    _write_payload(repo_root / "data.json", projects, now)
    print(f"Wrote {len(projects)} projects → data.json")

    # Write per-view filtered files if views.json exists. Each view gets
    # its own data-{slug}.json containing only the matching projects, so a
    # shared ?view=<slug> URL's payload is strictly scoped to that view.
    views_path = repo_root / "views.json"
    if views_path.exists():
        try:
            views = json.loads(views_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            print(f"views.json is invalid: {e}", file=sys.stderr)
            return 1
        for v in views:
            slug = str(v.get("slug") or "").strip()
            label = str(v.get("label") or slug)
            flt = v.get("filter") or {}
            if not slug:
                print(f"Skipping view without slug: {v}", file=sys.stderr)
                continue
            filtered = _apply_filter(projects, flt)
            out_path = repo_root / f"data-{slug}.json"
            _write_payload(out_path, filtered, now, view_label=label)
            print(f"Wrote {len(filtered):3d} projects → {out_path.name} ({label})")

    print(f"(raw Zoho rows fetched: {len(raw)})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
