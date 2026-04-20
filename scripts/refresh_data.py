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
import statistics
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
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
    "Project_Owner,Date_of_Stage_Change,"
    "Last_Reviewed_At,Last_Reviewed_By,Last_Review_Notes"
)

# Stage-change tracking was enabled in Zoho on 2026-04-16. Records that
# haven't moved stages since then have Date_of_Stage_Change = null. We
# treat those as "last changed on the launch date" so they show a
# consistent, daily-incrementing floor until they actually move stages
# and pick up a real timestamp. Midnight ET on the launch date.
STAGE_TRACKING_LAUNCH_TS = "2026-04-16T00:00:00-04:00"

# Velocity stats (pipeline_velocity.json + stage_history.json) share the same
# cutoff. Any stage span that started before this date is marked truncated and
# is excluded from dwell/transition medians, because we don't have reliable
# entry timestamps for pre-cutoff history.
VELOCITY_CUTOFF_TS = STAGE_TRACKING_LAUNCH_TS
VELOCITY_CUTOFF_DT = datetime.fromisoformat(VELOCITY_CUTOFF_TS).astimezone(timezone.utc)

# Ordered stage list used for dwell-per-stage reporting (Project Closeout is
# not an active stage and has no dwell story worth plotting).
VELOCITY_STAGE_ORDER = [s for s in ACTIVE_STAGES if s != "On Hold"]

# Transitions the dashboard highlights. Each entry is (label, from, to);
# "__creation__" means "from the first observed stage in a project's timeline"
# (only counts if the first span is NOT truncated to the cutoff).
# Labels use Helio's business terminology — internal Zoho stage names (e.g.
# "Active Installation", "Inspection") are mapped to customer-facing terms
# ("Install Ready", "Install Complete") for the velocity cards only.
KEY_TRANSITIONS = [
    ("Sold to Install Ready",          "__creation__", "Active Installation"),
    ("Sold to Install Complete",       "__creation__", "Inspection"),
    ("Sold to Energized",              "__creation__", "Energized"),
    ("Install Complete to Energized",  "Inspection",   "Energized"),
]

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

        # PM-review fields (stamped by the "Mark as Reviewed" button in Zoho).
        # Last_Reviewed_By is a user lookup — returned as {"name": "...",
        # "id": "..."} or null. We expose the display name; the dashboard uses
        # the timestamp + name for the tooltip and the staleness comparison.
        last_reviewed_at = r.get("Last_Reviewed_At") or None
        reviewer_obj = r.get("Last_Reviewed_By")
        last_reviewed_by = ""
        if isinstance(reviewer_obj, dict):
            last_reviewed_by = (reviewer_obj.get("name") or "").strip()
        last_review_notes = (r.get("Last_Review_Notes") or "").strip()

        out.append({
            "project_id": (r.get("Project_ID") or "").strip(),
            "customer": (r.get("Name") or "").strip(),
            "stage": stage,
            "stage_entered_at": stage_change_ts,
            "days_in_stage": _days_since(stage_change_ts),
            "rep": (r.get("Sales_Representative") or "").strip(),
            "owner": owner_name,
            "last_reviewed_at": last_reviewed_at,
            "last_reviewed_by": last_reviewed_by,
            "last_review_notes": last_review_notes,
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


# =============================================================================
# Stage history + velocity tracking (pipeline_velocity.json)
# =============================================================================
#
# stage_history.json is a persistent store maintained across nightly runs and
# committed to the repo. Each record's stage transitions are captured as a
# list of spans: {stage, entered_at, exited_at, truncated}. A "truncated" span
# is one whose entered_at was clamped to the cutoff because the real entry
# happened before we started tracking — it's kept for dwell calculation of
# later spans, but the span itself is excluded from stats.
#
# On each run we walk the current Installs feed and:
#   * If a record has no history yet: seed it with a single open span at its
#     current stage. If Date_of_Stage_Change is before cutoff, mark truncated.
#   * If a record's latest span stage matches the current stage: no change.
#   * If the stage has changed: close the latest span at Date_of_Stage_Change
#     (or "now" if null) and open a new span for the new stage.
#   * If a record disappears from the active feed (Closed/Cancelled): close
#     any open span at "now".


def _parse_iso_utc(s: str) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def load_stage_history(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            print(f"stage_history.json is invalid ({e}); starting fresh", file=sys.stderr)
    return {
        "version": 1,
        "cutoff": VELOCITY_CUTOFF_TS,
        "last_run": None,
        "projects": {},
    }


def update_stage_history(history: dict, raw_rows: list[dict], now: datetime) -> dict:
    """Mutate history in place to reflect the current Installs feed."""
    projects = history.setdefault("projects", {})
    now_iso = _iso_utc(now)
    seen_ids: set[str] = set()

    for r in raw_rows:
        stage = (r.get("Project_Stage") or "").strip()
        if stage not in ACTIVE_STAGES_SET:
            continue
        record_id = str(r.get("id") or "").strip()
        if not record_id:
            continue
        seen_ids.add(record_id)

        # Owner resolution matches build_projects() above.
        owner_name = ""
        owner_obj = r.get("Owner")
        if isinstance(owner_obj, dict):
            owner_name = (owner_obj.get("name") or "").strip()
        if not owner_name:
            owner_name = (r.get("Project_Owner") or "").strip()

        stage_change_ts = r.get("Date_of_Stage_Change")
        created_ts = r.get("Created_Time")
        transition_dt = _parse_iso_utc(stage_change_ts) or _parse_iso_utc(created_ts) or now
        clamped_dt = max(transition_dt, VELOCITY_CUTOFF_DT)
        clamped_iso = _iso_utc(clamped_dt)
        was_truncated = transition_dt < VELOCITY_CUTOFF_DT

        entry = projects.setdefault(record_id, {
            "project_id": (r.get("Project_ID") or "").strip(),
            "customer": (r.get("Name") or "").strip(),
            "owner": owner_name,
            "rep": (r.get("Sales_Representative") or "").strip(),
            "spans": [],
        })
        # keep metadata current (customer names/owners can change)
        if r.get("Project_ID"): entry["project_id"] = (r["Project_ID"] or "").strip()
        if r.get("Name"): entry["customer"] = (r["Name"] or "").strip()
        if owner_name: entry["owner"] = owner_name
        if r.get("Sales_Representative"): entry["rep"] = (r["Sales_Representative"] or "").strip()

        spans = entry["spans"]
        if not spans:
            # We don't trust Zoho's Date_of_Stage_Change as a proxy for
            # "when did this project enter this stage" — that field gets
            # bumped on non-stage saves (blueprints, field edits, etc.).
            # On first-time seed we admit we don't know: stamp entered_at
            # at the cutoff and mark truncated so this span is excluded
            # from dwell/transition stats. Only stage changes observed
            # across subsequent runs will contribute real data.
            spans.append({
                "stage": stage,
                "entered_at": _iso_utc(VELOCITY_CUTOFF_DT),
                "exited_at": None,
                "truncated": True,
            })
            continue

        last = spans[-1]
        if last["stage"] == stage and last["exited_at"] is None:
            continue  # still in the same stage, nothing to update

        # Stage has changed since our last snapshot. Use `now` as the
        # transition time — we observed the change at this run, and we
        # don't trust Zoho's Date_of_Stage_Change (see first-seed comment).
        # The true transition happened sometime between last_run and now;
        # stamping at now is a small overestimate of the previous stage's
        # dwell and a small underestimate of this one, which averages out.
        last["exited_at"] = now_iso
        spans.append({
            "stage": stage,
            "entered_at": now_iso,
            "exited_at": None,
            "truncated": False,  # transition observed after cutoff = real data
        })

    # Close spans for records that have left the active feed entirely.
    for record_id, entry in projects.items():
        if record_id in seen_ids:
            continue
        spans = entry.get("spans") or []
        if spans and spans[-1].get("exited_at") is None:
            spans[-1]["exited_at"] = now_iso

    history["last_run"] = now_iso
    history["cutoff"] = VELOCITY_CUTOFF_TS
    return history


def _on_hold_overlap_seconds(start: datetime, end: datetime, spans: list[dict]) -> float:
    """Seconds of overlap between [start,end] and any On Hold spans."""
    if end <= start:
        return 0.0
    total = 0.0
    for s in spans:
        if s.get("stage") != "On Hold":
            continue
        s_start = _parse_iso_utc(s.get("entered_at"))
        s_end = _parse_iso_utc(s.get("exited_at")) or end
        if not s_start:
            continue
        lo = max(start, s_start)
        hi = min(end, s_end)
        if hi > lo:
            total += (hi - lo).total_seconds()
    return total


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    sv = sorted(values)
    if len(sv) == 1:
        return sv[0]
    # linear interpolation
    k = (len(sv) - 1) * (pct / 100.0)
    f, c = int(k), min(int(k) + 1, len(sv) - 1)
    if f == c:
        return sv[f]
    return sv[f] + (sv[c] - sv[f]) * (k - f)


def compute_velocity(history: dict, now: datetime,
                     owner_filter: set[str] | None = None) -> dict:
    projects_map = history.get("projects") or {}
    proj_iter = []
    for record_id, entry in projects_map.items():
        if owner_filter and entry.get("owner") not in owner_filter:
            continue
        proj_iter.append((record_id, entry))

    # Per-stage dwell: only count spans that are NOT truncated. Closed spans
    # use their full duration; currently-open spans contribute their running
    # duration up to "now" (useful for stages where projects tend to pile up,
    # like Interconnection).
    dwell_samples: dict[str, list[float]] = {s: [] for s in VELOCITY_STAGE_ORDER}
    dwell_open: dict[str, int] = {s: 0 for s in VELOCITY_STAGE_ORDER}

    for _, entry in proj_iter:
        for sp in entry.get("spans") or []:
            if sp.get("truncated"):
                continue
            stage = sp.get("stage")
            if stage not in dwell_samples:
                continue
            start = _parse_iso_utc(sp.get("entered_at"))
            end = _parse_iso_utc(sp.get("exited_at")) or now
            if not start or end <= start:
                continue
            if sp.get("exited_at") is None:
                dwell_open[stage] += 1
            days = (end - start).total_seconds() / 86400.0
            dwell_samples[stage].append(days)

    dwell_rows = []
    for s in VELOCITY_STAGE_ORDER:
        vals = dwell_samples[s]
        dwell_rows.append({
            "stage": s,
            "sample_count": len(vals),
            "open_count": dwell_open[s],
            "median_days": _percentile(vals, 50),
            "p75_days": _percentile(vals, 75),
        })

    # Named transitions.
    transitions = []
    for label, from_stage, to_stage in KEY_TRANSITIONS:
        samples = []
        for _, entry in proj_iter:
            spans = entry.get("spans") or []
            if not spans:
                continue
            # Locate the "from" anchor
            if from_stage == "__creation__":
                first = spans[0]
                if first.get("truncated"):
                    continue  # real creation date unknown
                from_dt = _parse_iso_utc(first.get("entered_at"))
            else:
                from_dt = None
                for sp in spans:
                    if sp.get("stage") == from_stage and not sp.get("truncated"):
                        from_dt = _parse_iso_utc(sp.get("entered_at"))
                        break
            if not from_dt:
                continue
            # Locate the "to" entry that follows from_dt
            to_dt = None
            for sp in spans:
                if sp.get("stage") != to_stage:
                    continue
                candidate = _parse_iso_utc(sp.get("entered_at"))
                if candidate and candidate >= from_dt:
                    to_dt = candidate
                    break
            if not to_dt:
                continue
            total_seconds = (to_dt - from_dt).total_seconds()
            if total_seconds <= 0:
                continue
            on_hold = _on_hold_overlap_seconds(from_dt, to_dt, spans)
            samples.append(max(0.0, (total_seconds - on_hold) / 86400.0))
        transitions.append({
            "label": label,
            "from": from_stage,
            "to": to_stage,
            "sample_count": len(samples),
            "median_days": _percentile(samples, 50),
            "p75_days": _percentile(samples, 75),
        })

    return {
        "generated_at": _iso_utc(now),
        "cutoff": VELOCITY_CUTOFF_TS,
        "project_count": len(proj_iter),
        "transitions": transitions,
        "dwell_per_stage": dwell_rows,
        "notes": [
            "Tracking started 2026-04-16. Projects with a stage entered before the cutoff "
            "are excluded from that span's stats (but still count in later transitions).",
            "On Hold time is subtracted from transition durations; On Hold itself is not "
            "plotted as a dwell stage.",
            "Currently-open spans contribute their running duration to dwell stats.",
        ],
    }


def _write_velocity(path: Path, payload: dict, view_label: str | None = None) -> None:
    if view_label is not None:
        payload = dict(payload)  # shallow copy so the per-view label doesn't leak
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

    # Update the persistent stage-history store and derive velocity stats.
    history_path = repo_root / "stage_history.json"
    history = load_stage_history(history_path)
    update_stage_history(history, raw, now)
    history_path.write_text(
        json.dumps(history, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    total_spans = sum(len(p.get("spans") or []) for p in history["projects"].values())
    print(f"Wrote {len(history['projects'])} project histories "
          f"({total_spans} spans) → stage_history.json")

    # Default (admin) velocity file: no owner filter.
    velocity = compute_velocity(history, now)
    _write_velocity(repo_root / "pipeline_velocity.json", velocity)
    print(f"Wrote pipeline_velocity.json "
          f"(projects in scope: {velocity['project_count']})")

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

            # Per-view velocity uses only owner-filter for now. Rep filter is
            # easy to add later if we ever ship rep-scoped share links.
            owner_set = set(flt.get("owners") or []) or None
            v_payload = compute_velocity(history, now, owner_filter=owner_set)
            v_out = repo_root / f"pipeline_velocity-{slug}.json"
            _write_velocity(v_out, v_payload, view_label=label)
            print(f"Wrote velocity {v_out.name} "
                  f"({v_payload['project_count']} projects scope)")

    print(f"(raw Zoho rows fetched: {len(raw)})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
