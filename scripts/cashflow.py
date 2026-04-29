#!/usr/bin/env python3
"""Cash flow forecast layer for the Helio Install pipeline.

Consumes raw Zoho Installs rows (the same feed refresh_data.py fetches) and
produces a cashflow.json payload for the dashboard. The forecast model is
stage-aware:

  Each cash milestone fires at a *trigger event* — almost always a stage
  transition in the Zoho Installs blueprint (substantial completion =
  Inspection entry, PTO = Energized entry, etc.). Resolution rules:

    * If the trigger has fired and the lender lag has elapsed but
      Lending_Status hasn't advanced to "Paid" / "Funded": Past-due.
      This is the only path to past-due. Stage-time / SLA Red has zero
      bearing — that's a velocity concern, not a cash collection one.

    * If the trigger has fired and we're within the lag window or beyond:
      forecast = trigger date + lender lag.

    * If the trigger has not fired yet: forecast =
        today + (sum of Yellow SLA dwells for current stage and every stage
                 between here and the trigger stage) + lender lag.
      Yellow values come from sla_thresholds.json at the repo root —
      single source of truth shared with the dashboard's SLA system.

  This means projects that are stuck in a stage stay forecastable (we
  estimate "from today, full SLA timeline through remaining stages").
  No more "Awaiting" buckets — every forecastable project lands on a
  real future week or in Past-due.

Schedule constants (Harry confirmed 2026-04-28):

  Cash:    20% deposit (signing) / 60% pre-install / 20% subst comp.
           Deposit gets a 7-day grace before flagging past-due.
  LR:      80% install / 20% activation. Lender pays Helio. 14d processing.
  SG:      90% install / 10% PTO. 5d processing.
  CF:      50% Phase 1 (permit approved) / 50% Phase 2 (WT/PTO entry). 5d.
  SE:      1/3 loan docs / 1/3 ICA approval / 1/3 PTO. 5d.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# Stages the cash flow tracker covers (pre-Energized).
ACTIVE_STAGES = {
    "Sales Ops Review", "Project Intake", "Site Survey", "Engineering",
    "Plan Review", "Interconnection", "Permitting", "Procurement & Scheduling",
    "Active Installation", "Inspection", "Witness Test / PTO",
}

# Pipeline-order ranks. On Hold = 0 (side branch).
STAGE_RANKS: dict[str, int] = {
    "On Hold":                   0,
    "Sales Ops Review":          1,
    "Project Intake":            2,
    "Site Survey":               3,
    "Engineering":               4,
    "Plan Review":               5,
    "Interconnection":           6,
    "Permitting":                7,
    "Procurement & Scheduling":  8,
    "Active Installation":       9,
    "Inspection":                10,
    "Witness Test / PTO":        11,
    "Energized":                 12,
    "Project Closeout":          13,
}

# Stages in pipeline order (excluding On Hold side branch).
PIPELINE_STAGES = [
    "Sales Ops Review", "Project Intake", "Site Survey", "Engineering",
    "Plan Review", "Interconnection", "Permitting", "Procurement & Scheduling",
    "Active Installation", "Inspection", "Witness Test / PTO", "Energized",
]

# Lending_Status → (lender_code, % collected, next milestone, next %).
STATUS_MAP: dict[str, tuple[str, float, str, float]] = {
    "Cash - 20PCT deposit invoiced":          ("Cash", 0.00, "Deposit (20%) — invoiced",                0.20),
    "Cash - 20PCT deposit paid":              ("Cash", 0.20, "Pre-install (60%)",                       0.60),
    "Cash - 60PCT invoiced":                  ("Cash", 0.20, "Pre-install (60%) — invoiced",            0.60),
    "Cash - 60PCT paid":                      ("Cash", 0.80, "Substantial completion (20%)",            0.20),
    "Cash - 20PCT final invoiced":            ("Cash", 0.80, "Substantial completion (20%) — invoiced", 0.20),
    "Cash - paid in full":                    ("Cash", 1.00, "—",                                       0.00),
    "LightReach":                             ("LR",   0.00, "Install package",                  0.80),
    "LR - NTP":                               ("LR",   0.00, "Install package",                  0.80),
    "LR - Install Package Submitted":         ("LR",   0.00, "Install package — submitted",      0.80),
    "LR - Install Package Paid":              ("LR",   0.80, "Activation package",               0.20),
    "LR - Activation Package Submitted":      ("LR",   0.80, "Activation package — submitted",   0.20),
    "LR - Activation Package Paid":           ("LR",   1.00, "—",                                0.00),
    "Sungage":                                ("SG",   0.00, "Install package",                  0.90),
    "SG - NTP":                               ("SG",   0.00, "Install package",                  0.90),
    "SG - Install Package Submitted":         ("SG",   0.00, "Install package — submitted",      0.90),
    "SG - Install Package Paid":              ("SG",   0.90, "PTO package",                      0.10),
    "SG - PTO Package Submitted":             ("SG",   0.90, "PTO package — submitted",          0.10),
    "SG - PTO Package Paid":                  ("SG",   1.00, "—",                                0.00),
    "ClimateFirst":                           ("CF",   0.00, "Phase 1 (50%) — permit issued",    0.50),
    "CF - NTP":                               ("CF",   0.00, "Phase 1 (50%) — permit issued",    0.50),
    "CF - Phase 1 Submitted":                 ("CF",   0.00, "Phase 1 funded (50%)",             0.50),
    "CF - Phase 1 Funded":                    ("CF",   0.50, "Phase 2 (50%) — WT/PTO entry",     0.50),
    "CF - Phase 2 Submitted":                 ("CF",   0.50, "Phase 2 funded (50%)",             0.50),
    "CF - Phase 2 Funded":                    ("CF",   1.00, "—",                                0.00),
    "SE - Application Submitted":             ("SE",   0.00,   "1/3 — loan docs signed",           0.3333),
    "SE - Loan Closed 1/3 Payment Funded":    ("SE",   0.3333, "1/3 — interconnection approval",   0.3333),
    "SE- Final 1/3 Payment Funded":           ("SE",   0.6667, "1/3 — PTO (CHECK mapping)",        0.3333),
    "SE- PTO Package Submitted":              ("SE",   0.6667, "1/3 — PTO funded",                 0.3333),
}

LENDER_LAGS = {"LR": 14, "SG": 5, "CF": 5, "SE": 5, "Cash": 0}
CASH_DEPOSIT_GRACE_DAYS = 7
SE_LOAN_DOCS_PROXY_DAYS = 14   # Project_Created + 14d (no native field)


def _load_sla_thresholds() -> dict:
    """Load Yellow SLA values from sla_thresholds.json at repo root.

    The 'yellow' field is the SLA ceiling per stage — used here as the
    expected dwell time when forecasting forward. Falls back to 14d per
    stage if the file is missing (defensive default for first deploy)."""
    repo_root = Path(__file__).resolve().parent.parent
    path = repo_root / "sla_thresholds.json"
    if not path.exists():
        return {s: {"green": 7, "yellow": 14} for s in PIPELINE_STAGES}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {s: {"green": 7, "yellow": 14} for s in PIPELINE_STAGES}
    return data.get("stages", {})


_SLA_CACHE: Optional[dict] = None


def _sla() -> dict:
    global _SLA_CACHE
    if _SLA_CACHE is None:
        _SLA_CACHE = _load_sla_thresholds()
    return _SLA_CACHE


def _yellow(stage: str) -> int:
    """Yellow SLA value for a stage, in days. Defaults to 14 if missing."""
    s = _sla().get(stage)
    if not s:
        return 14
    return int(s.get("yellow", 14))


def _remaining_days_to_rank(current_stage: str, target_rank: int) -> int:
    """Sum Yellow dwells for stages from current_stage's rank up to (but
    not including) target_rank. Used to estimate "from today, how many
    days until the project enters the trigger stage."

    Example: at Interconnection (rank 6), target rank 10 (Inspection):
      Yellow(Interconnection) + Yellow(Permitting) + Yellow(P&S) + Yellow(AI)
      = 14 + 10 + 5 + 21 = 50 days.

    If the current stage's rank is already >= target, returns 0.
    """
    if current_stage not in STAGE_RANKS:
        return 0
    current_rank = STAGE_RANKS[current_stage]
    if current_rank >= target_rank:
        return 0
    total = 0
    for s in PIPELINE_STAGES:
        rank = STAGE_RANKS.get(s, 0)
        if current_rank <= rank < target_rank:
            total += _yellow(s)
    return total


def _parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    s = s.split("T")[0] if "T" in s else s
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def _add_days(d: Optional[date], n: int) -> Optional[date]:
    if d is None:
        return None
    return d + timedelta(days=n)


def _infer_financing_type(financing_type: Optional[str], lending_status: Optional[str]) -> str:
    if financing_type:
        return financing_type
    if not lending_status:
        return ""
    s = lending_status
    if s.startswith("Cash"): return "Cash (inferred)"
    if s.startswith("LR") or s == "LightReach": return "Lease / PPA (inferred)"
    if s.startswith("SG") or s == "Sungage": return "Solar Loan (inferred)"
    if s.startswith("CF") or s == "ClimateFirst": return "Solar Loan (inferred)"
    if s.startswith("SE"): return "Smart-E Loan (inferred)"
    return ""


def _trigger_for(lender: str, next_desc: str) -> tuple[str, Optional[int]]:
    """Returns (trigger_event_label, trigger_stage_rank). trigger_stage_rank
    is None for date-driven triggers (CF Phase 1 = Permit_Approved,
    SE interconnection = ICA_Contingent_Approval)."""
    nd = next_desc.lower()
    if lender == "Cash":
        if "deposit" in nd:        return ("signing",                 0)
        if "pre-install" in nd:    return ("install start",           9)
        if "substantial" in nd:    return ("substantial completion",  10)
    elif lender in ("LR", "SG"):
        if "install" in nd:        return ("substantial completion",  10)
        if "activation" in nd or "PTO" in next_desc:
            return ("Energized (PTO granted)", 12)
    elif lender == "CF":
        if "phase 1" in nd:        return ("permit approval",         None)
        if "phase 2" in nd:        return ("WT/PTO submission",       11)
    elif lender == "SE":
        if "loan docs" in nd:      return ("loan documents signed",   0)
        if "interconnection" in nd:return ("ICA approval",            None)
        if "PTO" in next_desc:     return ("PTO",                     12)
    return ("unmapped", None)


def _compute(lender: str, next_desc: str, next_pct: float, today: date,
             stage: str, lending_status: Optional[str],
             project_created: Optional[date],
             subst: Optional[date],
             permit_approved: Optional[date],
             ica_approval: Optional[date],
             utility_pto: Optional[date],
             projected_install: Optional[date],
             date_of_stage_change: Optional[date]) -> dict:
    """Returns dict: forecast_date (str|None), anchor_source, status
    (paid|on_track|past_due)."""
    if next_pct in (None, 0):
        return {"forecast_date": None, "anchor_source": "100% collected", "status": "paid"}

    trigger_label, trigger_rank = _trigger_for(lender, next_desc)
    project_rank = STAGE_RANKS.get(stage, 0)
    lag = LENDER_LAGS.get(lender, 0)
    nd = next_desc.lower()

    # Has the trigger fired?
    if trigger_label == "permit approval":
        trigger_fired = permit_approved is not None
    elif trigger_label == "ICA approval":
        trigger_fired = ica_approval is not None
    elif trigger_label == "signing":
        trigger_fired = project_created is not None
    elif trigger_label == "loan documents signed":
        trigger_fired = project_created is not None  # proxy: signed shortly after creation
    elif trigger_rank is not None:
        trigger_fired = project_rank >= trigger_rank
    else:
        trigger_fired = False

    # ---- Trigger has fired: use the actual trigger date ----
    if trigger_fired:
        if trigger_label == "signing":
            forecast = project_created
            src = "Project_Created_Date (signing)"
        elif trigger_label == "loan documents signed":
            forecast = _add_days(project_created, SE_LOAN_DOCS_PROXY_DAYS)
            src = f"Project_Created_Date + {SE_LOAN_DOCS_PROXY_DAYS}d (proxy)"
        elif trigger_label == "permit approval":
            forecast = _add_days(permit_approved, lag)
            src = f"Permit_Approved + {lag}d"
        elif trigger_label == "ICA approval":
            forecast = _add_days(ica_approval, lag)
            src = f"ICA_Contingent_Approval + {lag}d"
        elif trigger_label == "install start":  # Cash 60%
            # Past Active Installation — install has happened. Use SC if available.
            anchor = subst or projected_install
            forecast = _add_days(anchor, -7) if anchor else None
            src = ("Substantial_Completion − 7d" if subst else
                   ("Projected_Install_Date − 7d" if projected_install else "no install anchor"))
        elif trigger_label == "substantial completion":  # LR/SG install, Cash 20% subst
            anchor = subst or projected_install
            if "substantial" in nd:  # Cash 20% — no lag
                forecast = anchor
                src = "Substantial_Completion" if subst else ("Projected_Install_Date" if projected_install else "no SC anchor")
            else:  # LR/SG install — + lender lag
                forecast = _add_days(anchor, lag) if anchor else None
                src = (("Substantial_Completion + " if subst else "Projected_Install_Date + ") + f"{lag}d") if anchor else "no SC anchor"
        elif trigger_label == "WT/PTO submission":  # CF Phase 2
            # Past WT/PTO entry. Use Date_of_Stage_Change if available, else SC + a small adjustment.
            if stage == "Witness Test / PTO" and date_of_stage_change:
                forecast = _add_days(date_of_stage_change, lag)
                src = f"WT/PTO entry (Date_of_Stage_Change) + {lag}d"
            elif subst:
                # SC happened, then project moved through Inspection (Yellow=5d) into WT/PTO
                approx_entry = _add_days(subst, _yellow("Inspection"))
                forecast = _add_days(approx_entry, lag)
                src = f"Substantial_Completion + {_yellow('Inspection')}d (Inspection SLA) + {lag}d"
            else:
                forecast = None
                src = "no WT/PTO anchor"
        elif trigger_label in ("Energized (PTO granted)", "PTO"):
            # Past Energized — out of active scope, but defensive fallback.
            if stage == "Energized" and date_of_stage_change:
                forecast = _add_days(date_of_stage_change, lag)
                src = f"Energized entry + {lag}d"
            else:
                forecast = None
                src = "no Energized anchor"
        else:
            forecast = None
            src = "unmapped trigger"

        # Past-due decision.
        if forecast and forecast < today:
            if trigger_label == "signing":
                if today > _add_days(forecast, CASH_DEPOSIT_GRACE_DAYS):
                    return {"forecast_date": forecast.isoformat(), "anchor_source": src, "status": "past_due"}
                return {"forecast_date": forecast.isoformat(), "anchor_source": src, "status": "on_track"}
            return {"forecast_date": forecast.isoformat(), "anchor_source": src, "status": "past_due"}
        return {"forecast_date": forecast.isoformat() if forecast else None,
                "anchor_source": src, "status": "on_track" if forecast else "past_due"}

    # ---- Trigger has not fired: forward-looking estimate using SLA Yellow values ----
    # Stage-aware "remaining time to trigger stage" based on Yellow dwells.
    if trigger_rank is not None:
        remaining = _remaining_days_to_rank(stage, trigger_rank)
        # Special case: Cash 60% pre-install fires when entering Active Installation.
        # The 60% gets invoiced ~7 days BEFORE install, so forecast = trigger - 7.
        adjustment = -7 if "pre-install" in nd else 0
        forecast = _add_days(today, remaining + lag + adjustment)
        # Build source string
        path_stages = []
        cr = STAGE_RANKS.get(stage, 0)
        for s in PIPELINE_STAGES:
            r = STAGE_RANKS.get(s, 0)
            if cr <= r < trigger_rank:
                path_stages.append(f"{s}({_yellow(s)}d)")
        src = f"today + {remaining}d ({' + '.join(path_stages)}) + {lag}d lag"
        if adjustment:
            src += f" − 7d (pre-install)"
        return {"forecast_date": forecast.isoformat(), "anchor_source": src, "status": "on_track"}

    # Date-based triggers awaiting their date field.
    if trigger_label == "permit approval":
        # Estimate when Permit_Approved gets set: through end of Permitting stage.
        permit_rank = STAGE_RANKS["Permitting"]
        # Sum dwells to reach Permitting + Permitting itself
        remaining = _remaining_days_to_rank(stage, permit_rank + 1)
        forecast = _add_days(today, remaining + lag)
        return {"forecast_date": forecast.isoformat(),
                "anchor_source": f"today + {remaining}d (through Permitting) + {lag}d",
                "status": "on_track"}

    if trigger_label == "ICA approval":
        ica_rank = STAGE_RANKS["Interconnection"]
        remaining = _remaining_days_to_rank(stage, ica_rank + 1)
        forecast = _add_days(today, remaining + lag)
        return {"forecast_date": forecast.isoformat(),
                "anchor_source": f"today + {remaining}d (through Interconnection) + {lag}d",
                "status": "on_track"}

    return {"forecast_date": None, "anchor_source": "unmapped", "status": "on_track"}


def _resolve_owner(row: dict) -> str:
    owner_obj = row.get("Owner")
    name = ""
    if isinstance(owner_obj, dict):
        name = (owner_obj.get("name") or "").strip()
    if not name:
        name = (row.get("Project_Owner") or "").strip()
    return name


def _resolve_project_manager(row: dict) -> str:
    pm = row.get("Project_Manager")
    if isinstance(pm, dict):
        return (pm.get("name") or "").strip()
    return ""


def build_projects(rows: list[dict], today: date) -> list[dict]:
    out: list[dict] = []
    for r in rows:
        stage = (r.get("Project_Stage") or "").strip()
        if stage not in ACTIVE_STAGES:
            continue
        lending_status = (r.get("Lending_Status") or "").strip() or None
        sm = STATUS_MAP.get(lending_status) if lending_status else None
        contract_total = r.get("Contract_Total")
        try:
            contract_total = float(contract_total) if contract_total is not None else None
        except (ValueError, TypeError):
            contract_total = None

        if sm:
            lender, pct_collected, next_desc, next_pct = sm
        else:
            lender, pct_collected, next_desc, next_pct = ("?", 0.0, "Unmapped", 0.0)

        subst = _parse_date(r.get("Substantial_Completion"))
        projected_install = _parse_date(r.get("Projected_Install_Date"))
        permit_approved = _parse_date(r.get("Permit_Approved"))
        project_created = _parse_date(r.get("Project_Created_Date"))
        utility_pto = _parse_date(r.get("Utility_PTO"))
        ica_approval = _parse_date(r.get("ICA_Contingent_Approval"))
        date_of_stage_change = _parse_date(r.get("Date_of_Stage_Change"))

        forecast = _compute(
            lender, next_desc, next_pct, today,
            stage, lending_status,
            project_created, subst, permit_approved, ica_approval,
            utility_pto, projected_install, date_of_stage_change,
        )

        next_dollar = (contract_total * next_pct) if (contract_total and next_pct) else 0.0
        dollars_collected = (contract_total * pct_collected) if (contract_total and pct_collected) else 0.0
        dollars_outstanding = (contract_total - dollars_collected) if contract_total else 0.0
        ftype = _infer_financing_type(r.get("Financing_Type"), lending_status)

        flags: list[str] = []
        if contract_total is None or contract_total == 0:
            flags.append("Contract_Total missing or zero")
        if not lending_status:
            flags.append("Lending_Status missing")
        if "(inferred)" in ftype:
            flags.append("Financing_Type inferred from Lending_Status")

        out.append({
            "project_id": (r.get("Project_ID") or "").strip(),
            "customer": (r.get("Name") or "").strip(),
            "stage": stage,
            "owner": _resolve_owner(r),
            "rep": (r.get("Sales_Representative") or "").strip(),
            "project_manager": _resolve_project_manager(r),
            "zoho_record_id": str(r.get("id") or ""),
            "financing_type": ftype,
            "lending_status": lending_status or "",
            "lender": lender,
            "contract_total": contract_total,
            "pct_collected": round(pct_collected, 4),
            "dollars_collected": round(dollars_collected, 2),
            "dollars_outstanding": round(dollars_outstanding, 2),
            "next_milestone": next_desc if next_pct > 0 else "—",
            "next_milestone_pct": round(next_pct, 4),
            "next_dollar": round(next_dollar, 2) if next_pct > 0 else 0.0,
            "forecast_date": forecast["forecast_date"],
            "anchor_source": forecast["anchor_source"],
            "status": forecast["status"],            # paid | on_track | past_due
            "flags": flags,
        })
    return out


def _monday(d: date) -> date:
    return d - timedelta(days=d.weekday())


def _build_weeks(today: date, n_weeks: int = 12) -> list[dict]:
    start = _monday(today)
    return [{
        "index": i,
        "start": (start + timedelta(weeks=i)).isoformat(),
        "end":   (start + timedelta(weeks=i, days=6)).isoformat(),
        "label": (start + timedelta(weeks=i)).strftime("Wk %m/%d"),
    } for i in range(n_weeks)]


def _bucket_for(p: dict, weeks: list[dict]) -> str:
    status = p.get("status")
    if status == "paid":
        return "Paid"
    if status == "past_due":
        return "Past-due"
    fd = _parse_date(p.get("forecast_date"))
    if not fd:
        return "Past-due"  # status=on_track but no date — defensive
    first_start = _parse_date(weeks[0]["start"])
    last_end = _parse_date(weeks[-1]["end"])
    if fd < first_start:
        return "Past-due"
    if fd > last_end:
        return "Beyond 12 wks"
    for w in weeks:
        if _parse_date(w["start"]) <= fd <= _parse_date(w["end"]):
            return w["label"]
    return "Beyond 12 wks"


def compute_cashflow(raw_rows: list[dict], now: datetime) -> dict:
    today = now.astimezone(timezone.utc).date()
    projects = build_projects(raw_rows, today)
    weeks = _build_weeks(today, n_weeks=12)

    weekly_totals: dict[str, float] = {w["label"]: 0.0 for w in weeks}
    weekly_totals["Past-due"] = 0.0
    weekly_totals["Beyond 12 wks"] = 0.0

    total_outstanding = 0.0
    total_collected = 0.0
    next_30_days = 0.0
    this_week_total = 0.0

    cutoff_30 = today + timedelta(days=30)
    first_week_start = _parse_date(weeks[0]["start"])
    first_week_end = _parse_date(weeks[0]["end"])

    for p in projects:
        bucket = _bucket_for(p, weeks)
        p["bucket"] = bucket
        amt = p.get("next_dollar") or 0
        if bucket in weekly_totals:
            weekly_totals[bucket] += amt
        total_outstanding += p["dollars_outstanding"] or 0
        total_collected += p["dollars_collected"] or 0
        if p["forecast_date"] and p["status"] == "on_track":
            fd = _parse_date(p["forecast_date"])
            if fd and first_week_start <= fd <= first_week_end:
                this_week_total += amt
            if fd and today <= fd <= cutoff_30:
                next_30_days += amt

    return {
        "generated_at": now.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "today": today.isoformat(),
        "weeks": weeks,
        "weekly_totals": {k: round(v, 2) for k, v in weekly_totals.items()},
        "summary": {
            "project_count": len(projects),
            "total_outstanding": round(total_outstanding, 2),
            "total_collected": round(total_collected, 2),
            "this_week": round(this_week_total, 2),
            "next_30_days": round(next_30_days, 2),
            "past_due": round(weekly_totals.get("Past-due", 0.0), 2),
            "beyond_12_wks": round(weekly_totals.get("Beyond 12 wks", 0.0), 2),
        },
        "projects": projects,
    }


def apply_filter(payload: dict, view_filter: dict) -> dict:
    owners = set(view_filter.get("owners") or [])
    reps = set(view_filter.get("reps") or [])
    out_projects = [p for p in payload["projects"]
                    if (not owners or p.get("owner") in owners)
                    and (not reps or p.get("rep") in reps)]

    today = _parse_date(payload["today"])
    weeks = payload["weeks"]
    weekly_totals = {w["label"]: 0.0 for w in weeks}
    weekly_totals["Past-due"] = 0.0
    weekly_totals["Beyond 12 wks"] = 0.0

    total_outstanding = 0.0
    total_collected = 0.0
    this_week_total = 0.0
    next_30_days = 0.0
    cutoff_30 = today + timedelta(days=30)
    first_week_start = _parse_date(weeks[0]["start"])
    first_week_end = _parse_date(weeks[0]["end"])

    for p in out_projects:
        bucket = p.get("bucket") or "Past-due"
        amt = p.get("next_dollar") or 0
        if bucket in weekly_totals:
            weekly_totals[bucket] += amt
        total_outstanding += p.get("dollars_outstanding", 0) or 0
        total_collected += p.get("dollars_collected", 0) or 0
        if p.get("forecast_date") and p.get("status") == "on_track":
            fd = _parse_date(p["forecast_date"])
            if fd and first_week_start <= fd <= first_week_end:
                this_week_total += amt
            if fd and today <= fd <= cutoff_30:
                next_30_days += amt

    return {
        "generated_at": payload["generated_at"],
        "today": payload["today"],
        "weeks": weeks,
        "weekly_totals": {k: round(v, 2) for k, v in weekly_totals.items()},
        "summary": {
            "project_count": len(out_projects),
            "total_outstanding": round(total_outstanding, 2),
            "total_collected": round(total_collected, 2),
            "this_week": round(this_week_total, 2),
            "next_30_days": round(next_30_days, 2),
            "past_due": round(weekly_totals.get("Past-due", 0.0), 2),
            "beyond_12_wks": round(weekly_totals.get("Beyond 12 wks", 0.0), 2),
        },
        "projects": out_projects,
    }
