#!/usr/bin/env python3
"""Cash flow forecast layer for the Helio Install pipeline.

Consumes the same raw Zoho Installs rows that refresh_data.py fetches and
produces a cashflow.json payload that the dashboard can render. Owns:

  * Schedule lookup per Lending_Status (% collected, next milestone, next %)
  * Install-anchor resolution: Substantial_Completion → Projected_Install_Date
    → Permit_Approved + 7d → Project_Created_Date + 45d → Unscheduled
  * Lender processing lag: LR=14d, SG=5d, CF=5d, SE=5d
  * Weekly bucketing for the 12-week rolling cash flow forecast

The Excel spreadsheet "Cash Flow Tracker - Starter.xlsx" is the original
home for this logic; this module is the canonical Python port that runs
in CI alongside the existing pipeline-monitor refresh.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# Cash flow scope: pre-Energized stages. Energized + Closeout + Canceled
# are excluded; On Hold is excluded from cash-flow forecasting (kept only
# for the spreadsheet's separate "On Hold" tab).
ACTIVE_STAGES = {
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
}

# Lending_Status (current Zoho picklist value) → (lender_code, % collected,
# next milestone description, next milestone %).
STATUS_MAP: dict[str, tuple[str, float, str, float]] = {
    # Cash schedule: 20% deposit / 60% pre-install / 20% substantial completion
    "Cash - 20PCT deposit invoiced":          ("Cash", 0.00, "Deposit (20%) — invoiced",                0.20),
    "Cash - 20PCT deposit paid":              ("Cash", 0.20, "Pre-install (60%)",                       0.60),
    "Cash - 60PCT invoiced":                  ("Cash", 0.20, "Pre-install (60%) — invoiced",            0.60),
    "Cash - 60PCT paid":                      ("Cash", 0.80, "Substantial completion (20%)",            0.20),
    "Cash - 20PCT final invoiced":            ("Cash", 0.80, "Substantial completion (20%) — invoiced", 0.20),
    "Cash - paid in full":                    ("Cash", 1.00, "—",                                       0.00),
    # Lightreach (Lease/PPA): 80% at install / 20% at PTO
    "LightReach":                             ("LR",   0.00, "Install package",                  0.80),
    "LR - NTP":                               ("LR",   0.00, "Install package",                  0.80),
    "LR - Install Package Submitted":         ("LR",   0.00, "Install package — submitted",      0.80),
    "LR - Install Package Paid":              ("LR",   0.80, "Activation package",               0.20),
    "LR - Activation Package Submitted":      ("LR",   0.80, "Activation package — submitted",   0.20),
    "LR - Activation Package Paid":           ("LR",   1.00, "—",                                0.00),
    # Sungage (loan): 90% at install / 10% at PTO
    "Sungage":                                ("SG",   0.00, "Install package",                  0.90),
    "SG - NTP":                               ("SG",   0.00, "Install package",                  0.90),
    "SG - Install Package Submitted":         ("SG",   0.00, "Install package — submitted",      0.90),
    "SG - Install Package Paid":              ("SG",   0.90, "PTO package",                      0.10),
    "SG - PTO Package Submitted":             ("SG",   0.90, "PTO package — submitted",          0.10),
    "SG - PTO Package Paid":                  ("SG",   1.00, "—",                                0.00),
    # ClimateFirst (loan): 50% at permit issue / 50% at substantial completion
    "ClimateFirst":                           ("CF",   0.00, "Phase 1 (50%) — permit issued",    0.50),
    "CF - NTP":                               ("CF",   0.00, "Phase 1 (50%) — permit issued",    0.50),
    "CF - Phase 1 Submitted":                 ("CF",   0.00, "Phase 1 funded (50%)",             0.50),
    "CF - Phase 1 Funded":                    ("CF",   0.50, "Phase 2 (50%) — substantial comp.",0.50),
    "CF - Phase 2 Submitted":                 ("CF",   0.50, "Phase 2 funded (50%)",             0.50),
    "CF - Phase 2 Funded":                    ("CF",   1.00, "—",                                0.00),
    # Smart-E (loan): 1/3 loan docs / 1/3 interconnection / 1/3 PTO
    "SE - Application Submitted":             ("SE",   0.00,   "1/3 — loan docs signed",           0.3333),
    "SE - Loan Closed 1/3 Payment Funded":    ("SE",   0.3333, "1/3 — interconnection approval",   0.3333),
    "SE- Final 1/3 Payment Funded":           ("SE",   0.6667, "1/3 — PTO (CHECK mapping)",        0.3333),
    "SE- PTO Package Submitted":              ("SE",   0.6667, "1/3 — PTO funded",                 0.3333),
}

LENDER_LAGS = {"LR": 14, "SG": 5, "CF": 5, "SE": 5, "Cash": 0}

# Heuristic constants for install-anchor resolution.
PERMIT_TO_INSTALL_DAYS = 7         # Permit_Approved → est. install
CREATION_TO_INSTALL_DAYS = 45      # Project_Created_Date → est. install


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
    """Derive Financing_Type from Lending_Status when the field is null."""
    if financing_type:
        return financing_type
    if not lending_status:
        return ""
    s = lending_status
    if s.startswith("Cash"):
        return "Cash (inferred)"
    if s.startswith("LR") or s == "LightReach":
        return "Lease / PPA (inferred)"
    if s.startswith("SG") or s == "Sungage":
        return "Solar Loan (inferred)"
    if s.startswith("CF") or s == "ClimateFirst":
        return "Solar Loan (inferred)"
    if s.startswith("SE"):
        return "Smart-E Loan (inferred)"
    return ""


def _resolve_install_anchor(
    subst_completion: Optional[date],
    projected_install: Optional[date],
    permit_approved: Optional[date],
    project_created: Optional[date],
) -> tuple[Optional[date], str]:
    """Lookup order: SC → PI → Permit+7d → Created+45d → None."""
    if subst_completion:
        return subst_completion, "Substantial_Completion"
    if projected_install:
        return projected_install, "Projected_Install_Date"
    if permit_approved:
        return _add_days(permit_approved, PERMIT_TO_INSTALL_DAYS), \
               f"Permit_Approved + {PERMIT_TO_INSTALL_DAYS}d (heuristic)"
    if project_created:
        return _add_days(project_created, CREATION_TO_INSTALL_DAYS), \
               f"Project_Created_Date + {CREATION_TO_INSTALL_DAYS}d (heuristic)"
    return None, "no anchor"


def _compute_forecast(
    lender: str,
    next_desc: str,
    subst_completion: Optional[date],
    projected_install: Optional[date],
    permit_approved: Optional[date],
    project_created: Optional[date],
    utility_pto: Optional[date],
    ica_approval: Optional[date],
) -> tuple[Optional[date], str]:
    """Compute the next-milestone forecast date for a single project.

    Returns (forecast_date_or_None, anchor_source_string).
    """
    lag = LENDER_LAGS.get(lender, 0)
    nd = next_desc.lower()

    # Cash schedule
    if lender == "Cash":
        if "deposit" in nd and "invoiced" in nd:
            return project_created, "Project_Created_Date"
        anchor, src = _resolve_install_anchor(subst_completion, projected_install, permit_approved, project_created)
        if anchor is None:
            return None, src
        if "pre-install" in nd:
            return _add_days(anchor, -7), f"{src} − 7d"
        if "substantial" in nd:
            return anchor, src
    # LR / SG: install + activation/PTO milestones
    elif lender in ("LR", "SG"):
        if "install package" in nd:
            anchor, src = _resolve_install_anchor(subst_completion, projected_install, permit_approved, project_created)
            if anchor is None:
                return None, src
            return _add_days(anchor, lag), f"{src} + {lag}d"
        if "activation" in nd or "PTO" in next_desc:
            if utility_pto:
                return _add_days(utility_pto, lag), f"Utility_PTO + {lag}d"
            if subst_completion:
                # Rough estimate: utility issues PTO ~30d after substantial completion.
                return _add_days(subst_completion, 30 + lag), \
                       f"Substantial_Completion + 30d + {lag}d (rough)"
            return None, "no PTO anchor"
    # CF: Phase 1 at permit / Phase 2 at substantial completion
    elif lender == "CF":
        if "phase 1" in nd:
            if permit_approved:
                return _add_days(permit_approved, lag), f"Permit_Approved + {lag}d"
            return None, "no permit anchor"
        if "phase 2" in nd:
            if subst_completion:
                return _add_days(subst_completion, lag), f"Substantial_Completion + {lag}d"
            if projected_install:
                return _add_days(projected_install, lag), f"Projected_Install_Date + {lag}d"
            return None, "no subst comp anchor"
    # SE: 1/3 / 1/3 / 1/3 — loan docs proxy = creation + 14d
    elif lender == "SE":
        if "loan docs" in nd:
            if project_created:
                return _add_days(project_created, 14), "Project_Created_Date + 14d (proxy)"
            return None, "no creation date"
        if "interconnection" in nd:
            if ica_approval:
                return _add_days(ica_approval, lag), f"ICA_Contingent_Approval + {lag}d"
            return None, "no ICA anchor"
        if "PTO" in next_desc:
            if utility_pto:
                return _add_days(utility_pto, lag), f"Utility_PTO + {lag}d"
            if subst_completion:
                return _add_days(subst_completion, 30 + lag), f"Substantial_Completion + 30d + {lag}d (rough)"
            return None, "no PTO anchor"
    return None, "unmapped"


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


def build_projects(rows: list[dict]) -> list[dict]:
    """Walk raw Zoho Installs rows and produce per-project cash flow records."""
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

        forecast_date, anchor_source = _compute_forecast(
            lender, next_desc,
            subst, projected_install, permit_approved, project_created,
            utility_pto, ica_approval,
        ) if next_pct > 0 else (None, "100% collected" if pct_collected >= 1.0 else "no schedule")

        next_dollar = (contract_total * next_pct) if (contract_total and next_pct) else 0.0
        dollars_collected = (contract_total * pct_collected) if (contract_total and pct_collected) else 0.0
        dollars_outstanding = (contract_total - dollars_collected) if contract_total else 0.0

        # Inferred Financing_Type
        ftype = _infer_financing_type(r.get("Financing_Type"), lending_status)

        flags: list[str] = []
        if contract_total is None or contract_total == 0:
            flags.append("Contract_Total missing or zero")
        if not lending_status:
            flags.append("Lending_Status missing")
        if "(inferred)" in ftype:
            flags.append("Financing_Type inferred from Lending_Status")
        if next_pct > 0 and forecast_date is None:
            flags.append("Forecast unscheduled — no anchor")

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
            "forecast_date": forecast_date.isoformat() if forecast_date else None,
            "anchor_source": anchor_source,
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


def _bucket_for(forecast_date: Optional[str], weeks: list[dict]) -> str:
    if not forecast_date:
        return "Unscheduled"
    fd = _parse_date(forecast_date)
    if fd is None:
        return "Unscheduled"
    first_start = _parse_date(weeks[0]["start"])
    last_end = _parse_date(weeks[-1]["end"])
    if fd < first_start:
        return "Past-due"
    if fd > last_end:
        return "Beyond 12 wks"
    for w in weeks:
        if _parse_date(w["start"]) <= fd <= _parse_date(w["end"]):
            return w["label"]
    return "Unscheduled"


def compute_cashflow(raw_rows: list[dict], now: datetime) -> dict:
    """Top-level entry point: returns the cashflow.json payload structure."""
    today = now.astimezone(timezone.utc).date()
    projects = build_projects(raw_rows)
    weeks = _build_weeks(today, n_weeks=12)

    # Bucket each project + compute aggregate weekly totals
    weekly_totals: dict[str, float] = {w["label"]: 0.0 for w in weeks}
    weekly_totals["Past-due"] = 0.0
    weekly_totals["Unscheduled"] = 0.0
    weekly_totals["Beyond 12 wks"] = 0.0

    total_outstanding = 0.0
    total_collected = 0.0
    next_30_days = 0.0
    this_week_total = 0.0

    cutoff_30 = today + timedelta(days=30)
    first_week_start = _parse_date(weeks[0]["start"])
    first_week_end = _parse_date(weeks[0]["end"])

    for p in projects:
        bucket = _bucket_for(p["forecast_date"], weeks)
        p["bucket"] = bucket
        if p["next_dollar"]:
            weekly_totals[bucket] = weekly_totals.get(bucket, 0.0) + p["next_dollar"]
        total_outstanding += p["dollars_outstanding"] or 0.0
        total_collected += p["dollars_collected"] or 0.0
        if p["forecast_date"]:
            fd = _parse_date(p["forecast_date"])
            if fd and first_week_start <= fd <= first_week_end:
                this_week_total += p["next_dollar"] or 0.0
            if fd and today <= fd <= cutoff_30:
                next_30_days += p["next_dollar"] or 0.0

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
            "unscheduled": round(weekly_totals.get("Unscheduled", 0.0)
                                 + weekly_totals.get("Beyond 12 wks", 0.0), 2),
        },
        "projects": projects,
    }


def apply_filter(payload: dict, view_filter: dict) -> dict:
    """Return a payload scoped to the projects matching view_filter (owner/rep)."""
    owners = set(view_filter.get("owners") or [])
    reps = set(view_filter.get("reps") or [])
    out_projects = []
    for p in payload["projects"]:
        if owners and p.get("owner") not in owners:
            continue
        if reps and p.get("rep") not in reps:
            continue
        out_projects.append(p)

    # Recompute aggregates for the filtered set.
    today = _parse_date(payload["today"])
    weeks = payload["weeks"]
    weekly_totals = {w["label"]: 0.0 for w in weeks}
    weekly_totals["Past-due"] = 0.0
    weekly_totals["Unscheduled"] = 0.0
    weekly_totals["Beyond 12 wks"] = 0.0

    total_outstanding = 0.0
    total_collected = 0.0
    this_week_total = 0.0
    next_30_days = 0.0
    cutoff_30 = today + timedelta(days=30)
    first_week_start = _parse_date(weeks[0]["start"])
    first_week_end = _parse_date(weeks[0]["end"])

    for p in out_projects:
        bucket = p.get("bucket") or "Unscheduled"
        if p.get("next_dollar"):
            weekly_totals[bucket] = weekly_totals.get(bucket, 0.0) + p["next_dollar"]
        total_outstanding += p.get("dollars_outstanding", 0) or 0
        total_collected += p.get("dollars_collected", 0) or 0
        if p.get("forecast_date"):
            fd = _parse_date(p["forecast_date"])
            if fd and first_week_start <= fd <= first_week_end:
                this_week_total += p.get("next_dollar", 0) or 0
            if fd and today <= fd <= cutoff_30:
                next_30_days += p.get("next_dollar", 0) or 0

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
            "unscheduled": round(weekly_totals.get("Unscheduled", 0.0)
                                 + weekly_totals.get("Beyond 12 wks", 0.0), 2),
        },
        "projects": out_projects,
    }
