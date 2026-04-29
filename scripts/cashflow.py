#!/usr/bin/env python3
"""Cash flow forecast layer for the Helio Install pipeline.

Consumes the same raw Zoho Installs rows that refresh_data.py fetches and
produces a cashflow.json payload that the dashboard can render.

Forecast model (Harry confirmed 2026-04-28):

  Each milestone fires at a *trigger event* — almost always a stage transition
  in the Zoho Installs blueprint (substantial completion = Inspection entry,
  PTO = Energized entry, etc.). For each project we compare the project's
  current Project_Stage to the trigger's stage rank and route the milestone
  into one of three states:

    * trigger fired + lender lag elapsed + status not advanced → Past-due
        (real chase: cash should already be in but isn't)
    * trigger fired + payment expected in a near-future week → forecast bucket
    * trigger NOT fired → estimate using current-stage signals and route to
        a future weekly bucket (if estimate is realistic) or to the
        "Awaiting [event]" bucket (if the estimate has already lapsed —
        meaning the project is running slower than the heuristic predicted,
        and surfacing it as past-due would be a false alarm because no
        chase is warranted yet).

  Trigger-event mapping (lender → milestone → trigger):

    Cash 20% deposit      → signing (Project_Created_Date), 7-day grace
    Cash 60% pre-install  → Active Installation entry (install start)
    Cash 20% subst comp   → Inspection entry (substantial completion)
    LR 80% install        → Inspection entry, +14d lender lag
    LR 20% activation     → Energized entry, +14d lender lag
    SG 90% install        → Inspection entry, +5d
    SG 10% PTO            → Energized entry, +5d
    CF 50% Phase 1        → Permit_Approved date, +5d
    CF 50% Phase 2        → Witness Test / PTO entry, +5d
    SE 1/3 loan docs      → Project_Created_Date + 14d (proxy)
    SE 1/3 interconnection→ ICA_Contingent_Approval date, +5d
    SE 1/3 PTO            → Energized entry, +5d

  Awaiting-stage estimates (when the trigger stage hasn't yet been entered):

    For Energized-trigger milestones (LR/SG activation, SE PTO):
      forecast = WT/PTO entry + 21d + lender lag
      WT/PTO entry: Date_of_Stage_Change if currently at WT/PTO, else
                    Substantial_Completion + 28d (Inspection takes a few
                    weeks before WT/PTO entry).

    For install-trigger milestones (LR/SG install, Cash 60%/20%subst,
    CF Phase 2): standard install-anchor lookup —
      Substantial_Completion → Projected_Install_Date → Permit_Approved + 7d
      → Project_Created_Date + 45d.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# Stages the cash flow tracker covers (pre-Energized). Energized appears in
# STAGE_RANKS for trigger comparisons but isn't a current-state filter target —
# Energized projects are out of cash-flow scope (their final milestone has
# already paid, by the time the project gets there).
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

# Pipeline-order ranks. Used for "has the trigger stage been entered yet?"
# comparisons. On Hold = 0 because it's a side branch, not a forward step.
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
    # ClimateFirst (loan): 50% at permit issue / 50% at WT/PTO entry
    "ClimateFirst":                           ("CF",   0.00, "Phase 1 (50%) — permit issued",    0.50),
    "CF - NTP":                               ("CF",   0.00, "Phase 1 (50%) — permit issued",    0.50),
    "CF - Phase 1 Submitted":                 ("CF",   0.00, "Phase 1 funded (50%)",             0.50),
    "CF - Phase 1 Funded":                    ("CF",   0.50, "Phase 2 (50%) — WT/PTO entry",     0.50),
    "CF - Phase 2 Submitted":                 ("CF",   0.50, "Phase 2 funded (50%)",             0.50),
    "CF - Phase 2 Funded":                    ("CF",   1.00, "—",                                0.00),
    # Smart-E (loan): 1/3 loan docs / 1/3 interconnection / 1/3 PTO
    "SE - Application Submitted":             ("SE",   0.00,   "1/3 — loan docs signed",           0.3333),
    "SE - Loan Closed 1/3 Payment Funded":    ("SE",   0.3333, "1/3 — interconnection approval",   0.3333),
    "SE- Final 1/3 Payment Funded":           ("SE",   0.6667, "1/3 — PTO (CHECK mapping)",        0.3333),
    "SE- PTO Package Submitted":              ("SE",   0.6667, "1/3 — PTO funded",                 0.3333),
}

LENDER_LAGS = {"LR": 14, "SG": 5, "CF": 5, "SE": 5, "Cash": 0}

# Heuristic constants.
PERMIT_TO_INSTALL_DAYS = 7        # Permit_Approved → est. install
CREATION_TO_INSTALL_DAYS = 45     # Project_Created → est. install
SC_TO_WT_PTO_DAYS = 28            # Substantial_Completion → est. WT/PTO entry
WT_PTO_TO_ENERGIZED_DAYS = 21     # WT/PTO entry → est. Energized entry
CASH_DEPOSIT_GRACE_DAYS = 7       # Project_Created → deposit-overdue threshold
SE_LOAN_DOCS_PROXY_DAYS = 14      # Project_Created → est. loan docs signed


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
    if s.startswith("Cash"): return "Cash (inferred)"
    if s.startswith("LR") or s == "LightReach": return "Lease / PPA (inferred)"
    if s.startswith("SG") or s == "Sungage": return "Solar Loan (inferred)"
    if s.startswith("CF") or s == "ClimateFirst": return "Solar Loan (inferred)"
    if s.startswith("SE"): return "Smart-E Loan (inferred)"
    return ""


# ---------------------------------------------------------------------------
# Trigger metadata per milestone
# ---------------------------------------------------------------------------
# Returns (trigger_event_label, trigger_stage_rank, awaiting_label_for_bucket).
# trigger_stage_rank is None for date-driven triggers (CF Phase 1 = Permit_Approved
# is set, SE interconnection = ICA_Contingent_Approval set) — those use a date
# field instead of a stage rank.

def _trigger_for(lender: str, next_desc: str) -> tuple[str, Optional[int], str]:
    nd = next_desc.lower()
    if lender == "Cash":
        if "deposit" in nd:        return ("signing",                 0,  "Awaiting deposit settlement")
        if "pre-install" in nd:    return ("install start",           9,  "Awaiting install start")
        if "substantial" in nd:    return ("substantial completion",  10, "Awaiting substantial completion")
    elif lender in ("LR", "SG"):
        if "install" in nd:        return ("substantial completion",  10, "Awaiting substantial completion")
        if "activation" in nd or "PTO" in next_desc:
            return ("Energized (PTO granted)", 12, "Awaiting PTO")
    elif lender == "CF":
        if "phase 1" in nd:        return ("permit approval",         None, "Awaiting permit approval")
        if "phase 2" in nd:        return ("WT/PTO submission",       11, "Awaiting WT/PTO submission")
    elif lender == "SE":
        if "loan docs" in nd:      return ("loan documents signed",   0,  "Awaiting loan docs")
        if "interconnection" in nd:return ("ICA approval",            None, "Awaiting ICA approval")
        if "PTO" in next_desc:     return ("PTO",                     12, "Awaiting PTO")
    return ("unmapped", None, "Awaiting [unmapped]")


def _resolve_install_anchor(subst, projected_install, permit_approved, project_created):
    """For install-driven forecasts, return (date, source_label) using the
    standard fallback chain. None if no signal at all."""
    if subst:
        return subst, "Substantial_Completion"
    if projected_install:
        return projected_install, "Projected_Install_Date"
    if permit_approved:
        return _add_days(permit_approved, PERMIT_TO_INSTALL_DAYS), \
               f"Permit_Approved + {PERMIT_TO_INSTALL_DAYS}d"
    if project_created:
        return _add_days(project_created, CREATION_TO_INSTALL_DAYS), \
               f"Project_Created_Date + {CREATION_TO_INSTALL_DAYS}d"
    return None, "no anchor"


def _resolve_wt_pto_entry(stage, date_of_stage_change, subst):
    """Best estimate of when the project entered (or will enter) Witness Test / PTO.
    Used both for Energized-trigger awaiting estimates and CF Phase 2 trigger."""
    if stage == "Witness Test / PTO" and date_of_stage_change:
        return date_of_stage_change, "Date_of_Stage_Change (current WT/PTO)"
    if subst:
        return _add_days(subst, SC_TO_WT_PTO_DAYS), \
               f"Substantial_Completion + {SC_TO_WT_PTO_DAYS}d"
    return None, "no SC anchor"


def _compute(lender: str, next_desc: str, next_pct: float, today: date,
             stage: str, lending_status: Optional[str],
             project_created: Optional[date],
             subst: Optional[date],
             projected_install: Optional[date],
             permit_approved: Optional[date],
             ica_approval: Optional[date],
             utility_pto: Optional[date],
             date_of_stage_change: Optional[date]) -> dict:
    """Returns a dict with: forecast_date, anchor_source, status (one of
    'on_track' | 'past_due' | 'awaiting' | 'paid'), awaiting_label."""
    if next_pct in (None, 0):
        return {"forecast_date": None, "anchor_source": "100% collected",
                "status": "paid", "awaiting_label": ""}

    trigger_label, trigger_rank, awaiting_label = _trigger_for(lender, next_desc)
    project_rank = STAGE_RANKS.get(stage, 0)
    lag = LENDER_LAGS.get(lender, 0)
    nd = next_desc.lower()

    # Decide whether the trigger has fired.
    if trigger_label == "permit approval":
        trigger_fired = permit_approved is not None
    elif trigger_label == "ICA approval":
        trigger_fired = ica_approval is not None
    elif trigger_label == "signing":
        trigger_fired = project_created is not None
    elif trigger_label == "loan documents signed":
        # Approximate: assume signed shortly after project creation.
        trigger_fired = project_created is not None
    elif trigger_rank is not None:
        trigger_fired = project_rank >= trigger_rank
    else:
        trigger_fired = False

    # Compute forecast date based on whether the trigger fired.
    if trigger_fired:
        if trigger_label == "signing":  # Cash deposit
            forecast = project_created
            anchor_src = "Project_Created_Date (signing)"
        elif trigger_label == "loan documents signed":  # SE 1/3 docs
            forecast = _add_days(project_created, SE_LOAN_DOCS_PROXY_DAYS)
            anchor_src = f"Project_Created_Date + {SE_LOAN_DOCS_PROXY_DAYS}d (proxy)"
        elif trigger_label == "permit approval":  # CF Phase 1
            forecast = _add_days(permit_approved, lag)
            anchor_src = f"Permit_Approved + {lag}d"
        elif trigger_label == "ICA approval":  # SE interconnection
            forecast = _add_days(ica_approval, lag)
            anchor_src = f"ICA_Contingent_Approval + {lag}d"
        elif trigger_label == "install start":  # Cash 60% — fires at AI entry
            anchor, src = _resolve_install_anchor(subst, projected_install, permit_approved, project_created)
            forecast = _add_days(anchor, -7) if anchor else None
            anchor_src = (src + " − 7d") if anchor else src
        elif trigger_label == "substantial completion":  # LR/SG install, Cash 20% subst
            anchor, src = _resolve_install_anchor(subst, projected_install, permit_approved, project_created)
            if "substantial" in nd:  # Cash 20% subst — pays at SC, no lag
                forecast = anchor
                anchor_src = src
            else:  # LR/SG 80%/90% — pays at SC + lender lag
                forecast = _add_days(anchor, lag) if anchor else None
                anchor_src = (src + f" + {lag}d") if anchor else src
        elif trigger_label == "WT/PTO submission":  # CF Phase 2
            entry, src = _resolve_wt_pto_entry(stage, date_of_stage_change, subst)
            forecast = _add_days(entry, lag) if entry else None
            anchor_src = (src + f" + {lag}d") if entry else src
        elif trigger_label == "Energized (PTO granted)" or trigger_label == "PTO":
            # Project at Energized — but Energized isn't in ACTIVE_STAGES so this
            # is rare in practice. Fall back to Date_of_Stage_Change + lag.
            if stage == "Energized" and date_of_stage_change:
                forecast = _add_days(date_of_stage_change, lag)
                anchor_src = f"Energized entry + {lag}d"
            else:
                # Project should be at Energized but isn't — defensive fallback.
                forecast = None
                anchor_src = "no Energized anchor"
        else:
            forecast = None
            anchor_src = "unmapped trigger"

        # Past-due decision.
        if forecast and forecast < today:
            # Cash deposit gets 7-day grace from signing.
            if trigger_label == "signing":
                threshold = _add_days(forecast, CASH_DEPOSIT_GRACE_DAYS)
                if today > threshold:
                    return {"forecast_date": forecast.isoformat(), "anchor_source": anchor_src,
                            "status": "past_due", "awaiting_label": ""}
                else:
                    return {"forecast_date": forecast.isoformat(), "anchor_source": anchor_src,
                            "status": "on_track", "awaiting_label": ""}
            else:
                return {"forecast_date": forecast.isoformat(), "anchor_source": anchor_src,
                        "status": "past_due", "awaiting_label": ""}
        return {"forecast_date": forecast.isoformat() if forecast else None,
                "anchor_source": anchor_src,
                "status": "on_track" if forecast else "awaiting",
                "awaiting_label": "" if forecast else awaiting_label}

    # ---- Trigger has NOT fired: build an estimate ----
    if trigger_label == "Energized (PTO granted)":
        # Project at WT/PTO (or earlier) awaiting Energized.
        # Forecast = WT/PTO entry + 21d + lender lag.
        wt_entry, src = _resolve_wt_pto_entry(stage, date_of_stage_change, subst)
        if not wt_entry:
            # Pre-WT/PTO and no SC yet: estimate via install anchor + 28d + 21d + lag
            anchor, anchor_src = _resolve_install_anchor(subst, projected_install, permit_approved, project_created)
            if anchor:
                est = _add_days(anchor, SC_TO_WT_PTO_DAYS + WT_PTO_TO_ENERGIZED_DAYS + lag)
                src = f"{anchor_src} + {SC_TO_WT_PTO_DAYS}d (SC→WT/PTO) + {WT_PTO_TO_ENERGIZED_DAYS}d (WT/PTO→Energized) + {lag}d"
            else:
                est = None
        else:
            est = _add_days(wt_entry, WT_PTO_TO_ENERGIZED_DAYS + lag)
            src = f"{src} + {WT_PTO_TO_ENERGIZED_DAYS}d + {lag}d"
        if est and est >= today:
            return {"forecast_date": est.isoformat(), "anchor_source": src,
                    "status": "on_track", "awaiting_label": ""}
        return {"forecast_date": est.isoformat() if est else None,
                "anchor_source": src,
                "status": "awaiting", "awaiting_label": awaiting_label}

    if trigger_label in ("substantial completion", "install start", "WT/PTO submission"):
        anchor, src = _resolve_install_anchor(subst, projected_install, permit_approved, project_created)
        if not anchor:
            return {"forecast_date": None, "anchor_source": src,
                    "status": "awaiting", "awaiting_label": awaiting_label}
        if trigger_label == "WT/PTO submission":  # CF Phase 2 — anchor + 28d + lag
            est = _add_days(anchor, SC_TO_WT_PTO_DAYS + lag)
            src = f"{src} + {SC_TO_WT_PTO_DAYS}d (SC→WT/PTO) + {lag}d"
        elif trigger_label == "install start":
            est = _add_days(anchor, -7)
            src = f"{src} − 7d"
        elif "substantial" in nd:  # Cash 20% subst comp
            est = anchor
        else:  # LR/SG install
            est = _add_days(anchor, lag)
            src = f"{src} + {lag}d"
        if est and est >= today:
            return {"forecast_date": est.isoformat(), "anchor_source": src,
                    "status": "on_track", "awaiting_label": ""}
        return {"forecast_date": est.isoformat() if est else None,
                "anchor_source": src,
                "status": "awaiting", "awaiting_label": awaiting_label}

    if trigger_label == "permit approval":
        # CF Phase 1 — no permit yet. Use install-anchor heuristic for permit timing
        # (the Permit_Approved field gets set when permit lands; in the meantime
        # we don't have a great signal). Estimate = Project_Created + 30d.
        if project_created:
            est = _add_days(project_created, 30)  # rough permit-from-creation
            est_with_lag = _add_days(est, lag)
            if est_with_lag >= today:
                return {"forecast_date": est_with_lag.isoformat(),
                        "anchor_source": f"Project_Created + 30d + {lag}d (rough permit estimate)",
                        "status": "on_track", "awaiting_label": ""}
            return {"forecast_date": est_with_lag.isoformat(),
                    "anchor_source": f"Project_Created + 30d + {lag}d (estimate elapsed)",
                    "status": "awaiting", "awaiting_label": awaiting_label}
        return {"forecast_date": None, "anchor_source": "no creation date",
                "status": "awaiting", "awaiting_label": awaiting_label}

    if trigger_label == "ICA approval":
        if project_created:
            est = _add_days(project_created, 60)
            return {"forecast_date": est.isoformat(),
                    "anchor_source": f"Project_Created + 60d + {lag}d (rough ICA estimate)",
                    "status": "on_track" if est >= today else "awaiting",
                    "awaiting_label": "" if est >= today else awaiting_label}
        return {"forecast_date": None, "anchor_source": "no signals",
                "status": "awaiting", "awaiting_label": awaiting_label}

    return {"forecast_date": None, "anchor_source": "unmapped",
            "status": "awaiting", "awaiting_label": awaiting_label}


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
            project_created, subst, projected_install, permit_approved,
            ica_approval, utility_pto, date_of_stage_change,
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
            "status": forecast["status"],            # paid | on_track | past_due | awaiting
            "awaiting_label": forecast["awaiting_label"],
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
    """Place a project into a display bucket based on its status + forecast."""
    status = p.get("status")
    if status == "paid":
        return "Paid"  # excluded from chart but visible in summary
    if status == "past_due":
        return "Past-due"
    if status == "awaiting":
        return p.get("awaiting_label") or "Awaiting"
    # on_track: place in the appropriate week
    fd = _parse_date(p.get("forecast_date"))
    if not fd:
        return "Awaiting"
    first_start = _parse_date(weeks[0]["start"])
    last_end = _parse_date(weeks[-1]["end"])
    if fd < first_start:
        # Should not happen for on_track, but defensive — treat as past-due.
        return "Past-due"
    if fd > last_end:
        return "Beyond 12 wks"
    for w in weeks:
        if _parse_date(w["start"]) <= fd <= _parse_date(w["end"]):
            return w["label"]
    return "Awaiting"


def compute_cashflow(raw_rows: list[dict], now: datetime) -> dict:
    today = now.astimezone(timezone.utc).date()
    projects = build_projects(raw_rows, today)
    weeks = _build_weeks(today, n_weeks=12)

    weekly_totals: dict[str, float] = {w["label"]: 0.0 for w in weeks}
    weekly_totals["Past-due"] = 0.0
    weekly_totals["Beyond 12 wks"] = 0.0
    awaiting_totals: dict[str, float] = {}

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
        if bucket.startswith("Awaiting"):
            awaiting_totals[bucket] = awaiting_totals.get(bucket, 0) + amt
        elif bucket in weekly_totals:
            weekly_totals[bucket] += amt
        total_outstanding += p["dollars_outstanding"] or 0
        total_collected += p["dollars_collected"] or 0
        if p["forecast_date"]:
            fd = _parse_date(p["forecast_date"])
            if fd and first_week_start <= fd <= first_week_end and p["status"] == "on_track":
                this_week_total += amt
            if fd and today <= fd <= cutoff_30 and p["status"] == "on_track":
                next_30_days += amt

    return {
        "generated_at": now.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "today": today.isoformat(),
        "weeks": weeks,
        "weekly_totals": {k: round(v, 2) for k, v in weekly_totals.items()},
        "awaiting_totals": {k: round(v, 2) for k, v in awaiting_totals.items()},
        "summary": {
            "project_count": len(projects),
            "total_outstanding": round(total_outstanding, 2),
            "total_collected": round(total_collected, 2),
            "this_week": round(this_week_total, 2),
            "next_30_days": round(next_30_days, 2),
            "past_due": round(weekly_totals.get("Past-due", 0.0), 2),
            "awaiting": round(sum(awaiting_totals.values()), 2),
        },
        "projects": projects,
    }


def apply_filter(payload: dict, view_filter: dict) -> dict:
    owners = set(view_filter.get("owners") or [])
    reps = set(view_filter.get("reps") or [])
    out_projects = []
    for p in payload["projects"]:
        if owners and p.get("owner") not in owners:
            continue
        if reps and p.get("rep") not in reps:
            continue
        out_projects.append(p)

    today = _parse_date(payload["today"])
    weeks = payload["weeks"]
    weekly_totals = {w["label"]: 0.0 for w in weeks}
    weekly_totals["Past-due"] = 0.0
    weekly_totals["Beyond 12 wks"] = 0.0
    awaiting_totals: dict[str, float] = {}

    total_outstanding = 0.0
    total_collected = 0.0
    this_week_total = 0.0
    next_30_days = 0.0
    cutoff_30 = today + timedelta(days=30)
    first_week_start = _parse_date(weeks[0]["start"])
    first_week_end = _parse_date(weeks[0]["end"])

    for p in out_projects:
        bucket = p.get("bucket") or "Awaiting"
        amt = p.get("next_dollar") or 0
        if bucket.startswith("Awaiting"):
            awaiting_totals[bucket] = awaiting_totals.get(bucket, 0) + amt
        elif bucket in weekly_totals:
            weekly_totals[bucket] += amt
        total_outstanding += p.get("dollars_outstanding", 0) or 0
        total_collected += p.get("dollars_collected", 0) or 0
        if p.get("forecast_date"):
            fd = _parse_date(p["forecast_date"])
            if fd and first_week_start <= fd <= first_week_end and p.get("status") == "on_track":
                this_week_total += amt
            if fd and today <= fd <= cutoff_30 and p.get("status") == "on_track":
                next_30_days += amt

    return {
        "generated_at": payload["generated_at"],
        "today": payload["today"],
        "weeks": weeks,
        "weekly_totals": {k: round(v, 2) for k, v in weekly_totals.items()},
        "awaiting_totals": {k: round(v, 2) for k, v in awaiting_totals.items()},
        "summary": {
            "project_count": len(out_projects),
            "total_outstanding": round(total_outstanding, 2),
            "total_collected": round(total_collected, 2),
            "this_week": round(this_week_total, 2),
            "next_30_days": round(next_30_days, 2),
            "past_due": round(weekly_totals.get("Past-due", 0.0), 2),
            "awaiting": round(sum(awaiting_totals.values()), 2),
        },
        "projects": out_projects,
    }
