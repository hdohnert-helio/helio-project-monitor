# LightReach Submission Checklist — System Guide

A guide to the submission-checklist tool and its LightReach/Zoho integration:
what each piece does, where it lives, and how data flows between them.

---

## 1. The checklist tool

**Live at:** https://hdohnert-helio.github.io/helio-project-monitor/checklist.html
**Source:** `checklist.html` in this repo (`helio-project-monitor`), served via GitHub Pages.

A single static HTML/JS page (no backend, no build step) covering the full
M0 (NTP) / M1 (Installation) / M2 (Activation) submission checklist — 51
items across NTP, Site Qualifying, Administrative, Design, Install Photos,
Deliverables, Monitoring, and System Settings categories.

### Zoho project lookup
Typing a customer name or project ID into "Project / homeowner" searches
`data.json` (fetched live from this same GitHub Pages site) and shows
matches with stage/rep. Selecting one auto-fills:
- Project name + Account/ID
- **Stage badge** (green/yellow/red) using the same SLA thresholds as the
  pipeline dashboard (`sla_thresholds.json`)
- **Open in Zoho** link, straight to the CRM record
- **LightReach reports** panel (see §3) — LightReach's own approve/reject
  history for that project, if any events have landed

### Per-item tracking
Each of the 51 items has three states instead of a plain checkbox:
- **Done** (green check)
- **Rejected** (red ×, reveals a reason textarea)
- **N/A** (greyed out, excluded from the completion count)

A "Needs fix" filter jumps straight to rejected items.

### Autosave + switching projects
Progress saves automatically to the browser (`localStorage`), keyed by
Account/ID (or project name if no ID). Reloading the page resumes the last
active project. The **Saved projects** button lists everything saved so
ops can switch between projects without losing work. **Reset for next
project** clears the current view but keeps the finished project's data
in the list.

**Caveat:** all of this is local to one browser/device. Two people working
on the same project in different browsers won't see each other's progress
— there's no shared backend for checklist state (only for the LightReach
data feeding into it, see below).

---

## 2. Data pipeline: Zoho → data.json → checklist

```
Zoho CRM (Installs module)
        │  scripts/refresh_data.py  (runs via GitHub Action, 2x/day: 11:00 & 18:00 UTC)
        ▼
data.json  (committed to this repo, served by GitHub Pages)
        │  fetched client-side by checklist.html
        ▼
Checklist's Zoho lookup + LightReach reports panel
```

- **`scripts/refresh_data.py`** — pulls a fixed field list (`FIELDS` constant)
  from the Zoho `Installs` module and writes `data.json`. Also used by the
  pipeline dashboard (`index.html`) and cash flow tooling.
- **`sla_thresholds.json`** — green/yellow/red day-count thresholds per
  stage; same file the pipeline dashboard uses for consistency.
- The GitHub Action (`.github/workflows/`) commits refreshed data twice
  daily; the checklist always reads the latest committed version.

### Relevant `data.json` project fields
| Field | Source |
|---|---|
| `project_id`, `customer`, `stage`, `days_in_stage`, `rep`, `project_manager`, `zoho_record_id` | Core Zoho Install fields |
| `lightreach_finance_status` | `LightReach_Finance_Status` (Zoho) |
| `lightreach_ntp_granted_at` | `LightReach_NTP_Granted_At` (Zoho) |
| `lightreach_outstanding_stipulations` | `LightReach_Outstanding_Stipulations` (Zoho) |
| `lightreach_requirement_log` | `LightReach_RequirementLog` (Zoho, parsed from JSON) |
| `lightreach_milestone_log` | `LightReach_MilestoneLog` (Zoho, parsed from JSON) |

---

## 3. LightReach (Palmetto) webhook integration

```
LightReach/Palmetto  ──POST──▶  aurora-zoho-sync.onrender.com/webhook/lightreach
                                         │
                                         ▼
                          Matches event to a Zoho Install record
                          (by LightReach_Account_ID → email → LightReach_Quote_ID)
                                         │
                                         ▼
                     Writes/append-logs onto that Install record
```

**Receiver:** `/webhook/lightreach` in `main.py`, repo `aurora-zoho-sync`
(separate repo — same Render service that runs Zoho sync/commissions).
Auth: validates a Palmetto-issued `apiKey` header against
`LIGHTREACH_API_KEY` env var.

### Currently subscribed events (LightReach webhook admin → "Helio Zoho Sync")
| Event | What it does in our handler |
|---|---|
| `contractSigned` | Sets `LightReach_Contract_Status`, `LightReach_Contract_Signed_At` |
| `applicationStatus` | Sets `LightReach_Finance_Status` to the application status (approved/approvedWithStipulations/creditFrozen/declined/expired) |
| `stipulationAdded` | Sets `LightReach_Stipulation_Action_Needed=true`, `LightReach_Outstanding_Stipulations` |
| `stipulationCleared` | Logs only (no field write) |
| `allStipulationsCleared` | Clears the two stipulation fields above |
| `requirementCompleted`, `requirementStatusChanged` | Appends to `LightReach_RequirementLog` (see payload shape below) |
| `milestoneAchieved` | Appends to `LightReach_MilestoneLog`; also sets `LightReach_NTP_Granted_At` when the milestone is NTP |
| `milestoneStatusChanged` **(new)** | Appends to `LightReach_MilestoneLog` — this is the real M0/M1/M2 approve/reject signal |
| `allConsumerTaskEvents` **(new)** | For `activityType=="requirement"` entries only, appends to `LightReach_RequirementLog` — the only event with an explicit rejection reason |
| `quoteCreated` | Received but not specially handled beyond the raw payload log |

Every event also always writes `LightReach_Last_Updated` (timestamp) and
`LightReach_Raw_Payload` (full JSON body) — so nothing is ever silently
lost even if a specific field extraction turns out wrong.

### Real payload shapes (per docs.palmetto.com/finance/webhooks/)
LightReach has **no dedicated "rejected" event** — rejection shows up as a
status value inside these events:

```jsonc
// requirementStatusChanged — can report multiple requirements in one call
{
  "accountId": "...",
  "event": "requirementStatusChanged",
  "requirementStatusUpdates": [
    { "requirementType": "shadeReport", "newStatus": "rejected", "previousStatus": "submitted" }
  ]
}
// newStatus/previousStatus: completed | error | inProgress | pending | rejected | resubmitted | submitted
// No reason field.

// milestoneStatusChanged — the real M0/M1/M2 approve/reject signal
{
  "accountId": "...",
  "event": "milestoneStatusChanged",
  "milestoneType": "install",
  "newStatus": "rejected",
  "previousStatus": "submitted"
}
// newStatus/previousStatus: approved | conditionallyApproved | paused | pending | rejected | restarted | resubmitted | submitted
// No reason field.

// milestoneAchieved — means the milestone was *reached*, not that it passed review
{ "accountId": "...", "event": "milestoneAchieved", "newMilestone": "noticeToProceed" }
// newMilestone: noticeToProceed (M0) | install (M1) | activation (M2)

// allConsumerTaskEvents — the ONLY event with an explicit rejection reason
{
  "accountId": "...", "event": "allConsumerTaskEvents",
  "name": "Shade Report Upload", "activityType": "requirement",
  "status": "rejected", "rejectionReasons": ["Blurry image", "Wrong angle"]
}
```

### New Zoho fields (created via API this session, module: `Installs`)
- **`LightReach_RequirementLog`** — large text (32,000 char), JSON array of
  `{event, code, name, status, previous_status, reason, at}` entries, oldest
  dropped first if it would exceed ~31,000 chars.
- **`LightReach_MilestoneLog`** — same shape, keyed by `milestone` instead
  of `code`/`name`.

### Known gap / next verification step
`milestoneStatusChanged` and `allConsumerTaskEvents` were just subscribed —
connectivity and auth are confirmed working (Palmetto's "Test" button
returns 200 OK), but that test payload is a generic connectivity probe,
**not** a realistic sample of either event's real shape. We're waiting for
a genuine event to land to confirm the parsing logic end-to-end. Check via:
```sql
select id, Name, Project_ID, LightReach_RequirementLog, LightReach_MilestoneLog, LightReach_Last_Updated
from Installs
where LightReach_Finance_Status in ('milestoneStatusChanged', 'allConsumerTaskEvents')
order by LightReach_Last_Updated desc
```
(COQL doesn't support `is not null` on textarea fields, so filter on
`LightReach_Finance_Status` or `LightReach_Last_Updated` instead.)

---

## 4. Quick reference

| What | Where |
|---|---|
| Checklist (live) | https://hdohnert-helio.github.io/helio-project-monitor/checklist.html |
| Checklist source | `checklist.html` (this repo) |
| Pipeline dashboard | https://hdohnert-helio.github.io/helio-project-monitor/ |
| Data feed | https://hdohnert-helio.github.io/helio-project-monitor/data.json |
| SLA thresholds | https://hdohnert-helio.github.io/helio-project-monitor/sla_thresholds.json |
| Data refresh script | `scripts/refresh_data.py` (this repo) |
| Refresh schedule | GitHub Action, 11:00 & 18:00 UTC daily |
| Webhook receiver | `aurora-zoho-sync` repo, `main.py`, `/webhook/lightreach` |
| Webhook service (live) | https://aurora-zoho-sync.onrender.com |
| LightReach webhook docs | https://docs.palmetto.com/finance/webhooks/ |
| Zoho module | `Installs` (custom module, CRM) |
