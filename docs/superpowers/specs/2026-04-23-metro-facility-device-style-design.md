# Metro & Facility Detail Pages — Device-Style Redesign

**Date:** 2026-04-23  
**Status:** Approved

## Overview

Redesign the Metro and Facility detail pages to match the visual language of the Device detail page. The Device page uses stat pills and split Used/Available capacity cards instead of traditional bordered `dl` cards. The goal is a consistent look across all three entity detail pages.

## What Changes

### Both pages: remove the 3-column `dl` card layout

The current layout has three bordered `bg-card` cards (Infrastructure, Details/Traffic, Map) each containing a `<dl>` key-value list. This entire section is replaced.

### New layout: stat pills grid + map column + capacity cards

**Stat pills grid (2 rows × 4 cols + map spanning both rows):**

```
[ pill ][ pill ][ pill ][ pill ][ map         ]
[ pill ][ pill ][ pill ][ pill ][ (spans rows) ]
```

Grid: `grid-template-columns: 1fr 1fr 1fr 1fr 1.8fr`, `grid-template-rows: auto auto`, `gap-1.5`.

- Each pill: `bg-muted/30 rounded-lg px-2 py-2.5 text-center` — value on top (`text-sm font-medium`), label below (`text-xs text-muted-foreground`). Matches Device page's stat grid style.
- Map column (`grid-row: span 2`): `MiniMap` component with `rounded-lg overflow-hidden border border-border`. Same dark look as current mini-maps. Only rendered when `lat/lng !== 0`. If no coordinates, the 4th cell of row 2 fills the last column instead.

**Capacity cards (3 cards, full width, below the grid):**

```
[ Unicast  Used | Available ][ Subscribers Used | Available ][ Publishers Used | Available ]
```

`grid-cols-3 gap-1.5`. Same split card as Device page (`DeviceInfoContent`):
- `border border-border rounded-lg overflow-hidden`
- Left half: used count, `bg-muted/30`; right half: available count, `bg-muted/10` dimmed
- 3px progress bar at bottom — blue < 70%, amber 70–89%, red ≥ 90%
- Only rendered when at least one of the three capacity values > 0

---

### Facility pills

| Row | Pills (left→right) |
|-----|-------------------|
| 1 | Country, Metro (link), Users `count/max`, Devices |
| 2 | Pubkey `pk[0:4]…pk[-4]` (CopyableText, spans 2 cols), Code (CopyableText), PeeringDB ID (link, only if `loc_id > 0`) |

Notes:
- "Pubkey" displays `facility.pk` abbreviated — same as existing Details card
- PeeringDB pill omitted (cell left empty / map fills) if `loc_id === 0`
- Metro pill is a `Link` to `/dz/metros/:metro_pk` when `metro_pk` is set

Capacity values: use the effective-max computation already in the component (sum of device `effUnicast`/`effSubs`/`effPubs` from `rawDevices`).

---

### Metro pills

| Row | Pills (left→right) |
|-----|-------------------|
| 1 | Devices, Facilities, Users `count/max`, Validators |
| 2 | Inbound (`formatBps`), Outbound (`formatBps`), Stake (`formatStake`), _(empty — map fills)_ |

Notes:
- `MetroDetail` has no `stake_share` field — omit that pill
- If `metro.stake_sol === 0`, Stake pill shows `—`
- Row 2 has only 3 pills; the 4th slot in the grid aligns with the map column naturally (map spans both rows, so no orphan cell)

Capacity values: use the existing `effUnicast = Math.max(metro.unicast_users_count, metro.max_unicast_users)` etc., same as current code.

---

## What Does NOT Change

- Page header (icon + title + status pill + back button)
- Devices table and Facilities table (Metro page)
- Loading / error states
- All sorting logic
- API calls and data fetching

## Files to Edit

1. `web/src/components/facility-detail-page.tsx` — replace info grid section (lines ~198–308)
2. `web/src/components/metro-detail-page.tsx` — replace info grid section (lines ~201–295)

No backend changes. No new components — reuses `MiniMap` and `CopyableText` already imported.
