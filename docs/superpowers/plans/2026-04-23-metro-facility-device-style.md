# Metro & Facility Device-Style Redesign — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the `dl`-card info sections on Metro and Facility detail pages with stat pills + split Used/Available capacity cards, matching the Device page's visual style.

**Architecture:** Two self-contained JSX block replacements — one per page. Each removes the `grid grid-cols-3` bordered-card section and replaces it with a CSS grid of stat pills (2 rows × 4 cols + map column) followed by three split capacity cards. No new components, no backend changes.

**Tech Stack:** React, TypeScript, Tailwind CSS v4, existing `MiniMap` and `CopyableText` components.

---

### Task 1: Facility detail page — replace info grid

**Files:**
- Modify: `web/src/components/facility-detail-page.tsx:198-324`

- [ ] **Step 1: Replace the info grid block**

In `web/src/components/facility-detail-page.tsx`, find and replace the entire `{/* Info grid */}` block (lines 198–324 — from `{/* Info grid */}` through the closing `</div>` of the outer grid) with the following:

```tsx
        {/* Info grid */}
        <div
          className="grid gap-1.5 mb-6"
          style={{ gridTemplateColumns: '1fr 1fr 1fr 1fr 1.8fr', gridTemplateRows: 'auto auto' }}
        >
          {/* Row 1 */}
          <div className="bg-muted/30 rounded-lg px-2 py-2.5 text-center">
            <div className="text-sm font-medium">{facility.country || '—'}</div>
            <div className="text-xs text-muted-foreground">Country</div>
          </div>
          <div className="bg-muted/30 rounded-lg px-2 py-2.5 text-center">
            <div className="text-sm font-medium font-mono">
              {facility.metro_pk
                ? <Link to={`/dz/metros/${facility.metro_pk}`} className="text-foreground/85 hover:text-foreground hover:underline">{facility.metro_code}</Link>
                : (facility.metro_code || '—')}
            </div>
            <div className="text-xs text-muted-foreground">Metro</div>
          </div>
          <div className="bg-muted/30 rounded-lg px-2 py-2.5 text-center">
            <div className="text-sm font-medium tabular-nums">
              {facility.user_count}
              {facility.max_users > 0 && <span className="text-muted-foreground">/{facility.max_users}</span>}
            </div>
            <div className="text-xs text-muted-foreground">Users</div>
          </div>
          <div className="bg-muted/30 rounded-lg px-2 py-2.5 text-center">
            <div className="text-sm font-medium">{facility.device_count}</div>
            <div className="text-xs text-muted-foreground">Devices</div>
          </div>
          {/* Map — spans both rows */}
          {facility.lat !== 0 && facility.lng !== 0 ? (
            <div className="rounded-lg overflow-hidden border border-border" style={{ gridRow: 'span 2' }}>
              <MiniMap
                lat={facility.lat}
                lng={facility.lng}
                googleMapsHref={`https://www.google.com/maps?q=${facility.lat},${facility.lng}`}
              />
            </div>
          ) : (
            <div style={{ gridRow: 'span 2' }} />
          )}
          {/* Row 2 */}
          <div className="bg-muted/30 rounded-lg px-2 py-2.5 text-center" style={{ gridColumn: 'span 2' }}>
            <div className="text-sm font-medium font-mono">
              <CopyableText text={facility.pk} className="font-mono text-sm">
                {facility.pk.slice(0, 4)}...{facility.pk.slice(-4)}
              </CopyableText>
            </div>
            <div className="text-xs text-muted-foreground">Pubkey</div>
          </div>
          <div className="bg-muted/30 rounded-lg px-2 py-2.5 text-center">
            <div className="text-sm font-medium font-mono">
              <CopyableText text={facility.code} className="font-mono text-sm" />
            </div>
            <div className="text-xs text-muted-foreground">Code</div>
          </div>
          {facility.loc_id > 0 ? (
            <div className="bg-muted/30 rounded-lg px-2 py-2.5 text-center">
              <div className="text-sm font-medium font-mono">
                <a
                  href={`https://www.peeringdb.com/fac/${facility.loc_id}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-foreground/85 hover:text-foreground hover:underline"
                >
                  {facility.loc_id}
                </a>
              </div>
              <div className="text-xs text-muted-foreground">PeeringDB</div>
            </div>
          ) : (
            <div />
          )}
        </div>

        {/* Capacity cards */}
        {(() => {
          const effUnicastMax = rawDevices.length > 0
            ? rawDevices.reduce((sum, d) => {
                const rem = d.max_users > 0 ? Math.max(0, d.max_users - d.current_users) : 0
                return sum + Math.max(d.unicast_users, d.max_unicast_users > 0 ? d.max_unicast_users : d.unicast_users + rem)
              }, 0)
            : (facility.max_unicast_users > 0 ? facility.max_unicast_users : facility.max_users)
          const effSubsMax = rawDevices.length > 0
            ? rawDevices.reduce((sum, d) => {
                const rem = d.max_users > 0 ? Math.max(0, d.max_users - d.current_users) : 0
                return sum + Math.max(d.multicast_subscribers_count, d.max_multicast_subscribers > 0 ? d.max_multicast_subscribers : d.multicast_subscribers_count + rem)
              }, 0)
            : (facility.max_multicast_subscribers > 0 ? facility.max_multicast_subscribers : facility.max_users)
          const effPubsMax = rawDevices.length > 0
            ? rawDevices.reduce((sum, d) => {
                const rem = d.max_users > 0 ? Math.max(0, d.max_users - d.current_users) : 0
                return sum + Math.max(d.multicast_publishers_count, d.max_multicast_publishers > 0 ? d.max_multicast_publishers : d.multicast_publishers_count + rem)
              }, 0)
            : (facility.max_multicast_publishers > 0 ? facility.max_multicast_publishers : facility.max_users)
          const hasCapacity = effUnicastMax > 0 || effSubsMax > 0 || effPubsMax > 0
            || facility.unicast_users_count > 0 || facility.multicast_subscribers_count > 0 || facility.multicast_publishers_count > 0
          if (!hasCapacity) return null
          return (
            <div className="grid grid-cols-3 gap-1.5 mb-6">
              {[
                { label: 'Unicast', used: facility.unicast_users_count, max: effUnicastMax },
                { label: 'Subscribers', used: facility.multicast_subscribers_count, max: effSubsMax },
                { label: 'Publishers', used: facility.multicast_publishers_count, max: effPubsMax },
              ].map(({ label, used, max }) => {
                const available = max > used ? max - used : 0
                const pct = max > 0 ? Math.min(100, (used / max) * 100) : 0
                const fillColor = pct >= 90 ? 'bg-red-500/50' : pct >= 70 ? 'bg-amber-500/40' : 'bg-blue-500/30'
                return (
                  <div key={label} className="border border-border rounded-lg overflow-hidden">
                    <div className="grid grid-cols-2 divide-x divide-border">
                      <div className="p-2.5 text-center bg-muted/30">
                        <div className="text-sm font-medium tabular-nums">{used}</div>
                        <div className="text-xs text-muted-foreground">{label}</div>
                      </div>
                      <div className="p-2.5 text-center bg-muted/10">
                        <div className="text-sm font-medium tabular-nums text-muted-foreground">{available}</div>
                        <div className="text-xs text-muted-foreground/60">Available</div>
                      </div>
                    </div>
                    {pct > 0 && (
                      <div className="relative h-[3px] bg-muted/60">
                        <div className={`absolute inset-y-0 left-0 ${fillColor}`} style={{ width: `${pct}%` }} />
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          )
        })()}
```

- [ ] **Step 2: Verify TypeScript**

```bash
cd web && bun run tsc -b
```

Expected: no output (clean).

- [ ] **Step 3: Commit**

```bash
git add web/src/components/facility-detail-page.tsx
git commit -m "web: facility detail — device-style stat pills and capacity cards"
```

---

### Task 2: Metro detail page — replace info grid

**Files:**
- Modify: `web/src/components/metro-detail-page.tsx:1` (import line) and `:201-295` (info grid)

- [ ] **Step 1: Remove `Info` from the lucide-react import**

In `web/src/components/metro-detail-page.tsx` line 3, change:

```tsx
import { Loader2, MapPin, AlertCircle, ArrowLeft, Info, ChevronUp, ChevronDown } from 'lucide-react'
```

to:

```tsx
import { Loader2, MapPin, AlertCircle, ArrowLeft, ChevronUp, ChevronDown } from 'lucide-react'
```

- [ ] **Step 2: Replace the info grid block**

Find and replace the entire `{/* Info grid */}` block (lines 201–295 — from `{/* Info grid */}` through the closing `</div>`) with the following:

```tsx
        {/* Info grid */}
        <div
          className="grid gap-1.5 mb-6"
          style={{ gridTemplateColumns: '1fr 1fr 1fr 1fr 1.8fr', gridTemplateRows: 'auto auto' }}
        >
          {/* Row 1 */}
          <div className="bg-muted/30 rounded-lg px-2 py-2.5 text-center">
            <div className="text-sm font-medium">{metro.device_count}</div>
            <div className="text-xs text-muted-foreground">Devices</div>
          </div>
          <div className="bg-muted/30 rounded-lg px-2 py-2.5 text-center">
            <div className="text-sm font-medium">{metro.facility_count}</div>
            <div className="text-xs text-muted-foreground">Facilities</div>
          </div>
          <div className="bg-muted/30 rounded-lg px-2 py-2.5 text-center">
            <div className="text-sm font-medium tabular-nums">
              {metro.user_count}
              {metro.max_users > 0 && <span className="text-muted-foreground">/{metro.max_users}</span>}
            </div>
            <div className="text-xs text-muted-foreground">Users</div>
          </div>
          <div className="bg-muted/30 rounded-lg px-2 py-2.5 text-center">
            <div className="text-sm font-medium">{metro.validator_count}</div>
            <div className="text-xs text-muted-foreground">Validators</div>
          </div>
          {/* Map — spans both rows */}
          <div className="rounded-lg overflow-hidden border border-border" style={{ gridRow: 'span 2' }}>
            <MiniMap
              lat={metro.latitude}
              lng={metro.longitude}
              googleMapsHref={`https://www.google.com/maps?q=${metro.latitude},${metro.longitude}`}
            />
          </div>
          {/* Row 2 */}
          <div className="bg-muted/30 rounded-lg px-2 py-2.5 text-center">
            <div className="text-sm font-medium">{formatBps(metro.in_bps)}</div>
            <div className="text-xs text-muted-foreground">Inbound</div>
          </div>
          <div className="bg-muted/30 rounded-lg px-2 py-2.5 text-center">
            <div className="text-sm font-medium">{formatBps(metro.out_bps)}</div>
            <div className="text-xs text-muted-foreground">Outbound</div>
          </div>
          <div className="bg-muted/30 rounded-lg px-2 py-2.5 text-center" style={{ gridColumn: 'span 2' }}>
            <div className="text-sm font-medium">{formatStake(metro.stake_sol)}</div>
            <div className="text-xs text-muted-foreground">Total Stake</div>
          </div>
        </div>

        {/* Capacity cards */}
        {(() => {
          const effUnicast = Math.max(metro.unicast_users_count, metro.max_unicast_users)
          const effSubs = Math.max(metro.multicast_subscribers_count, metro.max_multicast_subscribers)
          const effPubs = Math.max(metro.multicast_publishers_count, metro.max_multicast_publishers)
          const hasCapacity = effUnicast > 0 || effSubs > 0 || effPubs > 0
          if (!hasCapacity) return null
          return (
            <div className="grid grid-cols-3 gap-1.5 mb-10">
              {[
                { label: 'Unicast', used: metro.unicast_users_count, max: effUnicast },
                { label: 'Subscribers', used: metro.multicast_subscribers_count, max: effSubs },
                { label: 'Publishers', used: metro.multicast_publishers_count, max: effPubs },
              ].map(({ label, used, max }) => {
                const available = max > used ? max - used : 0
                const pct = max > 0 ? Math.min(100, (used / max) * 100) : 0
                const fillColor = pct >= 90 ? 'bg-red-500/50' : pct >= 70 ? 'bg-amber-500/40' : 'bg-blue-500/30'
                return (
                  <div key={label} className="border border-border rounded-lg overflow-hidden">
                    <div className="grid grid-cols-2 divide-x divide-border">
                      <div className="p-2.5 text-center bg-muted/30">
                        <div className="text-sm font-medium tabular-nums">{used}</div>
                        <div className="text-xs text-muted-foreground">{label}</div>
                      </div>
                      <div className="p-2.5 text-center bg-muted/10">
                        <div className="text-sm font-medium tabular-nums text-muted-foreground">{available}</div>
                        <div className="text-xs text-muted-foreground/60">Available</div>
                      </div>
                    </div>
                    {pct > 0 && (
                      <div className="relative h-[3px] bg-muted/60">
                        <div className={`absolute inset-y-0 left-0 ${fillColor}`} style={{ width: `${pct}%` }} />
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          )
        })()}
```

- [ ] **Step 3: Verify TypeScript**

```bash
cd web && bun run tsc -b
```

Expected: no output (clean).

- [ ] **Step 4: Commit**

```bash
git add web/src/components/metro-detail-page.tsx
git commit -m "web: metro detail — device-style stat pills and capacity cards"
```
