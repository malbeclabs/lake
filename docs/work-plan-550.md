# Work Plan: Issue #550 — Geolocation Explorer DZDP Concentration View

## Requirements

1. **View switcher** in `geoloc-explorer-page.tsx` with tabs: QA Explorer / DZDP Concentration / DZDP Validators
2. **URL query param sync** — `?view=concentration` etc., default to explorer
3. **DZDP Concentration view** (`dzdp-concentration-view.tsx`):
   - Hero stat row (4 metric cards with warning states)
   - Anchor point map (MapLibre, metro dots with proportional rings, hover tooltips)
   - Stake by country horizontal bar chart (Recharts, warning badges for >8%)
   - ASN concentration list (concentrated/normal badges)
   - "How it works" explainer strip
   - CTA banner linking to https://doublezero.xyz/geolocation-interest
4. **DZDP Validators** tab — placeholder "Coming soon"

## Design

- View switcher uses `useSearchParams` for URL state
- Concentration view uses existing `fetchGeoConcentration` API + `fetchMetros` for coordinates
- API types added to `web/src/lib/api.ts` (will be superseded when #549 merges)
- All components follow existing patterns (Tailwind v4, lucide icons, Recharts, MapLibre)

## Status

- [x] View switcher with URL param sync
- [x] DZDP Concentration view implementation
- [x] TypeScript types for API response
- [x] CHANGELOG updated
- [ ] Reviews and PR
