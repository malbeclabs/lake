# Publisher Check: Name, Stake & Search — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add validator name and stake columns to the publisher check page, and enable searching by vote ID and validator name.

**Architecture:** Add `validator_name` to the backend response (from the already-joined `validatorsapp_validators_current` table). Convert frontend search from server-side to fully client-side filtering to support matching on all fields. Add two new sortable columns (Name, Stake) after Vote ID.

**Tech Stack:** Go (backend handler), React/TypeScript (frontend), ClickHouse (data source)

---

### Task 1: Backend — Add `validator_name` to response

**Files:**
- Modify: `api/handlers/publisher_check.go`

**Step 1: Add field to struct**

Add `ValidatorName` to `PublisherCheckItem` after `ValidatorVersion`:

```go
ValidatorVersion        string `json:"validator_version"`
ValidatorName           string `json:"validator_name"`
ValidatorVersionOk      bool   `json:"validator_version_ok"`
```

**Step 2: Add column to SQL SELECT**

In the SQL query, add after the `validator_version` line (line 133):

```sql
if(va.software_version != '', va.software_version, COALESCE(g.version, '')) AS validator_version,
COALESCE(va.name, '') AS validator_name
```

**Step 3: Add to Scan**

Add `&p.ValidatorName` to the `rows.Scan()` call, after `&p.ValidatorVersion`:

```go
&p.ValidatorVersion,
&p.ValidatorName,
```

**Step 4: Run tests to verify nothing breaks**

Run: `cd /workspaces/code/lake && go test -v ./api/handlers/ -run TestGetPublisherCheck -count=1`
Expected: All existing tests pass.

**Step 5: Commit**

```
api: add validator_name to publisher check response
```

---

### Task 2: Backend test — Verify validator_name is returned

**Files:**
- Modify: `api/handlers/publisher_check_test.go`

**Step 1: Add assertions for ValidatorName**

In `TestGetPublisherCheck_AllPublishers`, add assertions after the existing `ValidatorClient` checks:

For `pub1` (dzuser1, has validators.app entry with name "Validator 1"):
```go
assert.Equal(t, "Validator 1", pub1.ValidatorName)
```

For `pub2` (dzuser2, has NO validators.app entry):
```go
assert.Equal(t, "", pub2.ValidatorName)
```

For `pub3` (dzuser3, has validators.app entry with name "Validator 3"):
```go
assert.Equal(t, "Validator 3", pub3.ValidatorName)
```

**Step 2: Run tests**

Run: `cd /workspaces/code/lake && go test -v ./api/handlers/ -run TestGetPublisherCheck -count=1`
Expected: All tests pass including new assertions.

**Step 3: Commit**

```
api: add validator_name assertions to publisher check tests
```

---

### Task 3: Frontend — Add `validator_name` to API types

**Files:**
- Modify: `web/src/lib/api.ts`

**Step 1: Add field to interface**

Add `validator_name` to `PublisherCheckItem` interface, after `validator_version`:

```typescript
validator_version: string
validator_name: string
validator_version_ok: boolean
```

**Step 2: Verify types compile**

Run: `cd /workspaces/code/lake/web && npx tsc --noEmit`
Expected: No errors.

**Step 3: Commit**

```
web: add validator_name to publisher check API types
```

---

### Task 4: Frontend — Convert search to client-side and add columns

**Files:**
- Modify: `web/src/components/publisher-check-page.tsx`

This is the main UI task. Four changes in one:
1. Convert search to client-side filtering
2. Add Name column
3. Add Stake column with compact formatting
4. Update sort and colSpan

**Step 1: Update SortField type**

Add `'validator_name'` and `'activated_stake'` to the `SortField` union type:

```typescript
type SortField =
  | 'publishing'
  | 'publisher_ip'
  | 'client_ip'
  | 'dz_user_pubkey'
  | 'vote_pubkey'
  | 'validator_name'
  | 'activated_stake'
  | 'dz_device_code'
  | 'dz_metro_code'
  | 'publishing_leader_shreds'
  | 'publishing_retransmitted'
  | 'leader_slots'
  | 'validator_client'
```

**Step 2: Add stake formatting helper**

Add before the component function:

```typescript
function formatStake(lamports: number): string {
  if (lamports === 0) return ''
  const sol = lamports / 1e9
  if (sol >= 1e6) return `${(sol / 1e6).toFixed(2)}M`
  if (sol >= 1e3) return `${(sol / 1e3).toFixed(0)}K`
  return sol.toLocaleString(undefined, { maximumFractionDigits: 0 })
}

function formatStakeExact(lamports: number): string {
  if (lamports === 0) return '0 SOL'
  return `${(lamports / 1e9).toLocaleString(undefined, { maximumFractionDigits: 2 })} SOL`
}
```

**Step 3: Convert search to client-side**

Change the fetch to always load all data (remove `activeFilter` from queryKey and queryFn):

```typescript
const { data, isLoading, error } = useQuery({
  queryKey: ['publisher-check', epochs],
  queryFn: () => fetchPublisherCheck(undefined, epochs),
  refetchInterval: 30000,
})
```

Change `handleFilterSubmit` to just update URL state without triggering a refetch. Remove `activeFilter` from the fetch dependencies. The `q` param now drives client-side filtering only.

Add a client-side filter step. Insert a new `useMemo` after `sortedPublishers` and before `nonBackupPublishers`, that filters on `activeFilter`:

```typescript
const searchFilteredPublishers = useMemo(() => {
  if (!activeFilter) return sortedPublishers
  const q = activeFilter.toLowerCase()
  return sortedPublishers.filter(pub =>
    pub.publisher_ip.toLowerCase().includes(q) ||
    pub.client_ip.toLowerCase().includes(q) ||
    pub.dz_user_pubkey.toLowerCase().includes(q) ||
    pub.vote_pubkey.toLowerCase().includes(q) ||
    pub.validator_name.toLowerCase().includes(q)
  )
}, [sortedPublishers, activeFilter])
```

Update `nonBackupPublishers` to use `searchFilteredPublishers` instead of `sortedPublishers`:

```typescript
const nonBackupPublishers = useMemo(() =>
  showBackups ? searchFilteredPublishers : searchFilteredPublishers.filter(pub => !pub.is_backup),
[searchFilteredPublishers, showBackups])
```

**Step 4: Update search input placeholder**

Change the placeholder text:

```
"Search by name, Vote ID, IP, or DZ ID..."
```

**Step 5: Add sort cases**

In the sort `switch`, add after the `vote_pubkey` case:

```typescript
case 'validator_name': cmp = a.validator_name.localeCompare(b.validator_name); break
case 'activated_stake': cmp = Number(a.activated_stake) - Number(b.activated_stake); break
```

Remove the `default` case that sorts by `activated_stake` and make `activated_stake` an explicit case. Keep `default: cmp = 0`.

**Step 6: Add table header columns**

After the Vote ID `<th>`, add:

```tsx
<th className={thClass} onClick={() => handleSort('validator_name')}>
  Name<SortIcon field="validator_name" />
</th>
<th className={thClass} onClick={() => handleSort('activated_stake')}>
  Stake<SortIcon field="activated_stake" />
</th>
```

**Step 7: Add table body cells**

After the Vote ID `<td>`, add:

```tsx
<td className="px-4 py-3 text-sm max-w-[200px] truncate" title={pub.validator_name}>
  {pub.validator_name || '\u2014'}
</td>
<td className="px-4 py-3 text-sm tabular-nums" title={formatStakeExact(pub.activated_stake)}>
  {pub.activated_stake ? formatStake(pub.activated_stake) : '\u2014'}
</td>
```

**Step 8: Update colSpan**

Change the empty-state `colSpan` from `11` to `13`.

**Step 9: Verify types compile**

Run: `cd /workspaces/code/lake/web && npx tsc --noEmit`
Expected: No errors.

**Step 10: Commit**

```
web: add name and stake columns, enable search by vote ID and name
```
