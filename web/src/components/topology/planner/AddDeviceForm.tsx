import { useEffect, useMemo, useState } from 'react'
import type { TopologyMetro } from '@/lib/api'
import { fetchContributors } from '@/lib/api'

const MAX_SUGGESTIONS = 8

export interface AddDeviceSubmitValue {
  code: string
  contributorCode: string
  contributorPk?: string
  metroPk?: string
  newMetro?: { code: string; latitude: number; longitude: number }
}

export function AddDeviceForm({
  metros,
  defaultMetroPk,
  newMetroCoords,
  onSubmit,
  onCancel,
}: {
  metros: TopologyMetro[]
  defaultMetroPk: string
  // [lng, lat] of the map-click drop point; becomes a new metro's coordinates.
  newMetroCoords: [number, number]
  onSubmit: (v: AddDeviceSubmitValue) => void
  onCancel: () => void
}) {
  const [code, setCode] = useState('')
  const [touched, setTouched] = useState(false)

  // Contributor combobox: a dropdown of existing contributors (fetched by code),
  // plus the ability to type a brand-new contributor code.
  const [contributors, setContributors] = useState<{ pk: string; code: string }[]>([])
  const [contributorInput, setContributorInput] = useState('')
  const [contributorPk, setContributorPk] = useState<string | undefined>(undefined)

  useEffect(() => {
    let cancelled = false
    fetchContributors(500)
      .then((res) => {
        if (!cancelled) setContributors(res.items.map((c) => ({ pk: c.pk, code: c.code })))
      })
      .catch(() => {
        if (!cancelled) setContributors([])
      })
    return () => {
      cancelled = true
    }
  }, [])

  const contributorMatches = useMemo(() => {
    const q = contributorInput.trim().toLowerCase()
    const list = q ? contributors.filter((c) => c.code.toLowerCase().includes(q)) : contributors
    return list.slice(0, MAX_SUGGESTIONS)
  }, [contributors, contributorInput])

  const pickContributor = (c: { pk: string; code: string }) => {
    setContributorInput(c.code)
    setContributorPk(c.pk)
  }
  const changeContributorInput = (v: string) => {
    setContributorInput(v)
    setContributorPk(contributors.find((c) => c.code === v)?.pk)
  }

  // Metro combobox: a dropdown of existing metros (from the live draft), plus the
  // ability to type a brand-new metro code -- its coordinates come from the map
  // click that opened this form (newMetroCoords), not from typed input.
  const defaultMetro = metros.find((m) => m.pk === defaultMetroPk)
  const [metroInput, setMetroInput] = useState(defaultMetro?.code ?? '')
  const [metroPk, setMetroPk] = useState<string | undefined>(defaultMetro?.pk)

  const metroMatches = useMemo(() => {
    const q = metroInput.trim().toLowerCase()
    const list = q
      ? metros.filter((m) => m.code.toLowerCase().includes(q) || m.name.toLowerCase().includes(q))
      : metros
    return list.slice(0, MAX_SUGGESTIONS)
  }, [metros, metroInput])

  const pickMetro = (m: TopologyMetro) => {
    setMetroInput(m.code)
    setMetroPk(m.pk)
  }
  const changeMetroInput = (v: string) => {
    setMetroInput(v)
    setMetroPk(metros.find((m) => m.code === v)?.pk)
  }

  const contributorCode = contributorInput.trim()
  const newMetroCode = metroPk ? '' : metroInput.trim()
  const valid = code.trim() !== '' && contributorCode !== '' && (metroPk !== undefined || newMetroCode !== '')

  const submit = () => {
    setTouched(true)
    if (!valid) return
    onSubmit({
      code: code.trim(),
      contributorCode,
      contributorPk,
      metroPk,
      newMetro: metroPk
        ? undefined
        : { code: newMetroCode, latitude: newMetroCoords[1], longitude: newMetroCoords[0] },
    })
  }

  return (
    <div className="bg-card border border-border rounded-md shadow-lg p-3 w-72 space-y-2">
      <div className="text-xs font-medium">New device</div>
      <label className="block text-xs text-muted-foreground">
        Code
        <input
          autoFocus
          value={code}
          onChange={(e) => setCode(e.target.value)}
          placeholder="nyc-x2"
          className="mt-1 w-full px-2 py-1 text-sm bg-muted border border-border rounded"
        />
      </label>
      <label className="block text-xs text-muted-foreground">
        Contributor
        <input
          value={contributorInput}
          onChange={(e) => changeContributorInput(e.target.value)}
          placeholder="Existing code, or type a new one"
          className="mt-1 w-full px-2 py-1 text-sm bg-muted border border-border rounded"
        />
      </label>
      {contributorMatches.length > 0 && (
        <div className="flex flex-wrap gap-1">
          {contributorMatches.map((c) => (
            <button
              key={c.pk}
              type="button"
              onClick={() => pickContributor(c)}
              className={`px-1.5 py-0.5 text-[11px] rounded border ${
                contributorPk === c.pk
                  ? 'border-accent bg-accent/20'
                  : 'border-border bg-muted hover:bg-muted/80'
              }`}
            >
              {c.code}
            </button>
          ))}
        </div>
      )}
      {contributorCode !== '' && !contributorPk && (
        <p className="text-[10px] text-muted-foreground">
          Will create a new contributor &ldquo;{contributorCode}&rdquo;.
        </p>
      )}
      <label className="block text-xs text-muted-foreground">
        Metro
        <input
          value={metroInput}
          onChange={(e) => changeMetroInput(e.target.value)}
          placeholder="Existing metro, or type a new one"
          className="mt-1 w-full px-2 py-1 text-sm bg-muted border border-border rounded"
        />
      </label>
      {metroMatches.length > 0 && (
        <div className="flex flex-wrap gap-1">
          {metroMatches.map((m) => (
            <button
              key={m.pk}
              type="button"
              onClick={() => pickMetro(m)}
              className={`px-1.5 py-0.5 text-[11px] rounded border ${
                metroPk === m.pk ? 'border-accent bg-accent/20' : 'border-border bg-muted hover:bg-muted/80'
              }`}
            >
              {m.code} - {m.name}
            </button>
          ))}
        </div>
      )}
      {newMetroCode !== '' && (
        <p className="text-[10px] text-muted-foreground">
          Will create a new metro &ldquo;{newMetroCode}&rdquo; at {newMetroCoords[1].toFixed(4)},{' '}
          {newMetroCoords[0].toFixed(4)}.
        </p>
      )}
      {touched && !valid && (
        <p className="text-[11px] text-red-500">Code, contributor and metro are required.</p>
      )}
      <div className="flex gap-2 pt-1">
        <button
          onClick={submit}
          className="flex-1 px-2 py-1 text-xs rounded bg-accent text-accent-foreground hover:bg-accent/90"
        >
          Add device
        </button>
        <button onClick={onCancel} className="px-2 py-1 text-xs rounded bg-muted hover:bg-muted/80">
          Cancel
        </button>
      </div>
    </div>
  )
}
