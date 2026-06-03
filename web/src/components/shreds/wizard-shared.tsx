import { Check } from 'lucide-react'

export function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section>
      <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wider mb-3">{title}</h2>
      {children}
    </section>
  )
}

export interface Step {
  label: string
  status: 'done' | 'current' | 'pending'
}

export function StepBar({ steps }: { steps: readonly Step[] }) {
  return (
    <div className="flex flex-wrap items-center gap-2 text-sm">
      {steps.map((step, i) => (
        <div key={step.label} className="flex items-center gap-2">
          <div
            className={`inline-flex items-center gap-2 px-3 py-1.5 rounded-full border transition-colors ${
              step.status === 'done'
                ? 'bg-primary text-primary-foreground border-primary'
                : step.status === 'current'
                  ? 'bg-primary/10 text-foreground border-primary border-2 py-[5px]'
                  : 'bg-background text-muted-foreground border-border'
            }`}
          >
            <span
              className={`inline-flex items-center justify-center h-5 w-5 rounded-full text-xs tabular-nums ${
                step.status === 'done'
                  ? 'bg-primary-foreground/20'
                  : step.status === 'current'
                    ? 'bg-primary text-primary-foreground'
                    : 'bg-muted'
              }`}
            >
              {step.status === 'done' ? <Check className="h-3 w-3" /> : i + 1}
            </span>
            <span className="font-medium text-xs uppercase tracking-wider">{step.label}</span>
          </div>
          {i < steps.length - 1 && <div className="h-px w-4 bg-border" />}
        </div>
      ))}
    </div>
  )
}
