import * as React from "react"
import * as TooltipPrimitive from "@radix-ui/react-tooltip"
import { cn } from "@/lib/utils"

const TooltipProvider = TooltipPrimitive.Provider
const TooltipRoot = TooltipPrimitive.Root
const TooltipTrigger = TooltipPrimitive.Trigger

const TooltipContent = React.forwardRef<
  React.ElementRef<typeof TooltipPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof TooltipPrimitive.Content>
>(({ className, sideOffset = 6, children, ...props }, ref) => (
  <TooltipPrimitive.Portal>
    <TooltipPrimitive.Content
      ref={ref}
      sideOffset={sideOffset}
      data-tooltip-content=""
      className={cn(
        "z-50 max-w-xs rounded-lg border border-border bg-popover px-3 py-2 text-xs leading-relaxed text-popover-foreground shadow-lg",
        className,
      )}
      {...props}
    >
      {children}
      <TooltipPrimitive.Arrow className="fill-popover" width={11} height={5} />
    </TooltipPrimitive.Content>
  </TooltipPrimitive.Portal>
))
TooltipContent.displayName = TooltipPrimitive.Content.displayName

interface TooltipProps
  extends Pick<
    React.ComponentPropsWithoutRef<typeof TooltipPrimitive.Content>,
    "side" | "align" | "sideOffset"
  > {
  /** Tooltip text/content. When empty the trigger renders as-is with no tooltip. */
  content: React.ReactNode
  /** Single element that triggers the tooltip; rendered via `asChild`. */
  children: React.ReactElement
  /** Override hover open delay (ms). Defaults to the provider value. */
  delayDuration?: number
  /** Extra classes for the tooltip content panel. */
  className?: string
}

/**
 * Convenience wrapper around the Radix tooltip primitives. Renders its child as
 * the trigger (via `asChild`), so it preserves DOM/table semantics, and portals
 * the content out of `overflow:hidden` containers.
 */
export function Tooltip({
  content,
  children,
  delayDuration,
  className,
  side,
  align,
  sideOffset,
}: TooltipProps) {
  if (content === null || content === undefined || content === "") return children
  return (
    <TooltipRoot delayDuration={delayDuration}>
      <TooltipTrigger asChild>{children}</TooltipTrigger>
      <TooltipContent side={side} align={align} sideOffset={sideOffset} className={className}>
        {content}
      </TooltipContent>
    </TooltipRoot>
  )
}

export { TooltipProvider, TooltipRoot, TooltipTrigger, TooltipContent }
