import { useMemo, useCallback, useState, useRef, useEffect } from "react";
import { useQuery, keepPreviousData } from "@tanstack/react-query";
import {
  Loader2,
  Puzzle,
  RefreshCw,
  TrendingUp,
  TrendingDown,
  AlertTriangle,
} from "lucide-react";
import {
  BarChart,
  Bar,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from "recharts";
import {
  fetchShredClientSeats,
  fetchShredsOverview,
  fetchShredEpochRevenue,
  fetchShredSubscriberHistory,
  fetchSwapRate,
  type ShredClientSeat,
} from "@/lib/api";
import { PageHeader } from "./page-header";
import { CopyableText } from "./copyable-text";
import { SmallDropdown } from "./topology/TimeRangeSelector";

// Helpers

function useDebouncedShimmer(
  active: boolean,
  delayMs = 250,
  minMs = 500,
): boolean {
  const [visible, setVisible] = useState(false);
  const shownAt = useRef<number>(0);
  const showTimer = useRef<ReturnType<typeof setTimeout>>(undefined);
  const hideTimer = useRef<ReturnType<typeof setTimeout>>(undefined);

  useEffect(() => {
    if (active) {
      clearTimeout(hideTimer.current);
      if (!visible && !showTimer.current) {
        showTimer.current = setTimeout(() => {
          showTimer.current = undefined;
          shownAt.current = Date.now();
          setVisible(true);
        }, delayMs);
      }
    } else {
      if (showTimer.current) {
        clearTimeout(showTimer.current);
        showTimer.current = undefined;
      }
      if (visible) {
        const remaining = minMs - (Date.now() - shownAt.current);
        const finish = () => {
          shownAt.current = 0;
          setVisible(false);
        };
        if (remaining <= 0) finish();
        else hideTimer.current = setTimeout(finish, remaining);
      }
    }
  }, [active, visible, delayMs, minMs]);

  useEffect(
    () => () => {
      clearTimeout(showTimer.current);
      clearTimeout(hideTimer.current);
    },
    [],
  );

  return visible;
}

function useRefreshButton(
  refetch: () => void,
  isFetching: boolean,
  minMs = 400,
) {
  const [spinning, setSpinning] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout>>(undefined);
  const onClick = useCallback(() => {
    setSpinning(true);
    refetch();
    clearTimeout(timer.current);
    timer.current = setTimeout(() => setSpinning(false), minMs);
  }, [refetch, minMs]);
  useEffect(() => () => clearTimeout(timer.current), []);
  return { spinning: spinning || isFetching, onClick };
}

function formatUSDC(n: number, compact = false): string {
  if (compact) {
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M USDC`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K USDC`;
  }
  return `${n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} USDC`;
}

function format2Z(n: number, compact = false): string {
  if (compact) {
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M 2Z`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K 2Z`;
  }
  return `${n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} 2Z`;
}

function usdcTo2Z(usdc: number, twoZPriceUSD: number | undefined): number | null {
  if (!twoZPriceUSD || twoZPriceUSD <= 0) return null;
  return usdc / twoZPriceUSD;
}

function truncatePK(pk: string, head = 6, tail = 4): string {
  if (pk.length <= head + tail + 3) return pk;
  return `${pk.slice(0, head)}...${pk.slice(-tail)}`;
}

const EPOCHS_PER_MONTH = 15;
const USDC_SCALE = 1_000_000;

// Sub-components

function Skeleton({ className = "w-20" }: { className?: string }) {
  return (
    <span
      className={`inline-block h-5 ${className} rounded bg-muted animate-pulse align-middle`}
    />
  );
}

function SectionTitle({
  children,
  className = "mb-4",
}: {
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <div
      className={`text-xs font-medium text-muted-foreground/70 uppercase tracking-widest ${className}`}
    >
      {children}
    </div>
  );
}

function TwoZHint({
  usdc,
  twoZPriceUSD,
  compact = false,
}: {
  usdc: number;
  twoZPriceUSD: number | undefined;
  compact?: boolean;
}) {
  const twoZ = usdcTo2Z(usdc, twoZPriceUSD);
  if (twoZ == null) return null;
  return (
    <div className="text-xs font-normal text-muted-foreground/50 tabular-nums mt-0.5">
      ≈ {format2Z(twoZ, compact)}
    </div>
  );
}

// Metrics computation

interface EpochProjection {
  epoch: number;
  confirmed: number;
  atRisk: number;
}

interface MetroPricingStat {
  metro: string;
  price: number;
  monthlyEquiv: number;
  devices: number;
  tier: "Premium" | "Mid" | "Standard";
}

interface Economics {
  totalSeats: number;
  epochRevenue: number;
  mrr: number;
  arr: number;
  totalEscrow: number;
  survivingSeats: number;
  nextEpochRevenue: number;
  seatsAtRisk: number;
  revenueAtRisk: number;
  metroBreakdown: MetroStat[];
  pricingTiers: PricingTierStat[];
  runwayBuckets: RunwayBucket[];
  uniqueFunders: number;
  avgSeatsPerFunder: number;
  topFunders: TopFunder[];
  revenueProjection: EpochProjection[];
  metroPricing: MetroPricingStat[];
}

interface MetroStat {
  metro: string;
  seats: number;
  epochRevenue: number;
  mrr: number;
  escrow: number;
  revenuePct: number;
}

interface PricingTierStat {
  price: number;
  seats: number;
  epochRevenue: number;
  monthlyRevenue: number;
  seatsPct: number;
}

interface RunwayBucket {
  label: string;
  count: number;
  pct: number;
}

interface TopFunder {
  key: string;
  seats: number;
  epochRevenue: number;
  escrow: number;
  minRunwayEpochs: number;
}

function computeEconomics(seats: ShredClientSeat[]): Economics {
  const totalSeats = seats.length;
  const epochRevenue = seats.reduce(
    (sum, s) => sum + s.price_per_epoch_dollars,
    0,
  );
  const mrr = epochRevenue * EPOCHS_PER_MONTH;
  const arr = mrr * 12;
  const totalEscrow = seats.reduce(
    (sum, s) => sum + s.total_usdc_balance / USDC_SCALE,
    0,
  );

  const surviving = seats.filter(
    (s) => s.total_usdc_balance / USDC_SCALE >= s.price_per_epoch_dollars,
  );
  const atRisk = seats.filter(
    (s) => s.total_usdc_balance / USDC_SCALE < s.price_per_epoch_dollars,
  );
  const nextEpochRevenue = surviving.reduce(
    (sum, s) => sum + s.price_per_epoch_dollars,
    0,
  );

  const metroMap = new Map<
    string,
    { seats: number; revenue: number; escrow: number }
  >();
  for (const s of seats) {
    const m = metroMap.get(s.metro_code) ?? { seats: 0, revenue: 0, escrow: 0 };
    m.seats++;
    m.revenue += s.price_per_epoch_dollars;
    m.escrow += s.total_usdc_balance / USDC_SCALE;
    metroMap.set(s.metro_code, m);
  }
  const metroBreakdown: MetroStat[] = Array.from(metroMap.entries())
    .map(([metro, v]) => ({
      metro,
      seats: v.seats,
      epochRevenue: v.revenue,
      mrr: v.revenue * EPOCHS_PER_MONTH,
      escrow: v.escrow,
      revenuePct: epochRevenue > 0 ? (v.revenue / epochRevenue) * 100 : 0,
    }))
    .sort((a, b) => b.epochRevenue - a.epochRevenue);

  const tierMap = new Map<number, { seats: number }>();
  for (const s of seats) {
    const t = tierMap.get(s.price_per_epoch_dollars) ?? { seats: 0 };
    t.seats++;
    tierMap.set(s.price_per_epoch_dollars, t);
  }
  const pricingTiers: PricingTierStat[] = Array.from(tierMap.entries())
    .map(([price, v]) => ({
      price,
      seats: v.seats,
      epochRevenue: price * v.seats,
      monthlyRevenue: price * v.seats * EPOCHS_PER_MONTH,
      seatsPct: totalSeats > 0 ? (v.seats / totalSeats) * 100 : 0,
    }))
    .sort((a, b) => a.price - b.price);

  const bucketDefs: { label: string; min: number; max: number }[] = [
    { label: "0 epochs", min: 0, max: 0 },
    { label: "1 epoch", min: 1, max: 1 },
    { label: "2–5 epochs", min: 2, max: 5 },
    { label: "6–10 epochs", min: 6, max: 10 },
    { label: "10+ epochs", min: 11, max: Infinity },
  ];
  const bucketCounts = bucketDefs.map(() => 0);
  for (const s of seats) {
    const bal = s.total_usdc_balance / USDC_SCALE;
    const runway =
      s.price_per_epoch_dollars > 0
        ? Math.floor(bal / s.price_per_epoch_dollars)
        : 999;
    for (let i = 0; i < bucketDefs.length; i++) {
      if (runway >= bucketDefs[i].min && runway <= bucketDefs[i].max) {
        bucketCounts[i]++;
        break;
      }
    }
  }
  const maxBucket = Math.max(...bucketCounts, 1);
  const runwayBuckets: RunwayBucket[] = bucketDefs.map((b, i) => ({
    label: b.label,
    count: bucketCounts[i],
    pct: (bucketCounts[i] / maxBucket) * 100,
  }));

  const funderMap = new Map<string, ShredClientSeat[]>();
  for (const s of seats) {
    const arr = funderMap.get(s.funding_authority_key) ?? [];
    arr.push(s);
    funderMap.set(s.funding_authority_key, arr);
  }
  const uniqueFunders = funderMap.size;
  const avgSeatsPerFunder = uniqueFunders > 0 ? totalSeats / uniqueFunders : 0;

  const topFunders: TopFunder[] = Array.from(funderMap.entries())
    .map(([key, fSeats]) => {
      const fEpochRevenue = fSeats.reduce(
        (sum, s) => sum + s.price_per_epoch_dollars,
        0,
      );
      const fEscrow = fSeats.reduce(
        (sum, s) => sum + s.total_usdc_balance / USDC_SCALE,
        0,
      );
      const runways = fSeats.map((s) =>
        s.price_per_epoch_dollars > 0
          ? Math.floor(
              s.total_usdc_balance / USDC_SCALE / s.price_per_epoch_dollars,
            )
          : 999,
      );
      return {
        key,
        seats: fSeats.length,
        epochRevenue: fEpochRevenue,
        escrow: fEscrow,
        minRunwayEpochs: Math.min(...runways),
      };
    })
    .sort((a, b) => b.seats - a.seats)
    .slice(0, 10);

  const seatRunways = seats.map((s) => ({
    revenue: s.price_per_epoch_dollars,
    runway:
      s.price_per_epoch_dollars > 0
        ? Math.floor(
            s.total_usdc_balance / USDC_SCALE / s.price_per_epoch_dollars,
          )
        : 999,
  }));
  const maxEpochs = Math.min(
    Math.max(...seatRunways.map((s) => s.runway), 0),
    9,
  );
  const revenueProjection: EpochProjection[] = Array.from(
    { length: maxEpochs + 1 },
    (_, i) => ({
      epoch: i,
      confirmed: seatRunways
        .filter((s) => s.runway > i)
        .reduce((sum, s) => sum + s.revenue, 0),
      atRisk: seatRunways
        .filter((s) => s.runway === i)
        .reduce((sum, s) => sum + s.revenue, 0),
    }),
  );

  // Metro pricing: unique price per metro, count devices
  const metroPriceMap = new Map<string, { price: number; devices: number }>();
  for (const s of seats) {
    const entry = metroPriceMap.get(s.metro_code) ?? {
      price: s.price_per_epoch_dollars,
      devices: 0,
    };
    entry.devices++;
    metroPriceMap.set(s.metro_code, entry);
  }
  const metroPricing: MetroPricingStat[] = Array.from(metroPriceMap.entries())
    .map(
      ([metro, v]) =>
        ({
          metro,
          price: v.price,
          monthlyEquiv: v.price * EPOCHS_PER_MONTH,
          devices: v.devices,
          tier: v.price >= 100 ? "Premium" : v.price >= 60 ? "Mid" : "Standard",
        }) as MetroPricingStat,
    )
    .sort((a, b) => b.price - a.price || a.metro.localeCompare(b.metro));

  return {
    totalSeats,
    epochRevenue,
    mrr,
    arr,
    totalEscrow,
    survivingSeats: surviving.length,
    nextEpochRevenue,
    seatsAtRisk: atRisk.length,
    revenueAtRisk: epochRevenue - nextEpochRevenue,
    metroBreakdown,
    pricingTiers,
    runwayBuckets,
    uniqueFunders,
    avgSeatsPerFunder,
    topFunders,
    revenueProjection,
    metroPricing,
  };
}

function StatGroup({
  stats,
}: {
  stats: {
    label: string;
    value: React.ReactNode;
    sub?: React.ReactNode;
    accent?: "green" | "amber" | "red" | "blue";
  }[];
}) {
  return (
    <div className="grid grid-cols-1 xs:grid-cols-2 xl:grid-cols-4 gap-3">
      {stats.map((s, i) => {
        return (
          <div
            key={i}
            className="rounded-lg border border-border bg-card px-5 py-5 min-w-0"
          >
            <div className="text-[10px] font-medium text-muted-foreground/50 uppercase tracking-widest mb-3">
              {s.label}
            </div>
            <div className="text-xl sm:text-2xl font-semibold tabular-nums tracking-tight">
              {s.value}
            </div>
            {s.sub && (
              <div className="text-[11px] text-muted-foreground/40 mt-2">
                {s.sub}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

// Main page

export function ShredsEconomicsPage() {
  const {
    data: seatsData,
    isFetching: rawFetching,
    error,
    refetch,
  } = useQuery({
    queryKey: ["shred-client-seats-economics"],
    queryFn: () =>
      fetchShredClientSeats({
        limit: 500,
        status: "active,expiring,pending",
        sortBy: "last_activity",
        sortDir: "desc",
      }),
    placeholderData: keepPreviousData,
    refetchInterval: 30_000,
  });

  const { data: overview } = useQuery({
    queryKey: ["shreds-overview"],
    queryFn: fetchShredsOverview,
    refetchInterval: 30_000,
  });

  const { data: epochRevenueHistory } = useQuery({
    queryKey: ["shreds-epoch-revenue"],
    queryFn: () => fetchShredEpochRevenue(12),
    refetchInterval: 60_000,
  });

  const { data: subscriberHistory } = useQuery({
    queryKey: ["shreds-subscriber-history"],
    queryFn: () => fetchShredSubscriberHistory(50),
    refetchInterval: 60_000,
  });

  const { data: swapRate } = useQuery({
    queryKey: ["swap-rate"],
    queryFn: fetchSwapRate,
    refetchInterval: 60_000,
    staleTime: 30_000,
  });
  const twoZPriceUSD = swapRate?.twoz_price_usd;

  const [subscriberRange, setSubscriberRange] = useState<1 | 5 | 10 | "all">(
    "all",
  );
  const [revenueRange, setRevenueRange] = useState<1 | 5 | 10 | "all">("all");

  const refresh = useRefreshButton(refetch, rawFetching);
  const shimmerVisible = useDebouncedShimmer(rawFetching, 250, 800);

  const seats = seatsData?.items ?? [];
  const econ = useMemo(
    () => (seats.length > 0 ? computeEconomics(seats) : null),
    [seats],
  );

  const currentEpoch =
    overview?.current_solana_epoch ?? overview?.current_subscription_epoch;

  if (error && !seatsData) {
    return (
      <div className="flex flex-col items-center justify-center py-24 text-center">
        <AlertTriangle className="h-8 w-8 text-muted-foreground mb-3" />
        <p className="text-sm text-muted-foreground">
          Failed to load economics data.
        </p>
      </div>
    );
  }

  const survivalPct = econ ? (econ.survivingSeats / econ.totalSeats) * 100 : 0;
  const riskPct = econ ? (econ.revenueAtRisk / econ.epochRevenue) * 100 : 0;
  const epochCoverage = econ ? econ.totalEscrow / econ.epochRevenue : 0;

  return (
    <div className="flex-1 overflow-y-auto overflow-x-hidden">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={Puzzle}
          title="Shreds Economics"
          count={econ?.totalSeats}
          subtitle={
            <div className="flex items-center gap-2 text-xs text-muted-foreground">
              {currentEpoch != null && <span>Epoch {currentEpoch}</span>}
              {currentEpoch != null && <span className="text-muted-foreground/40">·</span>}
              <span>Fees are in USDC, protocol revenue in 2Z</span>
            </div>
          }
          actions={
            <button
              onClick={refresh.onClick}
              className="p-1.5 rounded text-muted-foreground hover:text-foreground hover:bg-muted transition-colors"
              title="Refresh"
            >
              {refresh.spinning ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <RefreshCw className="h-4 w-4" />
              )}
            </button>
          }
        />

        <div className="h-0.5 w-full overflow-hidden rounded-full mb-6">
          {shimmerVisible && (
            <div className="h-full w-1/3 bg-muted-foreground/40 animate-[shimmer_0.9s_ease-in-out_infinite] rounded-full" />
          )}
        </div>

        <div className="space-y-8">
          {/* Revenue Overview */}
          <section>
            <SectionTitle>Revenue Overview</SectionTitle>
            <StatGroup
              stats={[
                {
                  label: "Epoch Revenue",
                  value: econ ? (
                    <>
                      {formatUSDC(econ.epochRevenue)}
                      <TwoZHint usdc={econ.epochRevenue} twoZPriceUSD={twoZPriceUSD} />
                    </>
                  ) : (
                    <Skeleton />
                  ),
                  sub: "current epoch",
                  accent: "blue",
                },
                {
                  label: "MRR",
                  value: econ ? (
                    <>
                      {formatUSDC(econ.mrr)}
                      <TwoZHint usdc={econ.mrr} twoZPriceUSD={twoZPriceUSD} />
                    </>
                  ) : (
                    <Skeleton />
                  ),
                  sub: `${EPOCHS_PER_MONTH} epochs/mo`,
                  accent: "green",
                },
                {
                  label: "ARR",
                  value: econ ? (
                    <>
                      {formatUSDC(econ.arr, true)}
                      <TwoZHint usdc={econ.arr} twoZPriceUSD={twoZPriceUSD} compact />
                    </>
                  ) : (
                    <Skeleton />
                  ),
                  sub: "annualized",
                  accent: "green",
                },
                {
                  label: "Total Escrow",
                  value: econ ? (
                    <>
                      {formatUSDC(econ.totalEscrow)}
                      <TwoZHint usdc={econ.totalEscrow} twoZPriceUSD={twoZPriceUSD} />
                    </>
                  ) : (
                    <Skeleton />
                  ),
                  sub: "locked balance",
                },
              ]}
            />
          </section>

          {/* Next Epoch Forecast */}
          <section>
            <SectionTitle>Next Epoch Forecast</SectionTitle>
            <StatGroup
              stats={[
                {
                  label: "Predicted Revenue",
                  value: econ ? (
                    <>
                      {formatUSDC(econ.nextEpochRevenue)}
                      <TwoZHint usdc={econ.nextEpochRevenue} twoZPriceUSD={twoZPriceUSD} />
                    </>
                  ) : (
                    <Skeleton />
                  ),
                  sub: "seats with sufficient balance",
                  accent: "green",
                },
                {
                  label: "Surviving Seats",
                  value: econ ? (
                    <span>
                      {econ.survivingSeats}
                      <span className="text-base font-normal text-muted-foreground">
                        {" "}
                        / {econ.totalSeats}
                      </span>
                    </span>
                  ) : (
                    <Skeleton />
                  ),
                  sub: "balance ≥ price/epoch",
                },
                {
                  label: "Seats at Risk",
                  value: econ ? String(econ.seatsAtRisk) : <Skeleton />,
                  sub: "balance < price/epoch",
                  accent: econ && econ.seatsAtRisk > 0 ? "red" : undefined,
                },
                {
                  label: "Revenue at Risk",
                  value: econ ? (
                    <>
                      {formatUSDC(econ.revenueAtRisk)}
                      <TwoZHint usdc={econ.revenueAtRisk} twoZPriceUSD={twoZPriceUSD} />
                    </>
                  ) : (
                    <Skeleton />
                  ),
                  sub: "may not renew",
                  accent: econ && econ.revenueAtRisk > 0 ? "amber" : undefined,
                },
              ]}
            />
          </section>

          {/* Retention Signal */}
          {econ &&
            (() => {
              const seatColor =
                survivalPct >= 80
                  ? {
                      bar: "bg-green-500/60",
                      line: "bg-green-500/50",
                      icon: "text-green-400",
                    }
                  : survivalPct >= 50
                    ? {
                        bar: "bg-yellow-500/60",
                        line: "bg-yellow-500/50",
                        icon: "text-yellow-400",
                      }
                    : {
                        bar: "bg-red-500/60",
                        line: "bg-red-500/50",
                        icon: "text-red-400",
                      };
              const riskColor =
                riskPct === 0
                  ? {
                      bar: "bg-green-500/60",
                      line: "bg-green-500/50",
                      icon: "text-green-400",
                    }
                  : riskPct < 20
                    ? {
                        bar: "bg-yellow-500/60",
                        line: "bg-yellow-500/50",
                        icon: "text-yellow-400",
                      }
                    : {
                        bar: "bg-red-500/60",
                        line: "bg-red-500/50",
                        icon: "text-red-400",
                      };
              const covColor =
                epochCoverage >= 5
                  ? {
                      bar: "bg-green-500/60",
                      line: "bg-green-500/50",
                      icon: "text-green-400",
                    }
                  : epochCoverage >= 2
                    ? {
                        bar: "bg-yellow-500/60",
                        line: "bg-yellow-500/50",
                        icon: "text-yellow-400",
                      }
                    : {
                        bar: "bg-red-500/60",
                        line: "bg-red-500/50",
                        icon: "text-red-400",
                      };
              return (
                <section>
                  <SectionTitle>Retention Signal</SectionTitle>
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                    {/* Seats Funded */}
                    <div className="relative rounded-lg bg-card border border-border overflow-hidden p-5">
                      <div
                        className={`absolute top-0 left-0 right-0 h-px ${seatColor.line}`}
                      />
                      <div className="flex items-start justify-between mb-3">
                        <span className="text-[11px] font-medium text-muted-foreground/60 uppercase tracking-widest">
                          Seats Funded
                        </span>
                        <TrendingUp
                          className={`h-3.5 w-3.5 mt-0.5 ${seatColor.icon}`}
                        />
                      </div>
                      <div className="flex items-baseline gap-1 mb-4">
                        <span className="text-4xl font-bold tabular-nums tracking-tight">
                          {survivalPct.toFixed(1)}
                        </span>
                        <span className="text-lg text-muted-foreground font-medium">
                          %
                        </span>
                      </div>
                      <div className="h-0.5 rounded-full bg-muted/40 overflow-hidden mb-3">
                        <div
                          className={`h-full rounded-full transition-all duration-700 ${seatColor.bar}`}
                          style={{ width: `${survivalPct}%` }}
                        />
                      </div>
                      <div className="text-[11px] text-muted-foreground/50">
                        {econ.survivingSeats} of {econ.totalSeats} seats survive
                        next epoch
                      </div>
                    </div>
                    {/* Revenue at Risk */}
                    <div className="relative rounded-lg bg-card border border-border overflow-hidden p-5">
                      <div
                        className={`absolute top-0 left-0 right-0 h-px ${riskColor.line}`}
                      />
                      <div className="flex items-start justify-between mb-3">
                        <span className="text-[11px] font-medium text-muted-foreground/60 uppercase tracking-widest">
                          Revenue at Risk
                        </span>
                        <TrendingDown
                          className={`h-3.5 w-3.5 mt-0.5 ${riskColor.icon}`}
                        />
                      </div>
                      <div className="flex flex-col gap-0.5 mb-4">
                        <span className="text-4xl font-bold tabular-nums tracking-tight">
                          {formatUSDC(econ.revenueAtRisk, true)}
                        </span>
                        {(() => {
                          const twoZ = usdcTo2Z(econ.revenueAtRisk, twoZPriceUSD);
                          if (twoZ == null) return null;
                          return (
                            <span className="text-xs text-muted-foreground/50 tabular-nums">
                              ≈ {format2Z(twoZ, true)}
                            </span>
                          );
                        })()}
                      </div>
                      <div className="h-0.5 rounded-full bg-muted/40 overflow-hidden mb-3">
                        <div
                          className={`h-full rounded-full transition-all duration-700 ${riskColor.bar}`}
                          style={{ width: `${riskPct}%` }}
                        />
                      </div>
                      <div className="text-[11px] text-muted-foreground/50">
                        {econ.seatsAtRisk} seats may lapse —{" "}
                        {riskPct.toFixed(1)}% of epoch revenue
                      </div>
                    </div>
                    {/* Epoch Coverage */}
                    <div className="relative rounded-lg bg-card border border-border overflow-hidden p-5">
                      <div
                        className={`absolute top-0 left-0 right-0 h-px ${covColor.line}`}
                      />
                      <div className="flex items-start justify-between mb-3">
                        <span className="text-[11px] font-medium text-muted-foreground/60 uppercase tracking-widest">
                          Epoch Coverage
                        </span>
                        <span
                          className={`text-[11px] font-bold mt-0.5 ${covColor.icon}`}
                        >
                          EPOCHS
                        </span>
                      </div>
                      <div className="flex items-baseline gap-1 mb-4">
                        <span className="text-4xl font-bold tabular-nums tracking-tight">
                          {epochCoverage.toFixed(1)}
                        </span>
                        <span className="text-lg text-muted-foreground font-medium">
                          ×
                        </span>
                      </div>
                      <div className="h-0.5 rounded-full bg-muted/40 overflow-hidden mb-3">
                        <div
                          className={`h-full rounded-full transition-all duration-700 ${covColor.bar}`}
                          style={{
                            width: `${Math.min((epochCoverage / 10) * 100, 100)}%`,
                          }}
                        />
                      </div>
                      <div className="text-[11px] text-muted-foreground/50">
                        Total escrow covers {epochCoverage.toFixed(1)} epochs of
                        current revenue
                      </div>
                    </div>
                  </div>
                </section>
              );
            })()}

          {/* Area charts: Revenue per Epoch + Active Subscribers */}
          <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
            {/* Revenue per Epoch */}
            <section>
              <div className="flex items-center justify-between mb-4">
                <SectionTitle className="">
                  Revenue per Epoch — USDC
                </SectionTitle>
                <div>
                  <SmallDropdown
                    value={String(revenueRange)}
                    options={[
                      { value: "1", label: "Last epoch" },
                      { value: "5", label: "Last 5 epochs" },
                      { value: "10", label: "Last 10 epochs" },
                      { value: "all", label: "All" },
                    ]}
                    onChange={(v) =>
                      setRevenueRange(
                        v === "all" ? "all" : (Number(v) as 1 | 5 | 10),
                      )
                    }
                  />
                </div>
              </div>
              <div className="border border-border rounded-lg bg-card px-2 pt-4 pb-2">
                {(() => {
                  const raw = epochRevenueHistory
                    ? revenueRange === "all"
                      ? epochRevenueHistory
                      : epochRevenueHistory.slice(-revenueRange)
                    : undefined;
                  const data =
                    raw && raw.length === 1
                      ? [
                          {
                            epoch: `_${raw[0].epoch}`,
                            total_dollars: raw[0].total_dollars,
                          },
                          raw[0],
                        ]
                      : raw;
                  if (data && data.length > 0)
                    return (
                      <>
                        <div className="flex items-center gap-4 px-2 mb-3">
                          <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
                            <span className="inline-block h-2 w-2 rounded-sm bg-blue-400/80" />
                            USDC collected
                          </span>
                        </div>
                        <ResponsiveContainer width="100%" height={192}>
                          <AreaChart
                            data={data}
                            margin={{ top: 8, right: 12, left: 0, bottom: 0 }}
                          >
                            <defs>
                              <linearGradient
                                id="areaBlue"
                                x1="0"
                                y1="0"
                                x2="0"
                                y2="1"
                              >
                                <stop
                                  offset="0%"
                                  stopColor="#3b82f6"
                                  stopOpacity={0.35}
                                />
                                <stop
                                  offset="100%"
                                  stopColor="#3b82f6"
                                  stopOpacity={0}
                                />
                              </linearGradient>
                            </defs>
                            <CartesianGrid
                              strokeDasharray="3 3"
                              stroke="var(--border)"
                              vertical={false}
                            />
                            <XAxis
                              dataKey="epoch"
                              tickLine={false}
                              axisLine={false}
                              tick={{
                                fontSize: 11,
                                fill: "var(--muted-foreground)",
                              }}
                              dy={6}
                              tickFormatter={(v) =>
                                String(v).startsWith("_") ? "" : String(v)
                              }
                            />
                            <YAxis
                              tickLine={false}
                              axisLine={false}
                              tick={{
                                fontSize: 11,
                                fill: "var(--muted-foreground)",
                              }}
                              tickFormatter={(v: number) => `${v}`}
                              width={52}
                            />
                            <Tooltip
                              cursor={{
                                stroke: "var(--border)",
                                strokeWidth: 1,
                              }}
                              content={({ active, payload, label }) => {
                                if (
                                  !active ||
                                  !payload?.length ||
                                  String(label).startsWith("_")
                                )
                                  return null;
                                return (
                                  <div className="bg-card border border-border rounded-lg px-3 py-2 text-xs shadow-xl">
                                    <div className="text-muted-foreground mb-1">
                                      Epoch {label}
                                    </div>
                                    <div className="font-semibold text-foreground">
                                      {formatUSDC(Number(payload[0].value))}
                                    </div>
                                  </div>
                                );
                              }}
                            />
                            <Area
                              type="monotone"
                              dataKey="total_dollars"
                              stroke="#3b82f6"
                              strokeWidth={2}
                              fill="url(#areaBlue)"
                              dot={false}
                              activeDot={{
                                r: 4,
                                fill: "#3b82f6",
                                strokeWidth: 0,
                              }}
                            />
                          </AreaChart>
                        </ResponsiveContainer>
                        <p className="text-xs text-muted-foreground px-2 pt-2">
                          Historical USDC collected per epoch.
                        </p>
                      </>
                    );
                  if (data === undefined)
                    return (
                      <div className="h-56 rounded bg-muted animate-pulse" />
                    );
                  return (
                    <div className="h-56 flex items-center justify-center text-sm text-muted-foreground">
                      No epoch revenue data available
                    </div>
                  );
                })()}
              </div>
            </section>

            {/* Active Subscribers */}
            <section>
              <div className="flex items-center justify-between mb-4">
                <SectionTitle className="">
                  Active Subscribers per Epoch
                </SectionTitle>
                <div>
                  <SmallDropdown
                    value={String(subscriberRange)}
                    options={[
                      { value: "1", label: "Last epoch" },
                      { value: "5", label: "Last 5 epochs" },
                      { value: "10", label: "Last 10 epochs" },
                      { value: "all", label: "All" },
                    ]}
                    onChange={(v) =>
                      setSubscriberRange(
                        v === "all" ? "all" : (Number(v) as 1 | 5 | 10),
                      )
                    }
                  />
                </div>
              </div>
              <div className="border border-border rounded-lg bg-card px-2 pt-4 pb-2">
                {(() => {
                  const raw = subscriberHistory
                    ? subscriberRange === "all"
                      ? subscriberHistory
                      : subscriberHistory.slice(-subscriberRange)
                    : undefined;
                  const data =
                    raw && raw.length === 1
                      ? [
                          {
                            epoch: `_${raw[0].epoch}`,
                            active_seats: raw[0].active_seats,
                          },
                          raw[0],
                        ]
                      : raw;
                  if (data && data.length > 0)
                    return (
                      <>
                        <div className="flex items-center gap-4 px-2 mb-3">
                          <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
                            <span className="inline-block h-2 w-2 rounded-sm bg-violet-400/80" />
                            Active seats
                          </span>
                        </div>
                        <ResponsiveContainer width="100%" height={192}>
                          <AreaChart
                            data={data}
                            margin={{ top: 8, right: 12, left: 0, bottom: 0 }}
                          >
                            <defs>
                              <linearGradient
                                id="areaViolet"
                                x1="0"
                                y1="0"
                                x2="0"
                                y2="1"
                              >
                                <stop
                                  offset="0%"
                                  stopColor="#8b5cf6"
                                  stopOpacity={0.35}
                                />
                                <stop
                                  offset="100%"
                                  stopColor="#8b5cf6"
                                  stopOpacity={0}
                                />
                              </linearGradient>
                            </defs>
                            <CartesianGrid
                              strokeDasharray="3 3"
                              stroke="var(--border)"
                              vertical={false}
                            />
                            <XAxis
                              dataKey="epoch"
                              tickLine={false}
                              axisLine={false}
                              tick={{
                                fontSize: 11,
                                fill: "var(--muted-foreground)",
                              }}
                              dy={6}
                              tickFormatter={(v) =>
                                String(v).startsWith("_") ? "" : String(v)
                              }
                            />
                            <YAxis
                              tickLine={false}
                              axisLine={false}
                              tick={{
                                fontSize: 11,
                                fill: "var(--muted-foreground)",
                              }}
                              width={36}
                              allowDecimals={false}
                            />
                            <Tooltip
                              cursor={{
                                stroke: "var(--border)",
                                strokeWidth: 1,
                              }}
                              content={({ active, payload, label }) => {
                                if (
                                  !active ||
                                  !payload?.length ||
                                  String(label).startsWith("_")
                                )
                                  return null;
                                return (
                                  <div className="bg-card border border-border rounded-lg px-3 py-2 text-xs shadow-xl">
                                    <div className="text-muted-foreground mb-1">
                                      Epoch {label}
                                    </div>
                                    <div className="font-semibold text-foreground">
                                      {payload[0].value} seats
                                    </div>
                                  </div>
                                );
                              }}
                            />
                            <Area
                              type="monotone"
                              dataKey="active_seats"
                              stroke="#8b5cf6"
                              strokeWidth={2}
                              fill="url(#areaViolet)"
                              dot={false}
                              activeDot={{
                                r: 4,
                                fill: "#8b5cf6",
                                strokeWidth: 0,
                              }}
                            />
                          </AreaChart>
                        </ResponsiveContainer>
                        <p className="text-xs text-muted-foreground px-2 pt-2">
                          Distinct seats that transacted per epoch.
                        </p>
                      </>
                    );
                  if (data === undefined)
                    return (
                      <div className="h-56 rounded bg-muted animate-pulse" />
                    );
                  return (
                    <div className="h-56 flex items-center justify-center text-sm text-muted-foreground">
                      No subscriber history available
                    </div>
                  );
                })()}
              </div>
            </section>
          </div>

          {/* Bar charts: Predicted Revenue + Balance Runway */}
          <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
            {/* Predicted Revenue */}
            <section>
              <SectionTitle>
                Predicted Revenue (USDC) — Next{" "}
                {econ ? econ.revenueProjection.length : 0} Epochs
              </SectionTitle>
              <div className="border border-border rounded-lg bg-card px-2 pt-5 pb-2">
                {econ ? (
                  <>
                    <div className="flex items-center gap-x-3 gap-y-1.5 flex-wrap px-2 mb-3">
                      <span className="flex items-center gap-1.5 text-[11px] text-muted-foreground whitespace-nowrap">
                        <span className="inline-block h-2 w-2 rounded-sm bg-emerald-500/80 shrink-0" />
                        Confirmed
                      </span>
                      <span className="flex items-center gap-1.5 text-[11px] text-muted-foreground whitespace-nowrap">
                        <span className="inline-block h-2 w-2 rounded-sm bg-amber-400/80 shrink-0" />
                        At-risk
                      </span>
                    </div>
                    <ResponsiveContainer width="100%" height={192}>
                      <BarChart
                        data={econ.revenueProjection}
                        barCategoryGap="20%"
                        margin={{ top: 0, right: 12, left: 0, bottom: 0 }}
                      >
                        <defs>
                          <linearGradient
                            id="greenBar"
                            x1="0"
                            y1="0"
                            x2="0"
                            y2="1"
                          >
                            <stop
                              offset="0%"
                              stopColor="#22c55e"
                              stopOpacity={0.95}
                            />
                            <stop
                              offset="100%"
                              stopColor="#22c55e"
                              stopOpacity={0.65}
                            />
                          </linearGradient>
                          <linearGradient
                            id="amberBar"
                            x1="0"
                            y1="0"
                            x2="0"
                            y2="1"
                          >
                            <stop
                              offset="0%"
                              stopColor="#eab308"
                              stopOpacity={0.95}
                            />
                            <stop
                              offset="100%"
                              stopColor="#eab308"
                              stopOpacity={0.65}
                            />
                          </linearGradient>
                        </defs>
                        <CartesianGrid
                          strokeDasharray="3 3"
                          stroke="var(--border)"
                          vertical={false}
                        />
                        <XAxis
                          dataKey="epoch"
                          tickLine={false}
                          axisLine={false}
                          tick={{
                            fontSize: 11,
                            fill: "var(--muted-foreground)",
                          }}
                          dy={6}
                          tickFormatter={(v: number) =>
                            currentEpoch != null
                              ? String(currentEpoch + v)
                              : `+${v}`
                          }
                        />
                        <YAxis
                          tickLine={false}
                          axisLine={false}
                          tick={{
                            fontSize: 11,
                            fill: "var(--muted-foreground)",
                          }}
                          tickFormatter={(v: number) => `$${v}`}
                          width={52}
                        />
                        <Tooltip
                          cursor={{ fill: "var(--muted)", opacity: 0.4 }}
                          content={({ active, payload, label }) => {
                            if (!active || !payload?.length) return null;
                            const epochLabel =
                              currentEpoch != null
                                ? `Epoch ${currentEpoch + Number(label)}`
                                : `+${label} epochs`;
                            const confirmed = payload.find(
                              (p) => p.dataKey === "confirmed",
                            );
                            const atRisk = payload.find(
                              (p) => p.dataKey === "atRisk",
                            );
                            return (
                              <div className="bg-card border border-border rounded-lg px-3 py-2.5 text-xs shadow-xl space-y-1.5">
                                <div className="text-muted-foreground font-medium">
                                  {epochLabel}
                                </div>
                                {confirmed && Number(confirmed.value) > 0 && (
                                  <div className="flex items-center gap-2">
                                    <span className="inline-block h-1.5 w-1.5 rounded-full bg-emerald-400" />
                                    <span className="text-muted-foreground">
                                      Confirmed
                                    </span>
                                    <span className="ml-auto font-semibold">
                                      {formatUSDC(Number(confirmed.value))}
                                    </span>
                                  </div>
                                )}
                                {atRisk && Number(atRisk.value) > 0 && (
                                  <div className="flex items-center gap-2">
                                    <span className="inline-block h-1.5 w-1.5 rounded-full bg-amber-400" />
                                    <span className="text-muted-foreground">
                                      At-risk
                                    </span>
                                    <span className="ml-auto font-semibold">
                                      {formatUSDC(Number(atRisk.value))}
                                    </span>
                                  </div>
                                )}
                              </div>
                            );
                          }}
                        />
                        <Bar
                          dataKey="confirmed"
                          stackId="a"
                          fill="url(#greenBar)"
                          radius={[0, 0, 0, 0]}
                        />
                        <Bar
                          dataKey="atRisk"
                          stackId="a"
                          fill="url(#amberBar)"
                          radius={[3, 3, 0, 0]}
                        />
                      </BarChart>
                    </ResponsiveContainer>
                    <p className="text-xs text-muted-foreground px-2 pt-2">
                      Assumes no new deposits. At-risk = seats on their last
                      funded epoch.
                    </p>
                  </>
                ) : (
                  <div className="h-56 rounded bg-muted animate-pulse" />
                )}
              </div>
            </section>

            {/* Balance Runway */}
            <section>
              <SectionTitle>Balance Runway Distribution</SectionTitle>
              <div className="border border-border rounded-lg bg-card px-2 pt-5 pb-2">
                {econ ? (
                  <>
                    <div className="flex items-center gap-x-3 gap-y-1.5 flex-wrap px-2 mb-3">
                      {econ.runwayBuckets.map((b, i) => {
                        const colors = ["#ef4444","#f97316","#eab308","#3b82f6","#22c55e"];
                        return (
                          <span key={b.label} className="flex items-center gap-1.5 text-[11px] text-muted-foreground whitespace-nowrap">
                            <span className="inline-block h-2 w-2 rounded-sm shrink-0" style={{ background: colors[i] + "cc" }} />
                            {b.label}
                          </span>
                        );
                      })}
                    </div>
                    <ResponsiveContainer width="100%" height={192}>
                      <BarChart
                        data={econ.runwayBuckets}
                        barCategoryGap="20%"
                        margin={{ top: 0, right: 12, left: 0, bottom: 0 }}
                      >
                        <CartesianGrid
                          strokeDasharray="3 3"
                          stroke="var(--border)"
                          vertical={false}
                        />
                        <XAxis
                          dataKey="label"
                          tickLine={false}
                          axisLine={false}
                          tick={{
                            fontSize: 11,
                            fill: "var(--muted-foreground)",
                          }}
                          dy={6}
                        />
                        <YAxis
                          tickLine={false}
                          axisLine={false}
                          tick={{
                            fontSize: 11,
                            fill: "var(--muted-foreground)",
                          }}
                          width={28}
                          allowDecimals={false}
                        />
                        <Tooltip
                          cursor={{ fill: "var(--muted)", opacity: 0.4 }}
                          content={({ active, payload, label }) => {
                            if (!active || !payload?.length) return null;
                            const pct = (
                              (Number(payload[0].value) /
                                (econ.totalSeats || 1)) *
                              100
                            ).toFixed(0);
                            return (
                              <div className="bg-card border border-border rounded-lg px-3 py-2 text-xs shadow-xl">
                                <div className="text-muted-foreground mb-1">
                                  {label}
                                </div>
                                <div className="font-semibold text-foreground">
                                  {payload[0].value} seats{" "}
                                  <span className="font-normal text-muted-foreground">
                                    ({pct}%)
                                  </span>
                                </div>
                              </div>
                            );
                          }}
                        />
                        <Bar dataKey="count" radius={[3, 3, 0, 0]}>
                          {econ.runwayBuckets.map((_, i) => {
                            const colors = [
                              "#ef4444",
                              "#f97316",
                              "#eab308",
                              "#3b82f6",
                              "#22c55e",
                            ];
                            return (
                              <Cell
                                key={i}
                                fill={colors[i]}
                                fillOpacity={0.8}
                              />
                            );
                          })}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                    <p className="text-xs text-muted-foreground px-2 pt-2">
                      Epochs of runway remaining per seat balance.
                    </p>
                  </>
                ) : (
                  <div className="h-56 rounded bg-muted animate-pulse" />
                )}
              </div>
            </section>
          </div>

          {/* Pricing by Metro */}
          <section>
            <SectionTitle>Pricing by Metro</SectionTitle>
            <div className="border border-border rounded-lg overflow-hidden bg-card">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-xs text-muted-foreground border-b border-border bg-muted/30">
                      <th className="px-4 py-3 text-left font-medium uppercase tracking-wider">
                        Metro
                      </th>
                      <th className="px-4 py-3 text-right font-medium uppercase tracking-wider">
                        Price / Epoch
                      </th>
                      <th className="px-4 py-3 text-right font-medium uppercase tracking-wider">
                        Monthly Equiv
                      </th>
                      <th className="px-4 py-3 text-right font-medium uppercase tracking-wider">
                        Devices
                      </th>
                      <th className="px-4 py-3 text-left font-medium uppercase tracking-wider">
                        Tier
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {econ
                      ? econ.metroPricing.map((m, i) => {
                          const tierStyle =
                            m.tier === "Premium"
                              ? "bg-amber-500/10 text-amber-400 border-amber-500/20"
                              : m.tier === "Mid"
                                ? "bg-blue-500/10 text-blue-400 border-blue-500/20"
                                : "bg-green-500/10 text-green-400 border-green-500/20";
                          return (
                            <tr
                              key={m.metro}
                              className={`border-b border-border last:border-0 hover:bg-muted/20 transition-colors ${i % 2 === 0 ? "" : "bg-muted/10"}`}
                            >
                              <td className="px-4 py-3 font-mono text-sm font-medium uppercase tracking-wide">
                                {m.metro}
                              </td>
                              <td className="px-4 py-3 tabular-nums text-right">
                                {formatUSDC(m.price)}
                              </td>
                              <td className="px-4 py-3 tabular-nums text-right text-muted-foreground">
                                ~{formatUSDC(m.monthlyEquiv)}
                              </td>
                              <td className="px-4 py-3 tabular-nums text-right">
                                {m.devices}
                              </td>
                              <td className="px-4 py-3">
                                <span
                                  className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border ${tierStyle}`}
                                >
                                  {m.tier}
                                </span>
                              </td>
                            </tr>
                          );
                        })
                      : Array.from({ length: 5 }).map((_, i) => (
                          <tr
                            key={i}
                            className="border-b border-border last:border-0"
                          >
                            <td className="px-4 py-3">
                              <Skeleton className="w-16" />
                            </td>
                            <td className="px-4 py-3 text-right">
                              <Skeleton className="w-24" />
                            </td>
                            <td className="px-4 py-3 text-right">
                              <Skeleton className="w-20" />
                            </td>
                            <td className="px-4 py-3 text-right">
                              <Skeleton className="w-8" />
                            </td>
                            <td className="px-4 py-3">
                              <Skeleton className="w-16" />
                            </td>
                          </tr>
                        ))}
                  </tbody>
                </table>
              </div>
            </div>
          </section>

          {/* Metro Breakdown */}
          <section>
            <SectionTitle>Metro Breakdown</SectionTitle>
            <div className="relative border border-border rounded-lg overflow-hidden bg-card">
              {shimmerVisible && seatsData && (
                <div className="absolute inset-x-0 top-0 h-0.5 overflow-hidden z-10">
                  <div className="h-full w-1/3 bg-primary/60 animate-[shimmer_0.9s_ease-in-out_infinite] rounded-full" />
                </div>
              )}
              <div className="overflow-x-auto">
                <table className="min-w-160 w-full text-sm">
                  <thead>
                    <tr className="text-xs text-muted-foreground border-b border-border bg-muted/30">
                      <th className="px-4 py-3 text-left font-medium uppercase tracking-wider">
                        Metro
                      </th>
                      <th className="px-4 py-3 text-right font-medium uppercase tracking-wider">
                        Seats
                      </th>
                      <th className="px-4 py-3 text-right font-medium uppercase tracking-wider">
                        USDC/Epoch
                      </th>
                      <th className="px-4 py-3 text-right font-medium uppercase tracking-wider">
                        MRR
                      </th>
                      <th className="px-4 py-3 text-right font-medium uppercase tracking-wider">
                        Escrow
                      </th>
                      <th className="px-4 py-3 text-right font-medium uppercase tracking-wider">
                        % Rev
                      </th>
                      <th className="px-4 py-3 text-left font-medium uppercase tracking-wider w-32">
                        Share
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {econ
                      ? econ.metroBreakdown.map((m, i) => (
                          <tr
                            key={m.metro}
                            className={`border-b border-border last:border-0 hover:bg-muted/20 transition-colors ${i % 2 === 0 ? "" : "bg-muted/10"}`}
                          >
                            <td className="px-4 py-3 font-mono text-sm font-medium uppercase tracking-wide">
                              {m.metro}
                            </td>
                            <td className="px-4 py-3 tabular-nums text-right">
                              {m.seats}
                            </td>
                            <td className="px-4 py-3 tabular-nums text-right">
                              {formatUSDC(m.epochRevenue)}
                            </td>
                            <td className="px-4 py-3 tabular-nums text-right">
                              {formatUSDC(m.mrr)}
                            </td>
                            <td className="px-4 py-3 tabular-nums text-right">
                              {formatUSDC(m.escrow)}
                            </td>
                            <td className="px-4 py-3 tabular-nums text-right text-muted-foreground">
                              {m.revenuePct.toFixed(1)}%
                            </td>
                            <td className="px-4 py-3">
                              <div className="h-1.5 rounded-full bg-muted overflow-hidden w-28">
                                <div
                                  className="h-full rounded-full bg-blue-500/60"
                                  style={{ width: `${m.revenuePct}%` }}
                                />
                              </div>
                            </td>
                          </tr>
                        ))
                      : Array.from({ length: 5 }).map((_, i) => (
                          <tr
                            key={i}
                            className="border-b border-border last:border-0"
                          >
                            <td className="px-4 py-3">
                              <Skeleton className="w-16" />
                            </td>
                            <td className="px-4 py-3 text-right">
                              <Skeleton className="w-8" />
                            </td>
                            <td className="px-4 py-3 text-right">
                              <Skeleton className="w-20" />
                            </td>
                            <td className="px-4 py-3 text-right">
                              <Skeleton className="w-20" />
                            </td>
                            <td className="px-4 py-3" />
                            <td className="px-4 py-3" />
                            <td className="px-4 py-3" />
                          </tr>
                        ))}
                  </tbody>
                </table>
              </div>
            </div>
          </section>

          {/* Pricing Tiers + Runway */}
          <div className="flex flex-col gap-6">
            <section>
              <SectionTitle>Pricing Tiers</SectionTitle>
              <div className="relative border border-border rounded-lg overflow-hidden bg-card">
                <div className="overflow-x-auto">
                  <table className="min-w-96 w-full text-sm">
                    <thead>
                      <tr className="text-xs text-muted-foreground border-b border-border bg-muted/30">
                        <th className="px-4 py-3 text-left font-medium uppercase tracking-wider">
                          Price / Epoch
                        </th>
                        <th className="px-4 py-3 text-right font-medium uppercase tracking-wider">
                          Seats
                        </th>
                        <th className="px-4 py-3 text-right font-medium uppercase tracking-wider">
                          Epoch Rev
                        </th>
                        <th className="px-4 py-3 text-right font-medium uppercase tracking-wider">
                          Monthly
                        </th>
                        <th className="px-4 py-3 text-right font-medium uppercase tracking-wider">
                          Annual
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {econ
                        ? econ.pricingTiers.map((t, i) => (
                            <tr
                              key={t.price}
                              className={`border-b border-border last:border-0 ${i % 2 === 0 ? "" : "bg-muted/10"}`}
                            >
                              <td className="px-4 py-3 font-medium tabular-nums">
                                {formatUSDC(t.price)}
                                <span className="text-xs text-muted-foreground font-normal ml-1.5">
                                  ({t.seatsPct.toFixed(0)}%)
                                </span>
                              </td>
                              <td className="px-4 py-3 tabular-nums text-right">
                                {t.seats}
                              </td>
                              <td className="px-4 py-3 tabular-nums text-right">
                                {formatUSDC(t.epochRevenue)}
                              </td>
                              <td className="px-4 py-3 tabular-nums text-right">
                                {formatUSDC(t.monthlyRevenue)}
                              </td>
                              <td className="px-4 py-3 tabular-nums text-right text-muted-foreground">
                                {formatUSDC(t.monthlyRevenue * 12, true)}
                              </td>
                            </tr>
                          ))
                        : Array.from({ length: 3 }).map((_, i) => (
                            <tr
                              key={i}
                              className="border-b border-border last:border-0"
                            >
                              <td className="px-4 py-3">
                                <Skeleton />
                              </td>
                              <td className="px-4 py-3 text-right">
                                <Skeleton className="w-8" />
                              </td>
                              <td className="px-4 py-3 text-right">
                                <Skeleton className="w-20" />
                              </td>
                              <td className="px-4 py-3">
                                <Skeleton className="w-20" />
                              </td>
                              <td className="px-4 py-3">
                                <Skeleton className="w-20" />
                              </td>
                            </tr>
                          ))}
                    </tbody>
                    {econ && (
                      <tfoot>
                        <tr className="border-t-2 border-border bg-muted/20 font-semibold">
                          <td className="px-4 py-3 text-xs text-muted-foreground uppercase tracking-wider">
                            Total
                          </td>
                          <td className="px-4 py-3 tabular-nums text-right">
                            {econ.totalSeats}
                          </td>
                          <td className="px-4 py-3 tabular-nums text-right">
                            {formatUSDC(econ.epochRevenue)}
                          </td>
                          <td className="px-4 py-3 tabular-nums text-right">
                            {formatUSDC(econ.mrr)}
                          </td>
                          <td className="px-4 py-3 tabular-nums text-right">
                            {formatUSDC(econ.arr, true)}
                          </td>
                        </tr>
                      </tfoot>
                    )}
                  </table>
                </div>
              </div>
            </section>
          </div>

          {/* Top Funders */}
          <section>
            <div className="flex flex-wrap items-baseline justify-between gap-1 mb-4">
              <SectionTitle className="">Top Funders</SectionTitle>
              {econ && (
                <span className="text-xs text-muted-foreground">
                  {econ.uniqueFunders} unique · avg{" "}
                  {econ.avgSeatsPerFunder.toFixed(1)} seats/funder
                </span>
              )}
            </div>
            <div className="relative border border-border rounded-lg overflow-hidden bg-card">
              {shimmerVisible && seatsData && (
                <div className="absolute inset-x-0 top-0 h-0.5 overflow-hidden z-10">
                  <div className="h-full w-1/3 bg-primary/60 animate-[shimmer_0.9s_ease-in-out_infinite] rounded-full" />
                </div>
              )}
              <div className="overflow-x-auto">
                <table className="min-w-160 w-full text-sm">
                  <thead>
                    <tr className="text-xs text-muted-foreground border-b border-border bg-muted/30">
                      <th className="px-4 py-3 text-left font-medium uppercase tracking-wider">
                        Funding Authority
                      </th>
                      <th className="px-4 py-3 text-right font-medium uppercase tracking-wider">
                        Seats
                      </th>
                      <th className="px-4 py-3 text-right font-medium uppercase tracking-wider">
                        USDC/Epoch
                      </th>
                      <th className="px-4 py-3 text-right font-medium uppercase tracking-wider">
                        MRR
                      </th>
                      <th className="px-4 py-3 text-right font-medium uppercase tracking-wider">
                        Escrow
                      </th>
                      <th className="px-4 py-3 text-right font-medium uppercase tracking-wider">
                        Min Runway
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {econ
                      ? econ.topFunders.map((f, i) => {
                          const runwayColor =
                            f.minRunwayEpochs === 0
                              ? "text-red-400"
                              : f.minRunwayEpochs === 1
                                ? "text-amber-400"
                                : "text-green-400";
                          return (
                            <tr
                              key={f.key}
                              className={`border-b border-border last:border-0 hover:bg-muted/20 transition-colors ${i % 2 === 0 ? "" : "bg-muted/10"}`}
                            >
                              <td className="px-4 py-3 font-mono text-xs text-muted-foreground">
                                <CopyableText text={f.key}>
                                  {truncatePK(f.key)}
                                </CopyableText>
                              </td>
                              <td className="px-4 py-3 tabular-nums text-right font-medium">
                                {f.seats}
                              </td>
                              <td className="px-4 py-3 tabular-nums text-right">
                                {formatUSDC(f.epochRevenue)}
                              </td>
                              <td className="px-4 py-3 tabular-nums text-right">
                                {formatUSDC(f.epochRevenue * EPOCHS_PER_MONTH)}
                              </td>
                              <td className="px-4 py-3 tabular-nums text-right">
                                {formatUSDC(f.escrow)}
                              </td>
                              <td
                                className={`px-4 py-3 tabular-nums text-right font-medium ${runwayColor}`}
                              >
                                {f.minRunwayEpochs >= 999
                                  ? "∞"
                                  : `${f.minRunwayEpochs} ep`}
                              </td>
                            </tr>
                          );
                        })
                      : Array.from({ length: 5 }).map((_, i) => (
                          <tr
                            key={i}
                            className="border-b border-border last:border-0"
                          >
                            <td className="px-4 py-3">
                              <Skeleton className="w-32" />
                            </td>
                            <td className="px-4 py-3 text-right">
                              <Skeleton className="w-6" />
                            </td>
                            <td className="px-4 py-3 text-right">
                              <Skeleton className="w-20" />
                            </td>
                            <td className="px-4 py-3 text-right">
                              <Skeleton className="w-20" />
                            </td>
                            <td className="px-4 py-3 text-right">
                              <Skeleton className="w-20" />
                            </td>
                            <td className="px-4 py-3" />
                          </tr>
                        ))}
                  </tbody>
                </table>
              </div>
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}
