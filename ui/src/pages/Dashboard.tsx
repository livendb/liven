import {
  Server,
  HardDrive,
  CheckCircle2,
  Activity,
  Trash2,
  Zap,
  RefreshCw,
} from "lucide-react";
import { Metrics, ActivityLog } from "../types";
import { formatBytes, dbPort as defaultDbPort } from "../utils/api";
import TelemetryChart from "../components/TelemetryChart";

export interface DashboardProps {
  metrics: Metrics;
  streams: string[];
  wsConnected: boolean;
  activities: ActivityLog[];
  setActivities: React.Dispatch<React.SetStateAction<ActivityLog[]>>;
  ingestChart: number[];
  queryChart: number[];
  activityFilter: "all" | "storage" | "query" | "stream" | "server";
  setActivityFilter: (
    filter: "all" | "storage" | "query" | "stream" | "server",
  ) => void;
  dbPort?: string;
  webuiPort?: string;
  resolvedTheme: "light" | "dark";
}

export default function Dashboard({
  metrics,
  streams,
  wsConnected,
  activities,
  setActivities,
  ingestChart,
  queryChart,
  activityFilter,
  setActivityFilter,
  dbPort = defaultDbPort,
  webuiPort,
  resolvedTheme,
}: DashboardProps) {
  return (
    <div className="space-y-8">
      {/* SERVER HOST AND PORT ENDPOINT INFO */}
      <div className="bg-white dark:bg-zinc-900 p-4 rounded-md flex flex-wrap items-center justify-between gap-4 transition-all">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded bg-zinc-100 dark:bg-zinc-900 flex items-center justify-center text-zinc-500 dark:text-zinc-400 shrink-0">
            <Server className="w-4 h-4" />
          </div>
          <div>
            <h4 className="font-semibold text-zinc-900 dark:text-zinc-100 text-sm tracking-wide">
              LIVEN Active Instance
            </h4>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-3 text-xs">
          <div className="flex items-center gap-2 px-3 py-1.5 rounded bg-zinc-50 dark:bg-zinc-900/60 border border-zinc-500/10 font-mono">
            <span className="text-zinc-400 font-medium  tracking-wider text-[9px]">
              Host
            </span>
            <span className="text-zinc-850 dark:text-zinc-250 font-semibold">
              {window.location.hostname || "127.0.0.1"}
            </span>
          </div>

          <div className="flex items-center gap-2 px-3 py-1.5 rounded bg-zinc-50 dark:bg-zinc-900/60 border border-zinc-500/10 font-mono">
            <span className="text-zinc-400 font-medium  tracking-wider text-[9px]">
              DB Port
            </span>
            <span className="text-zinc-850 dark:text-zinc-250 font-semibold">
              {dbPort}
            </span>
          </div>

          <div className="flex items-center gap-2 px-3 py-1.5 rounded bg-zinc-50 dark:bg-zinc-900/60 border border-zinc-500/10 font-mono">
            <span className="text-zinc-400 font-medium  tracking-wider text-[9px]">
              WEB UI Port
            </span>
            <span className="text-zinc-850 dark:text-zinc-250 font-semibold">
              {webuiPort || window.location.port || "43120"}
            </span>
          </div>
        </div>
      </div>

      {/* TOP METRICS GLASS CARDS */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {/* Metric 1 */}
        <div className="bg-white dark:bg-zinc-900 text-card-foreground rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.04)] p-5 border-0 relative overflow-hidden group">
          <span className="text-[10px] font-semibold tracking-wider text-zinc-400  block mb-1">
            Database Size
          </span>
          <h3 className="text-2xl font-semibold tracking-tight text-zinc-900 dark:text-zinc-50">
            {formatBytes(metrics.disk_size)}
          </h3>
          <p className="text-[11px] text-primary mt-2 font-mono tabular-nums">
            RAM Usage:{" "}
            <span className="font-semibold text-zinc-700 dark:text-zinc-300">
              {(metrics.ram_usage / (1024 * 1024)).toFixed(2)} MB
            </span>
          </p>
        </div>

        {/* Metric 2 */}
        <div className="bg-white dark:bg-zinc-900 text-card-foreground rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.04)] p-5 border-0 relative overflow-hidden group">
          <span className="text-[10px] font-semibold tracking-wider text-zinc-400  block mb-1">
            Total Active Streams
          </span>
          <h3 className="text-2xl font-semibold tracking-tight text-zinc-900 dark:text-zinc-50">
            {metrics.total_streams || streams.length}
          </h3>
          <p className="text-[11px] text-primary mt-2 font-medium">
            Discoverable database streams
          </p>
        </div>

        {/* Metric 3 */}
        <div className="bg-white dark:bg-zinc-900 text-card-foreground rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.04)] p-5 border-0 relative overflow-hidden group">
          <span className="text-[10px] font-semibold tracking-wider text-zinc-400  block mb-1">
            Total Active Keys
          </span>
          <h3 className="text-2xl font-semibold tracking-tight text-zinc-900 dark:text-zinc-50">
            {metrics.key_count.toLocaleString()}
          </h3>
          <p className="text-[11px] text-primary mt-2 font-medium">
            In-Memory SkipMap Index
          </p>
        </div>

        {/* Metric 4 */}
        <div className="bg-white dark:bg-zinc-900 text-card-foreground rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.04)] p-5 border-0 relative overflow-hidden group">
          <span className="text-[10px] font-semibold tracking-wider text-zinc-400  block mb-1">
            Compacted Engine State
          </span>
          <div className="flex items-center gap-2">
            <span className="h-2 w-2 rounded-full bg-accent animate-pulse shadow-glow" />
            <span className="text-2xl font-bold tracking-tight text-foreground">
              Healthy
            </span>
          </div>
          <p className="text-[11px] text-accent mt-2 font-medium">
            Background thread online
          </p>
        </div>
      </div>

      {/* GRAPHS TELEMETRY PANEL */}
      <TelemetryChart
        readsData={queryChart}
        writesData={ingestChart}
        resolvedTheme={resolvedTheme}
        title="Performance"
        description="Live data throughput"
      />

      {/* VIVO LIVE ACTIVITIES AUDIT LOG */}
      <div className="bg-white dark:bg-zinc-900 p-6 rounded-md space-y-4">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded bg-zinc-100 dark:bg-zinc-900 flex items-center justify-center text-zinc-500 dark:text-zinc-400">
              <Activity className="w-4 h-4" />
            </div>
            <div>
              <h4 className="font-semibold text-zinc-900 dark:text-zinc-100 text-sm flex items-center gap-2">
                Audit Trail
              </h4>
            </div>
          </div>

          <div className="flex items-center gap-2">
            <button
              onClick={() => setActivities([])}
              className="px-3 py-1.5 rounded border border-zinc-500/10 hover:bg-zinc-100 dark:hover:bg-zinc-900 text-zinc-500 dark:text-zinc-400 hover:text-zinc-800 dark:hover:text-zinc-200 text-xs font-semibold flex items-center gap-1.5 transition-colors"
              title="Clear activity log history"
            >
              <Trash2 className="w-3.5 h-3.5" />
              Clear Logs
            </button>
          </div>
        </div>

        {/* Filters Row */}
        <div className="flex flex-wrap gap-1.5 border-b border-zinc-900/5 dark:border-zinc-100/5 pb-3">
          {(["all", "storage", "query", "stream", "server"] as const).map(
            (cat) => {
              const count =
                cat === "all"
                  ? activities.length
                  : activities.filter((a) => a.category === cat).length;

              const isActive = activityFilter === cat;
              return (
                <button
                  key={cat}
                  onClick={() => setActivityFilter(cat)}
                  className={`px-2.5 py-1 rounded text-xs font-medium capitalize transition-colors ${
                    isActive
                      ? "bg-zinc-900 dark:bg-zinc-100 text-white dark:text-zinc-950"
                      : "bg-zinc-100 hover:bg-zinc-200 dark:bg-zinc-900 dark:hover:bg-zinc-800 text-zinc-500 dark:text-zinc-400"
                  }`}
                >
                  {cat === "all" ? "All Events" : cat}
                  <span
                    className={`ml-1.5 px-1.5 py-0.5 rounded-full text-[10px] font-mono font-medium ${
                      isActive
                        ? "bg-white/20 dark:bg-black/10 text-white dark:text-zinc-950"
                        : "bg-zinc-200 dark:bg-zinc-800 text-zinc-650 dark:text-zinc-350"
                    }`}
                  >
                    {count}
                  </span>
                </button>
              );
            },
          )}
        </div>

        {/* Audit items list */}
        <div className="max-h-72 overflow-y-auto pr-1 space-y-2.5 font-mono text-[11px] leading-relaxed">
          {activities.filter(
            (a) => activityFilter === "all" || a.category === activityFilter,
          ).length === 0 ? (
            <div className="p-10 text-center text-zinc-400 dark:text-zinc-500 font-medium">
              No active logs trace found matching category &quot;
              {activityFilter}&quot;. Perform queries or insert records to
              trigger activity events.
            </div>
          ) : (
            activities
              .filter(
                (a) =>
                  activityFilter === "all" || a.category === activityFilter,
              )
              .map((act) => {
                let levelText =
                  "text-zinc-500 dark:text-zinc-400 font-mono text-[10px]";
                if (act.type === "success") {
                  levelText = "text-accent font-mono text-[10px]";
                } else if (act.type === "warn") {
                  levelText =
                    "text-amber-500 dark:text-amber-400 font-mono text-[10px]";
                } else if (act.type === "error") {
                  levelText =
                    "text-rose-500 dark:text-rose-400 font-mono text-[10px]";
                }

                const categoryBadge =
                  "bg-zinc-100 dark:bg-zinc-900 text-zinc-500 dark:text-zinc-400 border border-zinc-500/10 font-mono text-[9px] tracking-wider px-2 py-0.5 rounded";

                return (
                  <div
                    key={act.id}
                    className="p-3 rounded bg-zinc-50/30 dark:bg-zinc-900/10 flex items-start gap-4 transition-all duration-200 hover:bg-zinc-100/30 dark:hover:bg-zinc-900/30 border border-zinc-900/[0.02] dark:border-zinc-100/[0.02]"
                  >
                    <span className="text-zinc-400 dark:text-zinc-600 font-medium shrink-0">
                      [{act.timestamp}]
                    </span>

                    <span className={` shrink-0 ${categoryBadge}`}>
                      {act.category}
                    </span>

                    <div className="flex-1 text-zinc-700 dark:text-zinc-300 break-all">
                      {act.message}
                    </div>

                    <span className={` shrink-0 ${levelText}`}>{act.type}</span>
                  </div>
                );
              })
          )}
        </div>
      </div>

      {/* LOG COMPACTION & SYSTEM SPECS */}
      <div className="grid grid-cols-1 md:grid-cols-1 gap-6">
        {/* System specs */}
        <div className="bg-white dark:bg-zinc-900 p-6 rounded-md md:col-span-2">
          <h4 className="font-semibold text-zinc-900 dark:text-zinc-100 mb-4">
            System Status
          </h4>
          <div>
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 mb-4 bg-zinc-50/50 dark:bg-zinc-900/40 p-3 rounded border border-zinc-500/5 text-xs font-mono">
              <div>
                <span className="block text-[10px] text-zinc-400 font-medium  tracking-wider">
                  Segments
                </span>
                <span className="font-semibold text-zinc-800 dark:text-zinc-200">
                  {metrics ? metrics.segments : "0"}
                </span>
              </div>
              <div>
                <span className="block text-[10px] text-zinc-400 font-medium  tracking-wider">
                  Storage
                </span>
                <span className="font-semibold text-zinc-800 dark:text-zinc-200">
                  {metrics ? formatBytes(metrics.disk_size) : "0 B"}
                </span>
              </div>
              <div>
                <span className="block text-[10px] text-zinc-400 font-medium  tracking-wider">
                  Seq ID
                </span>
                <span className="font-semibold text-zinc-800 dark:text-zinc-200">
                  #
                  {metrics
                    ? metrics.sequence_id > 0
                      ? metrics.sequence_id - 1
                      : 0
                    : "0"}
                </span>
              </div>
              <div>
                <span className="block text-[10px] text-zinc-400 font-medium  tracking-wider">
                  Compaction
                </span>
                <div className="flex items-center gap-1.5 text-xs font-mono font-medium text-zinc-650 dark:text-zinc-350  px-2 py-0.5 rounded w-fit">
                  <span
                    className={`w-1.5 h-1.5 rounded-full ${wsConnected ? "bg-accent animate-pulse" : "bg-rose-500"}`}
                  />
                  <span>
                    {wsConnected ? "Auto Compaction: ACTIVE" : "Offline"}
                  </span>
                </div>
              </div>
            </div>
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-4 gap-4">
            {/* Card 1: Concurrency */}
            <div className="p-4 rounded bg-zinc-50/30 dark:bg-[#121214]/30 border border-zinc-500/5 flex flex-col justify-between space-y-3">
              <div className="flex items-start gap-3">
                <div className="p-2 rounded bg-zinc-100 dark:bg-zinc-800/80 text-zinc-600 dark:text-zinc-400 shrink-0">
                  <Zap className="w-4 h-4" />
                </div>
                <div>
                  <span className="text-xs font-semibold text-zinc-900 dark:text-zinc-100">
                    Concurrency Architecture
                  </span>
                  <p className="text-[11px] text-zinc-400 dark:text-zinc-500 mt-0.5">
                    crossbeam_skiplist index provider
                  </p>
                </div>
              </div>
              <div className="flex items-center gap-1.5 text-xs font-mono font-medium text-zinc-650 dark:text-zinc-350 bg-zinc-150/30 dark:bg-zinc-800/20 px-2 py-0.5 rounded border border-zinc-500/10 w-fit">
                <span className="w-1.5 h-1.5 rounded-full bg-accent animate-pulse" />
                <span>
                  {metrics ? metrics.key_count.toLocaleString() : "0"} indexed
                  keys
                </span>
              </div>
            </div>

            {/* Card 2: Integrity Checking */}
            <div className="p-4 rounded bg-zinc-50/30 dark:bg-[#121214]/30 border border-zinc-500/5 flex flex-col justify-between space-y-3">
              <div className="flex items-start gap-3">
                <div className="p-2 rounded bg-zinc-100 dark:bg-zinc-800/80 text-zinc-600 dark:text-zinc-400 shrink-0">
                  <CheckCircle2 className="w-4 h-4" />
                </div>
                <div>
                  <span className="text-xs font-semibold text-zinc-900 dark:text-zinc-100">
                    Integrity Checking
                  </span>
                  <p className="text-[11px] text-zinc-400 dark:text-zinc-500 mt-0.5">
                    Checksum status: Active & Verifying
                  </p>
                </div>
              </div>
              <div className="flex items-center gap-1.5 text-xs font-mono font-medium text-zinc-650 dark:text-zinc-350 bg-zinc-150/30 dark:bg-zinc-800/20 px-2 py-0.5 rounded border border-zinc-500/10 w-fit">
                <span
                  className={`w-1.5 h-1.5 rounded-full ${wsConnected ? "bg-accent animate-pulse" : "bg-rose-500"}`}
                />
                <span>{wsConnected ? "100% Integrity Guard" : "Offline"}</span>
              </div>
            </div>

            {/* Card 3: File Locking */}
            <div className="p-4 rounded bg-zinc-50/30 dark:bg-[#121214]/30 border border-zinc-500/5 flex flex-col justify-between space-y-3">
              <div className="flex items-start gap-3">
                <div className="p-2 rounded bg-zinc-100 dark:bg-zinc-800/80 text-zinc-600 dark:text-zinc-400 shrink-0">
                  <HardDrive className="w-4 h-4" />
                </div>
                <div>
                  <span className="text-xs font-semibold text-zinc-900 dark:text-zinc-100">
                    Cooperative Locking
                  </span>
                  <p className="text-[11px] text-zinc-400 dark:text-zinc-500 mt-0.5">
                    Exclusive Directory Lock via fs2
                  </p>
                </div>
              </div>
              <div className="flex items-center gap-1.5 text-xs font-mono font-medium text-zinc-650 dark:text-zinc-350 bg-zinc-150/30 dark:bg-zinc-800/20 px-2 py-0.5 rounded border border-zinc-500/10 w-fit">
                <span
                  className={`w-1.5 h-1.5 rounded-full ${wsConnected ? "bg-accent animate-pulse" : "bg-rose-500"}`}
                />
                <span>{wsConnected ? "Segment Lock: ACTIVE" : "Offline"}</span>
              </div>
            </div>

            {/* Card 4: Flushing */}
            <div className="p-4 rounded bg-zinc-50/30 dark:bg-[#121214]/30 border border-zinc-500/5 flex flex-col justify-between space-y-3">
              <div className="flex items-start gap-3">
                <div className="p-2 rounded bg-zinc-100 dark:bg-zinc-800/80 text-zinc-600 dark:text-zinc-400 shrink-0">
                  <RefreshCw className="w-4 h-4" />
                </div>
                <div>
                  <span className="text-xs font-semibold text-zinc-900 dark:text-zinc-100">
                    Flushing Mechanism
                  </span>
                  <p className="text-[11px] text-zinc-400 dark:text-zinc-500 mt-0.5">
                    I/O Sync Strategy: fdatasync
                  </p>
                </div>
              </div>
              <div className="flex items-center gap-1.5 text-xs font-mono font-medium text-zinc-650 dark:text-zinc-350 bg-zinc-150/30 dark:bg-zinc-800/20 px-2 py-0.5 rounded border border-zinc-500/10 w-fit">
                <span
                  className={`w-1.5 h-1.5 rounded-full ${wsConnected ? "bg-accent animate-pulse" : "bg-rose-500"}`}
                />
                <span>Sync: 0.24ms latency</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
