import { useRef, useEffect } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";

interface TelemetryChartProps {
  readsData: number[];
  writesData: number[];
  resolvedTheme: "light" | "dark";
  title: string;
  description: string;
}

// Iterative max to avoid stack overflow from spread operator on 100k+ data
function maxValue(data: number[]): number {
  let max = 0;
  for (let i = 0; i < data.length; i++) {
    if (data[i] > max) max = data[i];
  }
  return max;
}

// Format Y-axis tick values (e.g. 20000 -> 20K)
function formatYLabel(val: number): string {
  if (val >= 1000) {
    const kVal = val / 1000;
    return `${kVal % 1 === 0 ? kVal : kVal.toFixed(1)}K`;
  }
  return val.toString();
}

// Resolve colors to match light/dark themes
function resolveThemeColors(resolvedTheme: "light" | "dark") {
  const isDark = resolvedTheme === "dark";
  return {
    reads: {
      stroke: isDark ? "#10b981" : "#059669",
      gradientStart: isDark
        ? "rgba(16, 185, 129, 0.15)"
        : "rgba(5, 150, 105, 0.12)",
      gradientEnd: "rgba(16, 185, 129, 0.0)",
    },
    writes: {
      // Changed from Rose/Red to Amber/Orange
      stroke: isDark ? "#f59e0b" : "#d97706", // amber-500 or amber-600
      gradientStart: isDark
        ? "rgba(245, 158, 11, 0.15)"
        : "rgba(217, 119, 6, 0.12)",
      gradientEnd: "rgba(245, 158, 11, 0.0)",
    },
  };
}

export default function TelemetryChart({
  readsData,
  writesData,
  resolvedTheme,
  title,
  description,
}: TelemetryChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const uplotDomRef = useRef<HTMLDivElement>(null);
  const uplotRef = useRef<uPlot | null>(null);

  // Store currentTime in a ref so uPlot's axis splits & hooks can access the latest time dynamically
  const currentTimeRef = useRef<number>(Date.now());
  currentTimeRef.current = Date.now();

  // Top-Right Legend Value Display Refs
  const legendReadsRef = useRef<HTMLSpanElement>(null);
  const legendWritesRef = useRef<HTMLSpanElement>(null);

  // Interactive overlays
  const tooltipRef = useRef<HTMLDivElement>(null);
  const tooltipTimeRef = useRef<HTMLDivElement>(null);
  const tooltipReadsValueRef = useRef<HTMLSpanElement>(null);
  const tooltipWritesValueRef = useRef<HTMLSpanElement>(null);

  const currentReadsVal = readsData[readsData.length - 1] ?? 0;
  const currentWritesVal = writesData[writesData.length - 1] ?? 0;

  // ── Mount / Remount uPlot on theme changes ────────────────────────────────
  useEffect(() => {
    if (!uplotDomRef.current || !containerRef.current) return;

    // Destroy any existing uPlot instance
    if (uplotRef.current) {
      uplotRef.current.destroy();
      uplotRef.current = null;
    }

    const dataMax = Math.max(maxValue(readsData), maxValue(writesData));
    const initialMaxVal = Math.max(
      10,
      dataMax <= 10
        ? 10
        : dataMax <= 50
          ? Math.ceil(dataMax / 10) * 10
          : Math.ceil(dataMax / 50) * 50,
    );

    const xVals = Array.from(
      { length: Math.max(2, readsData.length) },
      (_, i) => i,
    );
    const yReadsVals =
      readsData.length > 0
        ? readsData
        : Array.from({ length: xVals.length }, () => 0);
    const yWritesVals =
      writesData.length > 0
        ? writesData
        : Array.from({ length: xVals.length }, () => 0);

    const colors = resolveThemeColors(resolvedTheme);
    const isDark = resolvedTheme === "dark";
    const gridColor = isDark
      ? "rgba(255, 255, 255, 0.06)"
      : "rgba(0, 0, 0, 0.04)";
    const textColor = isDark ? "#71717a" : "#a1a1aa";

    // Setup uPlot dimensions matching the parent container
    const width = containerRef.current.clientWidth || 600;
    const height = containerRef.current.clientHeight || 224;

    const opts: uPlot.Options = {
      width,
      height,
      pxAlign: false,
      legend: { show: false },
      cursor: {
        show: true,
        x: true,
        y: false,
        points: {
          show: true,
          size: () => 6,
          fill: (_, seriesIdx) =>
            seriesIdx === 1 ? colors.reads.stroke : colors.writes.stroke,
          stroke: () => "#ffffff",
          width: () => 2,
        },
      },
      padding: [15, 12, 0, 12],
      scales: {
        x: { time: false, range: [0, Math.max(1, readsData.length - 1)] },
        y: { range: [0, initialMaxVal] },
      },
      // 100% Canvas native axes and grid lines
      axes: [
        {
          show: true,
          grid: {
            show: true,
            stroke: gridColor,
            width: 1,
            dash: [2, 4],
          },
          ticks: { show: false },
          font: "10px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
          stroke: textColor,
          size: 30,
          gap: 10,
          values: (self, splits) => {
            return splits.map((idx) => {
              const dataIdx = Math.round(idx);
              const secondsAgo = self.data[0].length - 1 - dataIdx;
              const timestampDate = new Date(
                currentTimeRef.current - secondsAgo * 1000,
              );
              return timestampDate.toLocaleTimeString(undefined, {
                hour: "2-digit",
                minute: "2-digit",
                second: "2-digit",
                hour12: false,
              });
            });
          },
        },
        {
          show: true,
          grid: {
            show: true,
            stroke: gridColor,
            width: 1,
            dash: [3, 3],
          },
          ticks: { show: false },
          font: "10px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
          stroke: textColor,
          size: 40,
          gap: 10,
          values: (_, splits) => splits.map((val) => formatYLabel(val)),
        },
      ],
      series: [
        {}, // X Series
        {
          // Reads Series
          stroke: colors.reads.stroke,
          width: 2.2,
          paths: uPlot.paths?.spline?.(), // <-- Enable spline curved line drawing
          fill: (self) => {
            const ctx = self.ctx;
            const gradient = ctx.createLinearGradient(0, 15, 0, height - 30);
            gradient.addColorStop(0, colors.reads.gradientStart);
            gradient.addColorStop(1, colors.reads.gradientEnd);
            return gradient;
          },
          points: { show: false },
        },
        {
          // Writes Series
          stroke: colors.writes.stroke,
          width: 2.2,
          paths: uPlot.paths?.spline?.(), // <-- Enable spline curved line drawing
          fill: (self) => {
            const ctx = self.ctx;
            const gradient = ctx.createLinearGradient(0, 15, 0, height - 30);
            gradient.addColorStop(0, colors.writes.gradientStart);
            gradient.addColorStop(1, colors.writes.gradientEnd);
            return gradient;
          },
          points: { show: false },
        },
      ],
      hooks: {
        setCursor: [
          (self) => {
            const idx = self.cursor.idx;
            if (
              idx === undefined ||
              idx === null ||
              idx < 0 ||
              idx >= self.data[0].length
            ) {
              if (tooltipRef.current) tooltipRef.current.style.display = "none";
              return;
            }

            const xVal = self.data[0][idx];
            const readsVal = self.data[1][idx] as number;
            const writesVal = self.data[2][idx] as number;

            const xCss = self.valToPos(xVal, "x");
            const yCss = self.cursor.top ?? self.height / 2;

            const leftPct = (xCss / self.width) * 100;
            const topPct = (yCss / self.height) * 100;

            // Dynamically update the top-right legend counts on mouse hover
            if (legendReadsRef.current) {
              legendReadsRef.current.textContent = readsVal.toLocaleString();
            }
            if (legendWritesRef.current) {
              legendWritesRef.current.textContent = writesVal.toLocaleString();
            }

            if (tooltipRef.current) {
              tooltipRef.current.style.display = "flex";
              tooltipRef.current.style.left = `${leftPct}%`;
              tooltipRef.current.style.top = `${topPct}%`;

              if (idx < self.data[0].length / 3) {
                tooltipRef.current.className =
                  "absolute z-10 pointer-events-none bg-zinc-900/95 dark:bg-zinc-950/95 text-white p-2.5 rounded-lg shadow-xl text-[10px] font-mono border border-zinc-800/80 flex flex-col gap-1 min-w-[110px] transition-all duration-75 translate-x-3 -translate-y-full mt-[-10px]";
              } else if (idx > (self.data[0].length * 2) / 3) {
                tooltipRef.current.className =
                  "absolute z-10 pointer-events-none bg-zinc-900/95 dark:bg-zinc-950/95 text-white p-2.5 rounded-lg shadow-xl text-[10px] font-mono border border-zinc-800/80 flex flex-col gap-1 min-w-[110px] transition-all duration-75 -translate-x-full ml-[-12px] -translate-y-full mt-[-10px]";
              } else {
                tooltipRef.current.className =
                  "absolute z-10 pointer-events-none bg-zinc-900/95 dark:bg-zinc-950/95 text-white p-2.5 rounded-lg shadow-xl text-[10px] font-mono border border-zinc-800/80 flex flex-col gap-1 min-w-[110px] transition-all duration-75 -translate-x-1/2 -translate-y-full mt-[-10px]";
              }
            }

            if (tooltipTimeRef.current) {
              const secondsAgo = self.data[0].length - 1 - idx;
              tooltipTimeRef.current.textContent =
                secondsAgo === 0 ? "Just now" : `${secondsAgo}s ago`;
            }
            if (tooltipReadsValueRef.current) {
              tooltipReadsValueRef.current.textContent = `${readsVal.toLocaleString()} rec/s`;
            }
            if (tooltipWritesValueRef.current) {
              tooltipWritesValueRef.current.textContent = `${writesVal.toLocaleString()} rec/s`;
            }
          },
        ],
      },
    };

    const uplotInstance = new uPlot(
      opts,
      [xVals, yReadsVals, yWritesVals],
      uplotDomRef.current,
    );
    uplotRef.current = uplotInstance;

    return () => {
      uplotInstance.destroy();
      uplotRef.current = null;
    };
  }, [resolvedTheme]);

  // ── Push real-time data updates ──────────────────────────────────────────
  useEffect(() => {
    const uplot = uplotRef.current;
    if (!uplot) return;

    const dataMax = Math.max(maxValue(readsData), maxValue(writesData));
    const updatedMaxVal = Math.max(
      10,
      dataMax <= 10
        ? 10
        : dataMax <= 50
          ? Math.ceil(dataMax / 10) * 10
          : Math.ceil(dataMax / 50) * 50,
    );

    const xVals = Array.from(
      { length: Math.max(2, readsData.length) },
      (_, i) => i,
    );
    const yReadsVals =
      readsData.length > 0
        ? readsData
        : Array.from({ length: xVals.length }, () => 0);
    const yWritesVals =
      writesData.length > 0
        ? writesData
        : Array.from({ length: xVals.length }, () => 0);

    // Stream the new dataset to the active canvas
    uplot.batch(() => {
      uplot.setData([xVals, yReadsVals, yWritesVals], false);
      uplot.setScale("x", { min: 0, max: Math.max(1, readsData.length - 1) });
      uplot.setScale("y", { min: 0, max: updatedMaxVal });
    });
  }, [readsData, writesData]);

  // ── Setup ResizeObserver to keep canvas high-DPI crisp and sharp ─────────
  useEffect(() => {
    if (!containerRef.current || !uplotRef.current) return;

    const resizeObserver = new ResizeObserver((entries) => {
      if (!entries || entries.length === 0) return;
      const { width, height } = entries[0].contentRect;
      if (width > 0 && height > 0) {
        uplotRef.current?.setSize({ width, height });
      }
    });

    resizeObserver.observe(containerRef.current);
    return () => resizeObserver.disconnect();
  }, []);

  return (
    <div className="bg-white dark:bg-zinc-900 border border-zinc-200/50 dark:border-zinc-800/50 p-6 rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.04)] relative transition-all duration-300">
      <style>{`
        .uplot-custom-container .uplot {
          position: absolute !important;
          left: 0 !important;
          top: 0 !important;
        }
      `}</style>

      {/* ── Header ─────────────────────────────────────────────────────── */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-6">
        <div>
          <h4 className="font-semibold text-zinc-900 dark:text-zinc-100 text-sm tracking-wide">
            {title}
          </h4>
          <p className="text-[11px] text-zinc-400 dark:text-zinc-500 mt-0.5">
            {description}
          </p>
        </div>

        {/* Dynamic Color-Coded Header Legend */}
        <div className="flex items-center gap-5 text-xs font-mono font-semibold select-none">
          <div className="flex items-center gap-2 px-2.5 py-1 rounded bg-zinc-50 dark:bg-zinc-800/30 border border-zinc-500/10">
            <span className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse shadow-[0_0_6px_rgba(16,185,129,0.5)]" />
            <span className="text-zinc-400 dark:text-zinc-500 font-medium tracking-wider text-[9px] uppercase">
              Reads/sec
            </span>
            <span
              ref={legendReadsRef}
              className="text-emerald-600 dark:text-emerald-400 font-bold font-mono"
            >
              {currentReadsVal.toLocaleString()}
            </span>
          </div>
          <div className="flex items-center gap-2 px-2.5 py-1 rounded bg-zinc-50 dark:bg-zinc-800/30 border border-zinc-500/10">
            <span className="w-2 h-2 rounded-full bg-amber-500 animate-pulse shadow-[0_0_6px_rgba(245,158,11,0.5)]" />
            <span className="text-zinc-400 dark:text-zinc-500 font-medium tracking-wider text-[9px] uppercase">
              Writes/sec
            </span>
            <span
              ref={legendWritesRef}
              className="text-amber-600 dark:text-amber-400 font-bold font-mono"
            >
              {currentWritesVal.toLocaleString()}
            </span>
          </div>
        </div>
      </div>

      {/* ── Chart Canvas ────────────────────────────────────────────────── */}
      <div
        ref={containerRef}
        className="h-56 w-full relative cursor-crosshair select-none uplot-custom-container"
        onMouseLeave={() => {
          if (uplotRef.current) {
            uplotRef.current.setCursor({ left: -10, top: -10 });
          }
          if (tooltipRef.current) tooltipRef.current.style.display = "none";

          // Restore original live counts when user leaves the canvas
          if (legendReadsRef.current) {
            legendReadsRef.current.textContent =
              currentReadsVal.toLocaleString();
          }
          if (legendWritesRef.current) {
            legendWritesRef.current.textContent =
              currentWritesVal.toLocaleString();
          }
        }}
      >
        {/* uPlot Canvas Mount Point */}
        <div ref={uplotDomRef} className="absolute inset-0 w-full h-full" />

        {/* Custom HTML Overlay Tooltip */}
        <div
          ref={tooltipRef}
          style={{ display: "none" }}
          className="absolute z-10 pointer-events-none  text-white p-2.5 rounded-lg shadow-xl text-[10px] font-mono border border-zinc-800/80 flex flex-col gap-1 min-w-[110px]"
        >
          <div ref={tooltipTimeRef} className="text-zinc-400 font-medium" />
          <div className="flex items-center justify-between gap-4 mt-0.5">
            <span className="flex items-center gap-1.5 font-semibold text-white">
              <span className="w-1.5 h-1.5 rounded-full bg-emerald-500" />
              Reads:
            </span>
            <span
              ref={tooltipReadsValueRef}
              className="font-bold text-emerald-400 dark:text-emerald-300"
            />
          </div>
          <div className="flex items-center justify-between gap-4">
            <span className="flex items-center gap-1.5 font-semibold text-white">
              <span className="w-1.5 h-1.5 rounded-full bg-amber-500" />
              Writes:
            </span>
            <span
              ref={tooltipWritesValueRef}
              className="font-bold text-amber-500 dark:text-amber-400"
            />
          </div>
        </div>
      </div>
    </div>
  );
}
