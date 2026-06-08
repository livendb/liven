import { useState } from "react";
import { ChevronDown, ChevronRight } from "lucide-react";
import { Record } from "../types";
import { parseStringifiedJson } from "../utils/api";
import hljs from "highlight.js";

export interface RowExpandProps {
  record: Record;
  resolvedTheme: "light" | "dark";
}

export default function RowExpand({ record }: RowExpandProps) {
  const [isOpen, setIsOpen] = useState(false);
  // Controls the state of the collapsible internal engine metrics layout (hidden by default)
  const [isMetricsOpen, setIsMetricsOpen] = useState(false);

  let formattedValue = "";

  try {
    const parsedValue = parseStringifiedJson(record.value);
    formattedValue = JSON.stringify(parsedValue, null, 2);
  } catch {
    formattedValue = String(record.value);
  }

  // Generate gorgeous syntax-highlighted HTML for raw payload visual scan
  const highlightedHtml = (() => {
    try {
      return hljs.highlight(formattedValue, { language: "json" }).value;
    } catch {
      return formattedValue;
    }
  })();

  // Clean, ISO-standard UTC timestamp representation for the Center Block
  const dateStr = new Date(record.timestamp).toISOString();

  return (
    <div className="transition-all hover:bg-zinc-100/50 dark:hover:bg-zinc-800/10 border-b border-zinc-200/40 dark:border-zinc-800/30 last:border-b-0">
      {/* Primary Feed Layout (Uncollapsed Row View) */}
      <div
        className="px-6 py-3.5 flex items-center justify-between cursor-pointer text-xs font-mono select-none"
        onClick={() => setIsOpen(!isOpen)}
      >
        {/* Left Block: Namespace badge and 32-byte key/string identifier */}
        <div className="flex items-center gap-3 w-1/3 min-w-[200px] truncate">
          <span className="text-zinc-500 dark:text-zinc-400 font-mono text-[11px] font-semibold shrink-0  tracking-wider">
            Key:
          </span>
          <span className="text-zinc-900 dark:text-zinc-100 font-medium truncate ">
            {record.key}
          </span>
        </div>

        {/* Center Block: ISO-standard UTC timestamp representation */}
        <div className="text-center text-zinc-500 dark:text-zinc-400 w-1/4 shrink-0 font-mono text-[11px] tracking-tight tabular-nums">
          {dateStr}
        </div>

        {/* Right Block: Inline, truncated one-line view of the raw payload value & chevron toggle */}
        <div className="flex items-center justify-end gap-3 w-5/12 min-w-[250px] truncate text-right">
          <span className="text-zinc-400 dark:text-zinc-500 truncate text-[11px] font-medium font-mono">
            {record.flags & 0x02
              ? "TOMBSTONE"
              : formattedValue.replace(/\s+/g, " ").trim().substring(0, 50) +
                (formattedValue.length > 50 ? "..." : "")}
          </span>
          <button className="text-zinc-400 hover:text-zinc-600 dark:hover:text-zinc-200 p-0.5 shrink-0 transition-colors">
            {isOpen ? (
              <ChevronDown className="w-4 h-4" />
            ) : (
              <ChevronRight className="w-4 h-4" />
            )}
          </button>
        </div>
      </div>

      {/* Expanded Row View */}
      {isOpen && (
        <div className="px-6 pb-5 pt-3 border-t border-zinc-200/40 dark:border-zinc-800/30 bg-zinc-50/20 dark:bg-zinc-950/20 space-y-4">
          {/* Collapsible Accordion: Engine Metrics / System View (hidden by default) */}
          <div className="rounded-lg border border-zinc-200/60 dark:border-zinc-800/40 bg-zinc-100 dark:bg-zinc-900 overflow-hidden shadow-[0_1px_3px_rgba(0,0,0,0.02)]">
            <button
              onClick={(e) => {
                e.stopPropagation(); // Prevent row toggling when toggling accordion
                setIsMetricsOpen(!isMetricsOpen);
              }}
              className="w-full px-4 py-2.5 flex items-center justify-between text-left text-zinc-700 dark:text-zinc-300 hover:bg-zinc-100 dark:hover:bg-zinc-800/50 transition-colors border-b border-zinc-200/40 dark:border-zinc-800/30 outline-none"
            >
              <span className="text-[10px] font-bold  tracking-wider flex items-center gap-2">
                <span className="w-1.5 h-1.5 rounded-full bg-zinc-400 dark:bg-zinc-500" />
                Engine Metrics / System View
              </span>
              <span className="text-[10px] font-bold text-primary dark:text-emerald-500 hover:underline">
                {isMetricsOpen ? "Hide Metrics" : "Show Metrics"}
              </span>
            </button>

            {isMetricsOpen && (
              <div className="p-4 grid grid-cols-2 md:grid-cols-4 gap-4 text-xs font-mono bg-zinc-50/40 dark:bg-zinc-950/40 animate-fade-in border-t border-zinc-200/40 dark:border-zinc-800/30">
                <div className="space-y-1">
                  <span className="block text-[9px] text-zinc-400 dark:text-zinc-500 font-bold  tracking-wider">
                    Key
                  </span>
                  <span className="font-semibold text-zinc-800 dark:text-zinc-200">
                    {record.key}
                  </span>
                </div>

                {/* Monotonic sequence (#Seq) */}
                <div className="space-y-1">
                  <span className="block text-[9px] text-zinc-400 dark:text-zinc-500 font-bold  tracking-wider">
                    Monotonic Sequence
                  </span>
                  <span className="font-semibold text-zinc-800 dark:text-zinc-200">
                    Seq #{record.sequence_id}
                  </span>
                </div>
                {/* Physical Segment ID */}
                <div className="space-y-1">
                  <span className="block text-[9px] text-zinc-400 dark:text-zinc-500 font-bold  tracking-wider">
                    Physical Segment ID
                  </span>
                  <span className="font-semibold text-zinc-800 dark:text-zinc-200">
                    Segment #{Math.ceil(record.sequence_id / 100000) || 1}
                  </span>
                </div>

                {/* Record Flags */}
                <div className="space-y-1">
                  <span className="block text-[9px] text-zinc-400 dark:text-zinc-500 font-bold  tracking-wider">
                    Record Flags
                  </span>
                  <span className="inline-flex items-center gap-1.5 font-semibold text-zinc-800 dark:text-zinc-200">
                    <span
                      className={`w-1.5 h-1.5 rounded-full ${
                        record.flags & 0x02 ? "bg-rose-500" : "bg-emerald-500"
                      }`}
                    />
                    0x0{record.flags} (
                    {record.flags & 0x02 ? "Tombstoned" : "Active"})
                  </span>
                </div>
              </div>
            )}
          </div>

          {/* Pretty Raw Payload Preview (Always Visible when Row is Open) */}
          <div className="space-y-1.5">
            <span className="block text-[9px] text-zinc-400 dark:text-zinc-500 font-bold  tracking-wider pl-1">
              Raw Payload Document
            </span>
            <div className="relative">
              <pre className="overflow-x-auto p-4 bg-body-bg dark:bg-zinc-900 rounded-xl border border-zinc-900/[0.04] dark:border-zinc-800/50 shadow-sm font-mono text-xs tabular-nums tracking-normal max-h-96 hljs">
                <code dangerouslySetInnerHTML={{ __html: highlightedHtml }} />
              </pre>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
