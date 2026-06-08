import { useState } from "react";
import { ChevronDown, ChevronRight } from "lucide-react";
import { Record } from "../types";
import { parseStringifiedJson } from "../utils/api";
import hljs from "highlight.js";

export interface TableExpandRowProps {
  record: Record;
  resolvedTheme: "light" | "dark";
}

export default function TableExpandRow({ record }: TableExpandRowProps) {
  const [isOpen, setIsOpen] = useState(false);
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

  // Clean, ISO-standard UTC timestamp representation for the Timestamp column
  const dateStr = new Date(record.timestamp).toISOString();

  return (
    <>
      {/* Primary Table Row */}
      <tr
        onClick={() => setIsOpen(!isOpen)}
        className="transition-all hover:bg-zinc-100/50 dark:hover:bg-zinc-800/10 border-b border-zinc-200/40 dark:border-zinc-800/30 cursor-pointer text-xs font-mono select-none"
      >
        {/* Toggle Indicator Column */}
        <td className="py-2 px-4 text-center w-12 shrink-0">
          <button className="text-zinc-400 hover:text-zinc-600 dark:hover:text-zinc-200 p-0.5 shrink-0 transition-colors">
            {isOpen ? (
              <ChevronDown className="w-4 h-4" />
            ) : (
              <ChevronRight className="w-4 h-4" />
            )}
          </button>
        </td>

        {/* Sequence ID Column */}
        <td className="py-2 px-4 text-zinc-800 dark:text-zinc-200 font-bold tabular-nums tracking-tight w-24">
          #{record.sequence_id}
        </td>

        {/* Stream Name Column */}
        <td
          className="py-2 px-4 text-zinc-500 dark:text-zinc-400 font-bold  tracking-wider text-[11px] w-32 truncate max-w-[120px]"
          title={record.stream_name}
        >
          {record.stream_name}
        </td>

        {/* Key Column */}
        <td
          className="py-2 px-4 text-zinc-900 dark:text-zinc-100 font-medium truncate max-w-[160px]"
          title={record.key}
        >
          {record.key}
        </td>

        {/* Value Payload Column */}
        <td className="py-2 px-4 text-zinc-450 dark:text-zinc-500 truncate max-w-[400px]">
          {record.flags & 0x02 ? (
            <span className="text-rose-500 font-bold">TOMBSTONE</span>
          ) : (
            formattedValue.replace(/\s+/g, " ").trim().substring(0, 80) +
            (formattedValue.length > 80 ? "..." : "")
          )}
        </td>

        {/* Timestamp Column */}
        <td className="py-2 px-4 text-zinc-500 dark:text-zinc-400 font-medium tracking-tight tabular-nums w-48 shrink-0">
          {dateStr}
        </td>
      </tr>

      {/* Expanded Table Row */}
      {isOpen && (
        <tr className="bg-zinc-50/20 dark:bg-zinc-950/20 border-b border-zinc-200/40 dark:border-zinc-800/30">
          <td colSpan={6} className="py-4 px-6">
            <div className="space-y-4 text-left ">
              {/* Collapsible Accordion: Engine Metrics / System View (hidden by default) */}
              <div className="rounded-lg border border-zinc-200/60 dark:border-zinc-800/40 bg-panel-bg dark:bg-panel-bg overflow-hidden shadow-[0_1px_3px_rgba(0,0,0,0.02)] ">
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
                  <span className="text-[10px] font-bold text-primary hover:underline">
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
                            record.flags & 0x02
                              ? "bg-rose-500"
                              : "bg-emerald-500"
                          }`}
                        />
                        0x0{record.flags} (
                        {record.flags & 0x02 ? "Tombstoned" : "Active"})
                      </span>
                    </div>
                  </div>
                )}
              </div>

              {/* Pretty Raw Payload Preview */}
              <div className="space-y-1.5">
                <span className="block text-[9px] text-zinc-400 dark:text-zinc-500 font-bold  tracking-wider pl-1">
                  Raw Payload Document
                </span>
                <div className="relative font-mono text-xs">
                  <pre className="overflow-x-auto p-4 bg-body-bg dark:bg-body-bg rounded-xl border border-zinc-900/[0.04] dark:border-zinc-800/50 shadow-sm font-mono text-xs tabular-nums tracking-normal max-h-96 hljs">
                    <code
                      dangerouslySetInnerHTML={{ __html: highlightedHtml }}
                    />
                  </pre>
                </div>
              </div>
            </div>
          </td>
        </tr>
      )}
    </>
  );
}
