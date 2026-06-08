import React, { useState } from "react";
import CodeMirror from "@uiw/react-codemirror";
import {
  autocompletion,
  CompletionContext,
  CompletionResult,
} from "@codemirror/autocomplete";
import { StreamLanguage } from "@codemirror/language";
import {
  Terminal,
  HelpCircle,
  Play,
  XCircle,
  Search,
  CheckCircle2,
  RefreshCw,
  Trash2,
  Database,
  List,
  Table,
} from "lucide-react";
import { Record } from "../types";
import { getDbApiUrl } from "../utils/api";
import { gruvboxDark, atomOneLight } from "../utils/theme";
import RowExpand from "../components/RowExpand";
import PrettifiedGridView from "../components/PrettifiedGridView";

export interface QueryConsoleProps {
  query: string;
  setQuery: (query: string) => void;
  queryResults: Record[];
  setQueryResults: React.Dispatch<React.SetStateAction<Record[]>>;
  isQueryRunning: boolean;
  setIsQueryRunning: (running: boolean) => void;
  queryStats: { count: number; timeMs: number };
  setQueryStats: (stats: { count: number; timeMs: number }) => void;
  continuousStream: boolean;
  setContinuousStream: (continuous: boolean) => void;
  queryCurrentPage: number;
  setQueryCurrentPage: (page: number) => void;
  queryPageSize: number;
  setQueryPageSize: (size: number) => void;
  queryError: string;
  setQueryError: (error: string) => void;
  resolvedTheme: "light" | "dark";
  wsConnected: boolean;
  wsRef: React.MutableRefObject<WebSocket | null>;
  queriesThisSecondRef: React.MutableRefObject<number>;
  addActivity: (
    message: string,
    category: "storage" | "query" | "server" | "stream",
    type?: "info" | "success" | "warn" | "error",
  ) => void;
  setIsHelpOpen: (open: boolean) => void;
  streams: string[];
}

// Helper function to generate dynamic pagination numbers
const getPageNumbers = (currentPage: number, totalPages: number) => {
  const range: (number | string)[] = [];
  const delta = 1; // Number of pages to show on each side of active page

  if (totalPages <= 7) {
    for (let i = 1; i <= totalPages; i++) {
      range.push(i);
    }
    return range;
  }

  // Always show page 1
  range.push(1);

  // Calculate start and end of middle pages
  const start = Math.max(2, currentPage - delta);
  const end = Math.min(totalPages - 1, currentPage + delta);

  // If there's a gap between page 1 and start, add ellipsis or skipped pages
  if (start > 2) {
    if (start === 3) {
      range.push(2);
    } else {
      range.push("...");
    }
  }

  // Add middle pages
  for (let i = start; i <= end; i++) {
    range.push(i);
  }

  // If there's a gap between end and last page, add ellipsis or skipped pages
  if (end < totalPages - 1) {
    if (end === totalPages - 2) {
      range.push(totalPages - 1);
    } else {
      range.push("...");
    }
  }

  // Always show last page
  range.push(totalPages);

  return range;
};

const livenLanguage = StreamLanguage.define({
  token(stream) {
    if (stream.eatSpace()) return null;

    // Strings
    if (stream.match(/^"[^"]*"/)) return "string";
    if (stream.match(/^'[^']*'/)) return "string";

    // Numbers
    if (stream.match(/^\d+(?:\.\d+)?/)) return "number";

    // Dot-methods (e.g. .insert, .upsert) - className style
    if (stream.match(/^\.(?:insert|upsert|update|delete|empty)\b/))
      return "className";

    // Keywords
    if (stream.match(/^(?:from|drop|streams)\b/)) return "keyword";

    // Pipeline built-in functions
    if (
      stream.match(
        /^(?:filter|limit|get|map|count|group|sort|page|enrich|window)\b/,
      )
    )
      return "keyword";

    // Functions
    if (stream.match(/^(?:now|cursor)\b/)) return "keyword";

    // Operators
    if (
      stream.match(/^(?:and|or|not|in|contains|startsWith|endsWith|between)\b/)
    )
      return "operator";
    if (stream.match(/^(?:==|!=|>=|<=|>|<|=)/)) return "operator";
    if (stream.match(/^\|/)) return "operator";

    // Variables (unquoted names, e.g. amount, status)
    if (stream.match(/^[a-zA-Z_][a-zA-Z0-9_\-]*/)) return "variable";

    // Single character fallback
    stream.next();
    return null;
  },
});

function getQueryCompletions(streams: string[]) {
  return (context: CompletionContext): CompletionResult | null => {
    // 1. Detect if we are typing a stream name inside from("...") or drop("...")
    const before = context.state.doc.sliceString(
      Math.max(0, context.pos - 50),
      context.pos,
    );
    const matchStream = before.match(
      /(?:from|drop)\s*\(\s*["']([a-zA-Z0-9_\-]*)$/,
    );
    if (matchStream) {
      const typed = matchStream[1];
      return {
        from: context.pos - typed.length,
        options: streams.map((s) => ({
          label: s,
          type: "variable",
          detail: "Database Stream",
        })),
      };
    }

    // 2. Check if we are typing a terminal method (starts with .)
    const methodWord = context.matchBefore(/\.\w*/);
    if (methodWord) {
      return {
        from: methodWord.from,
        options: [
          {
            label: ".insert",
            apply: ".insert(",
            type: "function",
            detail: "Insert records",
          },
          {
            label: ".upsert",
            apply: ".upsert(",
            type: "function",
            detail: "Upsert records",
          },
          {
            label: ".update",
            apply: ".update(",
            type: "function",
            detail: "Update records",
          },
          {
            label: ".delete",
            apply: ".delete()",
            type: "function",
            detail: "Delete records",
          },
          {
            label: ".empty",
            apply: ".empty()",
            type: "function",
            detail: "Truncate stream",
          },
        ],
      };
    }

    // 3. Check if we are typing a pipeline operator (starts with |)
    const pipeWord = context.matchBefore(/\|\s*\w*/);
    if (pipeWord) {
      return {
        from: pipeWord.from,
        options: [
          {
            label: "| filter",
            apply: "| filter(",
            type: "function",
            detail: "Filter records",
          },
          {
            label: "| limit",
            apply: "| limit(",
            type: "function",
            detail: "Cap result count",
          },
          {
            label: "| get",
            apply: '| get("',
            type: "function",
            detail: "Lookup key O(1)",
          },
          {
            label: "| map",
            apply: "| map(",
            type: "function",
            detail: "Project fields",
          },
          {
            label: "| count",
            apply: "| count()",
            type: "function",
            detail: "Count matches",
          },
          {
            label: "| group",
            apply: "| group(",
            type: "function",
            detail: "Aggregations",
          },
          {
            label: "| sort",
            apply: "| sort(",
            type: "function",
            detail: "Sort results",
          },
          {
            label: "| page",
            apply: "| page(",
            type: "function",
            detail: "Offsetless paging",
          },
          {
            label: "| enrich",
            apply: '| enrich("',
            type: "function",
            detail: "Left-join streams",
          },
          {
            label: "| window",
            apply: "| window(",
            type: "function",
            detail: "Sliding window agg",
          },
        ],
      };
    }

    // 4. Default word completions
    const word = context.matchBefore(/\w+/);
    if (!word && !context.explicit) return null;

    const from = word ? word.from : context.pos;

    return {
      from,
      options: [
        {
          label: "from",
          apply: 'from("',
          type: "keyword",
          detail: 'from("stream")',
        },
        {
          label: "drop",
          apply: 'drop("',
          type: "keyword",
          detail: 'drop("stream")',
        },
        {
          label: "streams",
          apply: "streams()",
          type: "keyword",
          detail: "List active streams",
        },
        {
          label: "cursor",
          apply: 'cursor("',
          type: "function",
          detail: "Paging cursor",
        },
        {
          label: "now",
          apply: "now()",
          type: "function",
          detail: "Current timestamp",
        },
        { label: "and", apply: "and ", type: "keyword", detail: "Logical and" },
        { label: "or", apply: "or ", type: "keyword", detail: "Logical or" },
        { label: "not", apply: "not ", type: "keyword", detail: "Logical not" },
        {
          label: "in",
          apply: "in ",
          type: "keyword",
          detail: "Set membership",
        },
        {
          label: "contains",
          apply: "contains(",
          type: "function",
          detail: "Substring match",
        },
        {
          label: "startsWith",
          apply: 'startsWith "',
          type: "keyword",
          detail: "Prefix match",
        },
        {
          label: "endsWith",
          apply: 'endsWith "',
          type: "keyword",
          detail: "Suffix match",
        },
        {
          label: "between",
          apply: "between(",
          type: "function",
          detail: "Range filter",
        },
      ],
    };
  };
}

export default function QueryConsole({
  query,
  setQuery,
  queryResults,
  setQueryResults,
  isQueryRunning,
  setIsQueryRunning,
  queryStats,
  setQueryStats,
  continuousStream,
  setContinuousStream,
  queryCurrentPage,
  setQueryCurrentPage,
  queryPageSize,
  setQueryPageSize,
  queryError,
  setQueryError,
  resolvedTheme,
  wsConnected,
  wsRef,
  queriesThisSecondRef,
  addActivity,
  setIsHelpOpen,
  streams,
}: QueryConsoleProps) {
  const [viewMode, setViewMode] = useState<"list" | "table">("list");
  const [hasExecuted, setHasExecuted] = useState(false);

  // Setup CodeMirror autocomplete extension using streams prop
  const autocompleteExtension = React.useMemo(() => {
    return autocompletion({
      override: [getQueryCompletions(streams)],
    });
  }, [streams]);

  // Run Query
  const runQuery = async () => {
    if (!query.trim()) {
      setQueryError("Query expression cannot be empty.");
      return;
    }
    setQueryError("");
    setIsQueryRunning(true);
    setQueryResults([]);
    setQueryStats({ count: 0, timeMs: 0 });
    setQueryCurrentPage(1);
    setHasExecuted(false);

    const startTime = performance.now();
    queriesThisSecondRef.current++; // Register in query throughput metrics graph

    addActivity(
      `Initiating execution pipeline: "${query.substring(0, 45)}${query.length > 45 ? "..." : ""}"`,
      "query",
      "info",
    );

    if (continuousStream && wsRef.current && wsConnected) {
      // Stream over Websocket
      wsRef.current.send(JSON.stringify({ type: "query", query }));
      setIsQueryRunning(false); // Keeps socket listening, UI handles streaming
      setHasExecuted(true);
      addActivity(
        `Subscribed to real-time stream subscription for query: "${query}"`,
        "query",
        "info",
      );
    } else {
      // Query REST Endpoint (Historical Snapshot)
      try {
        const res = await fetch(getDbApiUrl("/api/query"), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ query }),
        });

        const elapsed = Math.round(performance.now() - startTime);

        if (res.ok) {
          const recs = await res.json();
          setQueryResults(recs);
          setQueryStats({ count: recs.length, timeMs: elapsed });
          setHasExecuted(true);

          // Detect operationType and compile detailed message
          const trimmedQuery = query.trim().toLowerCase();
          const opType = (() => {
            if (trimmedQuery.includes(".insert")) return "insert";
            if (trimmedQuery.includes(".upsert")) return "upsert";
            if (trimmedQuery.includes(".update")) return "update";
            if (
              trimmedQuery.includes(".delete") ||
              trimmedQuery.includes(".empty")
            )
              return "delete";
            if (trimmedQuery.startsWith("drop")) return "drop";
            return "fetch";
          })();

          const isMut =
            recs.length === 1 &&
            recs[0]?.key === "status" &&
            typeof recs[0]?.value === "string" &&
            recs[0]?.value.startsWith('{"status":');
          let affected = 0;
          if (isMut) {
            try {
              const data = JSON.parse(recs[0].value);
              affected = data.affected_rows || 0;
            } catch {}
          }

          let logMsg = `Pipeline query success: returned ${recs.length} matches in ${elapsed}ms`;
          if (opType === "insert" || opType === "upsert") {
            logMsg = `Write operation successful: appended ${recs.length} record(s) to stream in ${elapsed}ms`;
          } else if (isMut) {
            if (opType === "delete") {
              logMsg = `Mutation transaction committed: deleted ${affected} record(s) in ${elapsed}ms`;
            } else if (opType === "drop") {
              logMsg = `Schema mutation committed: dropped stream in ${elapsed}ms`;
            } else if (opType === "update") {
              logMsg = `Mutation transaction committed: updated ${affected} record(s) in ${elapsed}ms`;
            }
          }

          addActivity(logMsg, "query", "success");
        } else {
          const errText = await res.text();
          setQueryError(`Query Error: ${errText}`);
          addActivity(
            `Pipeline query compilation error: ${errText}`,
            "query",
            "error",
          );
        }
      } catch (e: any) {
        setQueryError(`Query Failed: ${e.message}`);
        addActivity(`Pipeline query failed: ${e.message}`, "query", "error");
      } finally {
        setIsQueryRunning(false);
      }
    }
  };

  const trimmedQuery = query.trim().toLowerCase();
  const operationType = (() => {
    if (trimmedQuery.includes(".insert")) return "insert";
    if (trimmedQuery.includes(".upsert")) return "upsert";
    if (trimmedQuery.includes(".update")) return "update";
    if (trimmedQuery.includes(".delete") || trimmedQuery.includes(".empty"))
      return "delete";
    if (trimmedQuery.startsWith("drop")) return "drop";
    return "fetch";
  })();

  const firstResult = queryResults[0];
  const isMutationStatus =
    queryResults.length === 1 &&
    firstResult?.key === "status" &&
    typeof firstResult?.value === "string" &&
    firstResult?.value.startsWith('{"status":');

  let mutationData: { status?: string; affected_rows?: number } = {};
  if (isMutationStatus) {
    try {
      mutationData = JSON.parse(firstResult.value as string);
    } catch (e) {
      // ignore
    }
  }

  const streamName = (() => {
    const q = query.trim();
    if (q.toLowerCase().startsWith("drop ")) {
      return q.substring(5).trim();
    }
    const match = q.match(/^([a-zA-Z0-9_\-]+)\./);
    if (match) {
      return match[1];
    }
    return "unknown";
  })();

  const totalCount = queryResults.length;
  const totalPages = Math.ceil(totalCount / queryPageSize);
  const startIndex = (queryCurrentPage - 1) * queryPageSize;
  const endIndex = Math.min(startIndex + queryPageSize, totalCount);
  const paginatedResults = queryResults.slice(startIndex, endIndex);

  return (
    <div className="space-y-6">
      {/* Input console */}
      <div className="bg-white dark:bg-zinc-900 p-6 rounded-md space-y-4">
        <div className="flex items-center justify-between">
          <div>
            <h4 className="font-semibold text-zinc-900 dark:text-white">
              Functional Pipeline Interface
            </h4>
            <p className="text-xs text-zinc-550 dark:text-zinc-400 mt-0.5">
              Input pipe expressions to extract streams
            </p>
          </div>

          <button
            onClick={() => setIsHelpOpen(true)}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-sm border border-zinc-200 dark:border-zinc-800 hover:bg-zinc-50 dark:hover:bg-zinc-900/60 text-zinc-600 dark:text-zinc-400 text-xs font-semibold transition-all active:scale-[0.98]"
          >
            <HelpCircle className="w-4 h-4 text-zinc-600 dark:text-zinc-400" />
            <span>Query Guide</span>
          </button>
        </div>

        <div className="border border-zinc-200 dark:border-zinc-800 rounded overflow-hidden text-sm">
          <CodeMirror
            value={query}
            height="120px"
            onChange={(val) => {
              setQuery(val);
              if (queryError) setQueryError("");
            }}
            theme={resolvedTheme === "dark" ? gruvboxDark : atomOneLight}
            basicSetup={{
              highlightActiveLine: false,
              highlightActiveLineGutter: false,
            }}
            extensions={[livenLanguage, autocompleteExtension]}
            className="font-mono text-sm"
          />
        </div>

        <div className="flex items-center justify-between pt-1">
          <div className="text-xs text-zinc-550 dark:text-zinc-400 flex items-center gap-2"></div>
          <div className="flex items-center gap-6">
            <label className="flex items-center gap-2 text-xs font-bold text-zinc-500 dark:text-zinc-400 cursor-pointer">
              <input
                type="checkbox"
                checked={continuousStream}
                onChange={(e) => setContinuousStream(e.target.checked)}
                className="w-4 h-4 rounded border-zinc-250 dark:border-zinc-800 bg-panel-bg checked:bg-primary focus:ring-0 cursor-pointer accent-primary"
              />
              Continuous Live Stream
            </label>

            <button
              onClick={runQuery}
              disabled={isQueryRunning}
              className="py-2 px-5 rounded bg-primary hover:bg-primary-hover text-white text-sm font-bold active:scale-[0.98] transition-all flex items-center gap-2"
            >
              <Play className="w-4 h-4" />
              {isQueryRunning ? "Running..." : "Run"}
            </button>
          </div>
        </div>

        {queryError && (
          <div className="mt-2.5 p-3.5 rounded bg-rose-500/10 border border-rose-500/20 text-rose-400 text-xs font-mono flex items-center gap-2 animate-fade-in">
            <XCircle className="w-4 h-4 shrink-0" />
            <span>{queryError}</span>
          </div>
        )}
      </div>

      {/* QUERY STATS AND RESULTS */}
      <div className="bg-white dark:bg-zinc-900 rounded-md overflow-hidden">
        <div className="px-6 py-4 border-b border-zinc-100 dark:border-zinc-800 bg-body-bg/50 dark:bg-panel-bg/30 flex items-center justify-between">
          <h4 className="font-semibold text-zinc-900 dark:text-white text-sm flex items-center gap-2">
            <Terminal className="w-4 h-4  text-zinc-600 dark:text-zinc-400" />
            Query Output
          </h4>
          {queryStats.count > 0 && (
            <div className="text-xs font-bold text-zinc-500 dark:text-zinc-400 flex items-center gap-4">
              <span>
                {isMutationStatus ? (
                  <>
                    Rows Mutated:{" "}
                    <strong className="text-rose-400">
                      {mutationData.affected_rows !== undefined
                        ? mutationData.affected_rows
                        : 0}
                    </strong>
                  </>
                ) : operationType === "insert" || operationType === "upsert" ? (
                  <>
                    Records Written:{" "}
                    <strong className="text-emerald-400">
                      {queryStats.count}
                    </strong>
                  </>
                ) : (
                  <>
                    Matches Count:{" "}
                    <strong className="text-zinc-900 dark:text-white">
                      {queryStats.count}
                    </strong>
                  </>
                )}
              </span>
              <span className="w-px h-3 bg-zinc-200 dark:bg-zinc-800" />
              <span>
                Compute Latency:{" "}
                <strong className="text-zinc-900 dark:text-white">
                  {queryStats.timeMs}ms
                </strong>
              </span>
            </div>
          )}
        </div>

        {queryResults.length === 0 ? (
          <div className="p-16 text-center text-zinc-500 dark:text-zinc-400 max-w-md mx-auto flex flex-col items-center justify-center animate-fade-in">
            <div className="p-4 rounded-full bg-zinc-50 dark:bg-zinc-800/30 border border-zinc-100 dark:border-zinc-800 text-zinc-400 dark:text-zinc-500 mb-4 shadow-inner">
              <Search className="w-8 h-8 opacity-75" />
            </div>
            <h5 className="text-sm font-semibold text-zinc-850 dark:text-zinc-200 mb-1">
              {hasExecuted
                ? "No Records Matched Query"
                : "Run Query and see result"}
            </h5>
          </div>
        ) : isMutationStatus ? (
          <div className="p-8 max-w-xl mx-auto my-6">
            <div className="relative bg-white dark:bg-zinc-900 rounded-md p-6 border-b-2 border-r-2 border-secondary/25 dark:border-secondary/15 bg-gradient-to-br from-panel-bg/80 to-body-bg/50 dark:from-panel-bg/90 dark:to-body-bg/80 /5 overflow-hidden animate-fade-in">
              {/* Neon top highlight glow */}
              <div className="absolute top-0 left-0 right-0 h-[2px] bg-gradient-to-r from-secondary via-primary to-secondary" />

              <div className="flex items-start gap-4">
                <div className="p-3 rounded bg-secondary/10 border border-secondary/20 text-secondary shadow-sm">
                  {operationType === "delete" ? (
                    <Trash2 className="w-6 h-6 animate-pulse" />
                  ) : operationType === "drop" ? (
                    <Database className="w-6 h-6 animate-pulse" />
                  ) : (
                    <RefreshCw className="w-6 h-6 animate-spin-slow" />
                  )}
                </div>

                <div className="flex-1 space-y-1">
                  <div className="flex items-center justify-between">
                    <span className="text-[10px]  tracking-wider font-extrabold px-2.5 py-0.5 rounded bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 flex items-center gap-1 -green/5">
                      <CheckCircle2 className="w-3.5 h-3.5" />
                      Transaction Committed
                    </span>
                    <span className="text-xs text-zinc-550 dark:text-zinc-400 font-mono">
                      Seq #{firstResult.sequence_id}
                    </span>
                  </div>

                  <h5 className="text-lg font-bold text-zinc-900 dark:text-white capitalize pt-1.5 flex items-center gap-1.5">
                    {operationType === "delete"
                      ? "Records Deleted"
                      : operationType === "drop"
                        ? "Stream Dropped"
                        : "Stream Updated"}
                  </h5>

                  <p className="text-xs text-zinc-550 dark:text-zinc-400 font-mono">
                    Query:{" "}
                    <span className="text-zinc-800 dark:text-zinc-200">
                      "
                      {query.length > 60
                        ? query.substring(0, 57) + "..."
                        : query}
                      "
                    </span>
                  </p>
                </div>
              </div>

              <div className="mt-6 pt-5 border-t border-zinc-200 dark:border-zinc-800/80 grid grid-cols-2 gap-4 text-xs font-mono">
                <div className="space-y-1">
                  <p className="text-[10px] text-zinc-450 dark:text-zinc-500 font-bold  tracking-wider">
                    Target Stream
                  </p>
                  <p className="font-bold text-secondary text-sm">
                    [{streamName}]
                  </p>
                </div>
                <div className="space-y-1">
                  <p className="text-[10px] text-zinc-450 dark:text-zinc-500 font-bold  tracking-wider">
                    Affected Records
                  </p>
                  <p className="font-bold text-zinc-800 dark:text-white text-sm">
                    {mutationData.affected_rows !== undefined
                      ? mutationData.affected_rows
                      : 0}{" "}
                    rows
                  </p>
                </div>
              </div>

              {/* Raw Payload Disclosure */}
              <div className="mt-6">
                <details className="group">
                  <summary className="flex items-center gap-1.5 text-xs text-zinc-550 dark:text-zinc-450 hover:text-zinc-800 dark:hover:text-zinc-200 font-bold cursor-pointer transition-colors outline-none select-none">
                    <span className="transition-transform group-open:rotate-90">
                      ▶
                    </span>
                    <span>View Raw Response Payload</span>
                  </summary>
                  <div className="mt-2.5 p-3.5 rounded bg-body-bg/50 dark:bg-body-bg/80 border border-zinc-200/50 dark:border-zinc-900 font-mono text-xs leading-relaxed text-zinc-600 dark:text-zinc-400">
                    {JSON.stringify(firstResult, null, 2)}
                  </div>
                </details>
              </div>
            </div>
          </div>
        ) : (
          <>
            {/* Top Total Count & Pagination Info summary */}
            <div className="px-6 py-3 border-b border-zinc-100 dark:border-zinc-800/80 bg-zinc-50/30 dark:bg-zinc-900/10 flex flex-col sm:flex-row sm:items-center justify-between gap-3 text-xs text-zinc-500 font-bold transition-colors">
              <div className="flex items-center gap-1">
                <span>Showing</span>
                <span className="font-bold text-zinc-700 dark:text-zinc-300">
                  {totalCount > 0 ? startIndex + 1 : 0}
                </span>
                <span>to</span>
                <span className="font-bold text-zinc-700 dark:text-zinc-300">
                  {endIndex}
                </span>
                <span>of</span>
                <span className="font-bold text-zinc-700 dark:text-zinc-300">
                  {totalCount}
                </span>
                <span>total matching records</span>
              </div>

              <div className="flex items-center gap-4">
                {/* View Switcher Segmented Control */}
                <div className="flex items-center gap-1.5 border border-zinc-200 dark:border-zinc-800/80 bg-white/40 dark:bg-zinc-950/20 p-1 rounded-sm shrink-0">
                  <button
                    onClick={() => setViewMode("list")}
                    className={`px-2.5 py-1.5 rounded-xs flex items-center gap-1.5 text-[11px] transition-all font-bold cursor-pointer ${
                      viewMode === "list"
                        ? "bg-primary text-white"
                        : "text-zinc-450 hover:text-zinc-800 dark:hover:text-zinc-200"
                    }`}
                    title="List View"
                  >
                    <List className="w-3.5 h-3.5" />
                    <span>List</span>
                  </button>
                  <button
                    onClick={() => setViewMode("table")}
                    className={`px-2.5 py-1.5 rounded-xs flex items-center gap-1.5 text-[11px] transition-all font-bold cursor-pointer ${
                      viewMode === "table"
                        ? "bg-primary text-white"
                        : "text-zinc-450 hover:text-zinc-800 dark:hover:text-zinc-200 "
                    }`}
                    title="Table View"
                  >
                    <Table className="w-3.5 h-3.5" />
                    <span>Table</span>
                  </button>
                </div>

                <div className="w-px h-4 bg-zinc-200 dark:bg-zinc-800 shrink-0" />

                <div className="flex items-center gap-2">
                  <span className="text-zinc-400 font-medium">Per Page:</span>
                  <select
                    value={queryPageSize}
                    onChange={(e) => {
                      setQueryPageSize(Number(e.target.value));
                      setQueryCurrentPage(1);
                    }}
                    className="bg-transparent text-zinc-700 dark:text-zinc-300 border border-zinc-200 dark:border-zinc-800 rounded-sm px-2.5 py-1 outline-none font-bold cursor-pointer hover:border-zinc-300 dark:hover:border-zinc-700 transition-colors focus:border-primary"
                  >
                    <option value={10}>10 records</option>
                    <option value={25}>25 records</option>
                    <option value={50}>50 records</option>
                    <option value={100}>100 records</option>
                    <option value={200}>200 records</option>
                    <option value={500}>500 records</option>
                  </select>
                </div>
              </div>
            </div>

            {viewMode === "list" ? (
              /* Paginated list */
              <div className="divide-y divide-zinc-100 dark:divide-zinc-800/60 font-mono text-xs">
                {paginatedResults.map((rec) => (
                  <RowExpand
                    key={rec.sequence_id}
                    record={rec}
                    resolvedTheme={resolvedTheme}
                  />
                ))}
              </div>
            ) : (
              /* Paginated Table (Prettified dynamic grid view) */
              <PrettifiedGridView
                records={paginatedResults}
                resolvedTheme={resolvedTheme}
              />
            )}

            {/* Pagination Controls */}
            {totalPages > 1 && (
              <div className="px-6 py-4 border-t border-zinc-100 dark:border-zinc-800 bg-zinc-50/20 dark:bg-zinc-900/5 flex items-center justify-between">
                <button
                  disabled={queryCurrentPage === 1}
                  onClick={() =>
                    setQueryCurrentPage(Math.max(1, queryCurrentPage - 1))
                  }
                  className="px-3 py-1.5 rounded border-zinc-200 dark:border-zinc-800 bg-zinc-200 hover:bg-zinc-50 dark:hover:bg-zinc-900 text-zinc-700 dark:text-zinc-400 text-xs font-bold disabled:opacity-40 disabled:cursor-not-allowed transition-all"
                >
                  Previous
                </button>

                <div className="flex items-center gap-1.5">
                  {getPageNumbers(queryCurrentPage, totalPages).map(
                    (p, idx) => {
                      if (p === "...") {
                        return (
                          <span
                            key={`dots-${idx}`}
                            className="w-8 h-8 flex items-center justify-center text-xs font-medium text-zinc-400 dark:text-zinc-600 select-none cursor-default"
                          >
                            ...
                          </span>
                        );
                      }
                      const pageNum = p as number;
                      const isActive = pageNum === queryCurrentPage;
                      return (
                        <button
                          key={pageNum}
                          onClick={() => setQueryCurrentPage(pageNum)}
                          className={`w-8 h-8 rounded flex items-center justify-center text-xs font-bold transition-all ${
                            isActive
                              ? "bg-primary/15 text-primary  border-zinc-200 dark:border-zinc-800 bg-zinc-200 hover:bg-zinc-50 dark:hover:bg-zinc-900 text-zinc-600 dark:text-zinc-400"
                              : "text-zinc-500 dark:text-zinc-400 hover:text-zinc-800 dark:hover:text-zinc-200 hover:bg-zinc-100 dark:hover:bg-zinc-900/60 border border-transparent"
                          }`}
                        >
                          {pageNum}
                        </button>
                      );
                    },
                  )}
                </div>

                <button
                  disabled={queryCurrentPage === totalPages}
                  onClick={() =>
                    setQueryCurrentPage(
                      Math.min(totalPages, queryCurrentPage + 1),
                    )
                  }
                  className="px-3 py-1.5 rounded border-zinc-200 dark:border-zinc-800 bg-zinc-200 hover:bg-zinc-50 dark:hover:bg-zinc-900 text-zinc-700 dark:text-zinc-400 text-xs font-bold disabled:opacity-40 disabled:cursor-not-allowed transition-all"
                >
                  Next
                </button>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
