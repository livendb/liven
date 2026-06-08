import { Record } from "../types";
import { parseStringifiedJson } from "../utils/api";

export interface PrettifiedGridViewProps {
  records: Record[];
  resolvedTheme: "light" | "dark";
}

export default function PrettifiedGridView({
  records,
}: PrettifiedGridViewProps) {
  // 1. Map and parse all records
  const parsedRecords = records.map((rec) => {
    let parsed: any = null;
    let isObject = false;

    if (!(rec.flags & 0x02)) {
      try {
        parsed = parseStringifiedJson(rec.value);
        isObject =
          typeof parsed === "object" &&
          parsed !== null &&
          !Array.isArray(parsed);
      } catch {
        parsed = rec.value;
      }
    }

    return {
      record: rec,
      parsed,
      isObject,
    };
  });

  // 2. Extract unique keys from all records that parsed as JSON objects
  const dynamicKeysSet = new Set<string>();
  parsedRecords.forEach((item) => {
    if (item.isObject) {
      Object.keys(item.parsed).forEach((k) => {
        dynamicKeysSet.add(k);
      });
    }
  });

  const dynamicColumns = Array.from(dynamicKeysSet).sort();

  // 3. Render Cell Helper
  const renderCellContent = (
    item: (typeof parsedRecords)[0],
    colKey: string,
  ) => {
    if (item.record.flags & 0x02) {
      return (
        <span className="text-rose-500 font-bold select-none text-[10px]  tracking-wider">
          &lt;Tombstone&gt;
        </span>
      );
    }

    if (!item.isObject) {
      // If the payload is a primitive, only render it under the fallback/value column, otherwise hyphen
      if (colKey === "__raw_value__") {
        return String(item.parsed);
      }
      return (
        <span className="text-zinc-350 dark:text-zinc-700 font-medium select-none">
          -
        </span>
      );
    }

    const value = item.parsed[colKey];
    if (value === undefined) {
      return (
        <span className="text-zinc-350 dark:text-zinc-700 font-medium select-none">
          -
        </span>
      );
    }

    if (value === null) {
      return (
        <span className="text-zinc-400 dark:text-zinc-500 italic select-none font-bold">
          null
        </span>
      );
    }

    if (typeof value === "boolean") {
      return (
        <span
          className={`font-mono text-[10px]  font-bold px-1.5 py-0.5 rounded ${
            value
              ? "bg-zinc-100 dark:bg-zinc-850 text-zinc-700 dark:text-zinc-300"
              : "bg-zinc-100 dark:bg-zinc-850 text-zinc-400 dark:text-zinc-500"
          }`}
        >
          {value ? "true" : "false"}
        </span>
      );
    }

    if (typeof value === "number") {
      return (
        <span className="text-zinc-800 dark:text-zinc-200 font-bold tabular-nums">
          {value}
        </span>
      );
    }

    if (typeof value === "object") {
      const stringified = JSON.stringify(value);
      return (
        <div
          className="bg-zinc-50 dark:bg-zinc-800/40 rounded p-1.5 text-[11px] text-zinc-600 dark:text-zinc-300 truncate max-w-[200px]"
          title={stringified}
        >
          {stringified}
        </div>
      );
    }

    // Default: string
    const strVal = String(value);
    return (
      <span
        className="text-zinc-700 dark:text-zinc-300 font-medium"
        title={strVal}
      >
        {strVal.length > 60 ? strVal.substring(0, 57) + "..." : strVal}
      </span>
    );
  };

  return (
    <>
      {/* 1. DESKTOP WIDE GRID VIEW */}
      <div className="hidden md:block overflow-x-auto border border-zinc-200/60 dark:border-zinc-800 bg-zinc-50/20 dark:bg-zinc-950/20">
        <table className="w-full text-left border-collapse min-w-[800px] table-auto">
          <thead>
            <tr className="border-b border-zinc-200/60 dark:border-zinc-800/40 text-zinc-500 dark:text-zinc-400  text-[10px] tracking-wider font-extrabold bg-zinc-50/50 dark:bg-zinc-800 select-none sticky top-0 z-10">
              <th className="py-2 px-4 border-r border-zinc-200/40 dark:border-zinc-800/30 w-24">
                Seq #
              </th>
              <th className="py-2 px-4 border-r border-zinc-200/40 dark:border-zinc-800/30 w-32">
                Stream
              </th>
              <th className="py-2 px-4 border-r border-zinc-200/40 dark:border-zinc-800/30 w-40">
                Key
              </th>

              {/* Inferred Payload Columns */}
              {dynamicColumns.length > 0 ? (
                dynamicColumns.map((col) => (
                  <th
                    key={col}
                    className="py-2 px-4 border-r border-zinc-200/40 dark:border-zinc-800/30 text-zinc-500 dark:text-zinc-400"
                  >
                    {col}
                  </th>
                ))
              ) : (
                <th className="py-2 px-4 border-r border-zinc-200/40 dark:border-zinc-800/30 text-zinc-500 dark:text-zinc-400">
                  Value / Payload
                </th>
              )}

              <th className="py-2 px-4 w-48">Timestamp</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-100 dark:divide-zinc-800/50 font-mono text-xs">
            {parsedRecords.map((item) => {
              const dateStr = new Date(item.record.timestamp).toISOString();
              return (
                <tr
                  key={item.record.sequence_id}
                  className="hover:bg-zinc-50/50 dark:hover:bg-zinc-800/10 transition-colors duration-150 group"
                >
                  {/* Seq # */}
                  <td className="py-2 px-4 border-r border-zinc-200/40 dark:border-zinc-800/30 text-zinc-400 group-hover:text-zinc-700 dark:group-hover:text-zinc-350 font-bold select-none tabular-nums">
                    #{item.record.sequence_id}
                  </td>

                  {/* Stream */}
                  <td
                    className="py-2 px-4 border-r border-zinc-200/40 dark:border-zinc-800/30 text-zinc-500 dark:text-zinc-400 font-bold  tracking-wider text-[11px] truncate max-w-[120px]"
                    title={item.record.stream_name}
                  >
                    {item.record.stream_name}
                  </td>

                  {/* Key */}
                  <td
                    className="py-2 px-4 border-r border-zinc-200/40 dark:border-zinc-800/30 text-zinc-900 dark:text-zinc-100 font-medium truncate max-w-[160px]"
                    title={item.record.key}
                  >
                    {item.record.key}
                  </td>

                  {/* Dynamic Columns Values */}
                  {dynamicColumns.length > 0 ? (
                    dynamicColumns.map((col) => (
                      <td
                        key={col}
                        className="py-2 px-4 border-r border-zinc-200/40 dark:border-zinc-800/30 max-w-[300px] truncate"
                      >
                        {renderCellContent(item, col)}
                      </td>
                    ))
                  ) : (
                    <td className="py-2 px-4 border-r border-zinc-200/40 dark:border-zinc-800/30 max-w-[400px] truncate">
                      {renderCellContent(item, "__raw_value__")}
                    </td>
                  )}

                  {/* Timestamp */}
                  <td className="py-2 px-4 text-zinc-500 dark:text-zinc-400 font-medium select-none tracking-tight tabular-nums">
                    {dateStr}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* 2. MOBILE RESPONSIVE CARD STACK VIEW */}
      <div className="block md:hidden space-y-3">
        {parsedRecords.map((item) => {
          const dateStr = new Date(item.record.timestamp).toISOString();
          const isTombstone = !!(item.record.flags & 0x02);

          return (
            <div
              key={item.record.sequence_id}
              className="bg-zinc-400 dark:bg-zinc-800 border border-zinc-200/60 dark:border-zinc-800/40 rounded-xl p-4  space-y-3  text-xs"
            >
              {/* Header */}
              <div className="flex items-center justify-between border-b border-zinc-200/40 dark:border-zinc-800/30 pb-2">
                <span className="font-mono font-bold text-zinc-900 dark:text-zinc-100 tabular-nums">
                  #{item.record.sequence_id}
                </span>
                <span className="font-mono text-[10px] font-bold text-zinc-500 dark:text-zinc-400  tracking-wider">
                  {item.record.stream_name}
                </span>
              </div>

              {/* Core Details */}
              <div className="grid grid-cols-2 gap-2 text-xs">
                <div>
                  <span className="block text-[9px] text-zinc-400 dark:text-zinc-500 font-bold  tracking-wider">
                    Key
                  </span>
                  <span className="font-mono font-medium text-zinc-900 dark:text-zinc-100 truncate block">
                    {item.record.key}
                  </span>
                </div>
                <div className="text-right">
                  <span className="block text-[9px] text-zinc-400 dark:text-zinc-500 font-bold  tracking-wider">
                    Timestamp
                  </span>
                  <span className="font-mono text-[10px] text-zinc-500 dark:text-zinc-400 block tracking-tight tabular-nums">
                    {dateStr.split("T")[1].substring(0, 8)}
                  </span>
                </div>
              </div>

              {/* Values / Payload Area */}
              <div className="pt-2 border-t border-zinc-200/40 dark:border-zinc-800/30 space-y-1">
                <span className="block text-[9px] text-zinc-400 dark:text-zinc-500 font-bold  tracking-wider mb-1">
                  Payload Fields
                </span>

                {isTombstone ? (
                  <div className="text-rose-500 font-bold font-mono text-[10px]  tracking-wider">
                    Tombstone
                  </div>
                ) : !item.isObject ? (
                  <div className="bg-zinc-50 dark:bg-zinc-800/40 rounded p-2 font-mono text-[11px] text-zinc-600 dark:text-zinc-300 break-all">
                    {String(item.parsed)}
                  </div>
                ) : (
                  <div className="space-y-1">
                    {dynamicColumns.map((col) => {
                      const val = item.parsed[col];
                      if (val === undefined) return null;
                      return (
                        <div
                          key={col}
                          className="flex items-start justify-between gap-4 py-1 border-b border-zinc-100/50 dark:border-zinc-800/10 last:border-b-0"
                        >
                          <span className="font-mono text-[10px] font-bold text-zinc-400 dark:text-zinc-500">
                            {col}
                          </span>
                          <span className="text-right font-mono max-w-[150px] truncate text-zinc-700 dark:text-zinc-300">
                            {renderCellContent(item, col)}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </>
  );
}
