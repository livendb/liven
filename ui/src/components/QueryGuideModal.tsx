import { useState } from "react";
import { X, Check, Copy, Play } from "lucide-react";
import {
  fetchSamples,
  insertSamples,
  updateSamples,
  deleteSamples,
  upsertSamples,
  relationshipSamples,
} from "../constants/samples";
import CodeBlock from "./CodeBlock";

export interface QueryGuideModalProps {
  isOpen: boolean;
  onClose: () => void;
  setQuery: (query: string) => void;
  addActivity: (
    message: string,
    category: "storage" | "query" | "server" | "stream",
    type?: "info" | "success" | "warn" | "error",
  ) => void;
}

export default function QueryGuideModal({
  isOpen,
  onClose,
  setQuery,
  addActivity,
}: QueryGuideModalProps) {
  const [helpActiveTab, setHelpActiveTab] = useState<
    "fetch" | "insert" | "upsert" | "update" | "delete" | "relationship"
  >("fetch");
  const [copiedText, setCopiedText] = useState("");

  if (!isOpen) return null;

  const handleCopy = (text: string) => {
    navigator.clipboard.writeText(text);
    setCopiedText(text);
    setTimeout(() => setCopiedText(""), 2000);
  };

  const getActiveSamples = () => {
    switch (helpActiveTab) {
      case "fetch":
        return fetchSamples;
      case "insert":
        return insertSamples;
      case "upsert":
        return upsertSamples;
      case "update":
        return updateSamples;
      case "delete":
        return deleteSamples;
      case "relationship":
        return relationshipSamples;
      default:
        return fetchSamples;
    }
  };

  const getTabLabel = (tab: string) => {
    switch (tab) {
      case "fetch":
        return "Fetch";
      case "insert":
        return "Insert";
      case "upsert":
        return "Upsert";
      case "update":
        return "Update";
      case "delete":
        return "Delete";
      case "relationship":
        return "Relations";
      default:
        return tab;
    }
  };

  return (
    <div className="fixed inset-0 bg-zinc-50 dark:bg-zinc-900 flex flex-col z-[100] select-none animate-fade-in w-screen h-screen overflow-hidden">
      <div className="w-full max-w-6xl mx-auto flex-1 flex flex-col h-full overflow-hidden px-6 md:px-12 pt-6 pb-8">
        {/* Header */}
        <div className="flex items-center justify-between border-b border-slate-200 dark:border-zinc-800 pb-5 shrink-0">
          <div>
            <h4 className="font-bold text-slate-900 dark:text-white text-2xl flex items-center gap-2.5 tracking-tight">
              Query Guide
            </h4>
          </div>
          <button
            onClick={onClose}
            className="p-2 rounded text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-900 border border-slate-200 dark:border-zinc-800 transition-all hover:scale-105 active:scale-95 shadow-sm"
          >
            <X className="w-6 h-6" />
          </button>
        </div>

        {/* Tab buttons */}
        <div className="flex items-center gap-2 p-1.5 bg-body-bg/80 dark:bg-zinc-900 border border-slate-200/50 dark:border-zinc-800 rounded-md mt-5 shrink-0 w-fit">
          {(
            [
              "fetch",
              "insert",
              "upsert",
              "update",
              "delete",
              "relationship",
            ] as const
          ).map((tab) => (
            <button
              key={tab}
              onClick={() => setHelpActiveTab(tab)}
              className={`px-5 py-2 text-xs font-bold transition-all rounded-sm capitalize ${
                helpActiveTab === tab
                  ? "bg-zinc-600 dark:bg-zinc-800 text-gray-200 rounded-lg"
                  : "text-slate-500 dark:text-slate-400 hover:text-slate-800 dark:hover:text-slate-200"
              }`}
            >
              {getTabLabel(tab)}
            </button>
          ))}
        </div>

        {/* Tab content */}
        <div className="flex-1 overflow-y-auto pr-1 mt-6 ">
          <div className="space-y-4 animate-fade-in pb-12">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              {getActiveSamples().map((sample, idx) => (
                <div
                  key={idx}
                  className="p-6 rounded-md border border-slate-200/80 dark:border-zinc-800/80 bg-panel-bg/70 dark:bg-panel-bg/40 hover:border-primary/40 dark:hover:border-primary/30 hover:shadow-md hover:translate-y-[-2px] transition-all duration-300 flex flex-col justify-between space-y-4 group shadow-sm"
                >
                  <div className="space-y-2">
                    <div className="flex items-center justify-between gap-2">
                      <span className="font-bold text-slate-900 dark:text-slate-100 text-[15px] tracking-tight">
                        {sample.title}
                      </span>
                      {(sample as any).badge && (
                        <span className="text-[10px] font-extrabold uppercase tracking-widest px-2 py-0.5 rounded-sm bg-zinc-200 dark:bg-zinc-800 text-zinc-500 dark:text-zinc-400 shrink-0">
                          {(sample as any).badge}
                        </span>
                      )}
                    </div>
                    <p className="text-xs leading-relaxed text-slate-500 dark:text-slate-400 font-medium">
                      {sample.desc}
                    </p>
                  </div>

                  {/* IDE-style Code Block */}
                  <div className="space-y-3.5">
                    <CodeBlock language="javascript" code={sample.code} />

                    {/* Actions bar */}
                    <div className="flex items-center justify-end gap-3 pt-1">
                      <button
                        onClick={() => handleCopy(sample.code)}
                        className="flex items-center gap-1.5 px-3 py-1.5 rounded-sm text-xs font-semibold text-slate-500 hover:text-slate-800 dark:text-slate-400 dark:hover:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800/60 transition-all"
                      >
                        {copiedText === sample.code ? (
                          <>
                            <Check className="w-3.5 h-3.5 text-accent" />{" "}
                            <span className="text-accent font-bold">
                              Copied
                            </span>
                          </>
                        ) : (
                          <>
                            <Copy className="w-3.5 h-3.5" /> <span>Copy</span>
                          </>
                        )}
                      </button>
                      <span className="w-px h-3.5 bg-slate-200 dark:bg-slate-800" />
                      <button
                        onClick={() => {
                          setQuery(sample.code);
                          onClose();
                          addActivity(
                            `Loaded template query: ${sample.code}`,
                            "query",
                            "info",
                          );
                        }}
                        className="flex items-center gap-1.5 px-3.5 py-1.5 rounded-sm bg-primary text-white hover:bg-primary-hover transition-all text-xs font-bold shadow-sm hover:/25"
                      >
                        <Play className="w-3.5 h-3.5 fill-current" />
                        <span>Load Query</span>
                      </button>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="border-t border-slate-200 dark:border-zinc-800 pt-5 flex justify-end shrink-0">
          <button
            onClick={onClose}
            className="py-2.5 px-6 rounded bg-zinc-800 text-white text-xs font-bold transition-all active:scale-[0.98]"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}
