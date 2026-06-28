import React, { useState, useEffect } from "react";
import CodeMirror from "@uiw/react-codemirror";
import { json } from "@codemirror/lang-json";
import {
  Database,
  Plus,
  Search,
  ChevronRight,
  XCircle,
  Download,
  ChevronDown,
  FileText,
  CheckCircle2,
  Upload,
  Sliders,
  Loader2,
  Trash2,
  AlertTriangle,
  List,
  Table,
} from "lucide-react";
import { Record } from "../types";
import { getDbApiUrl } from "../utils/api";
import { gruvboxDark, atomOneLight } from "../utils/theme";
import RowExpand from "../components/RowExpand";
import PrettifiedGridView from "../components/PrettifiedGridView";

export interface StreamExplorerProps {
  streams: string[];
  selectedStream: string;
  setSelectedStream: (stream: string) => void;
  browsedRecords: Record[];
  setBrowsedRecords: React.Dispatch<React.SetStateAction<Record[]>>;
  browsedCurrentPage: number;
  setBrowsedCurrentPage: (page: number) => void;
  browsedPageSize: number;
  setBrowsedPageSize: (size: number) => void;
  loadStreamRecords: () => Promise<void>;
  fetchStreams: () => Promise<void>;
  addActivity: (
    message: string,
    category: "storage" | "query" | "server" | "stream",
    type?: "info" | "success" | "warn" | "error",
  ) => void;
  resolvedTheme: "light" | "dark";
  userRole?: string | null;
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

export default function StreamExplorer({
  streams,
  selectedStream,
  setSelectedStream,
  browsedRecords,
  setBrowsedRecords,
  browsedCurrentPage,
  setBrowsedCurrentPage,
  browsedPageSize,
  setBrowsedPageSize,
  loadStreamRecords,
  fetchStreams,
  addActivity,
  resolvedTheme,
  userRole,
}: StreamExplorerProps) {
  const [viewMode, setViewMode] = useState<"list" | "table">("list");

  // Filter out the active security auth_keys stream to prevent deletion/modification
  const filteredStreams = streams.filter((stream) => stream !== "auth_keys");

  // Modal states for bulk import
  const [isImportingOpen, setIsImportingOpen] = useState(false);
  const [importFileContent, setImportFileContent] = useState("");
  const [importError, setImportError] = useState("");
  const [importSuccess, setImportSuccess] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  // Export states
  const [isExportDropdownOpen, setIsExportDropdownOpen] = useState(false);
  const [isExporting, setIsExporting] = useState(false);

  // Delete stream states
  const [isDeleteModalOpen, setIsDeleteModalOpen] = useState(false);
  const [deleteConfirmText, setDeleteConfirmText] = useState("");
  const [isDeleting, setIsDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState("");

  const handleDeleteStream = async () => {
    if (deleteConfirmText !== selectedStream) return;
    setIsDeleting(true);
    setDeleteError("");

    addActivity(
      `Executing drop stream query for stream "${selectedStream}"`,
      "stream",
      "info",
    );

    try {
      const res = await fetch(getDbApiUrl("/api/query"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query: `drop("${selectedStream}")` }),
      });

      if (!res.ok) {
        const errMsg = await res.text();
        throw new Error(errMsg);
      }

      addActivity(
        `Stream "${selectedStream}" has been dropped completely.`,
        "stream",
        "success",
      );

      // Reset selection and fetch updated list
      setSelectedStream("");
      setBrowsedRecords([]);
      await fetchStreams();

      setIsDeleteModalOpen(false);
      setDeleteConfirmText("");
    } catch (e: any) {
      setDeleteError(`Deletion failed: ${e.message}`);
      addActivity(
        `Failed to drop stream "${selectedStream}": ${e.message}`,
        "stream",
        "error",
      );
    } finally {
      setIsDeleting(false);
    }
  };

  // Dynamic pre-validation for bulk importer
  useEffect(() => {
    if (!importFileContent.trim()) {
      setImportError("");
      return;
    }
    const lines = importFileContent.split("\n");
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line) continue;
      try {
        JSON.parse(line);
      } catch (err: any) {
        setImportError(`JSONL Syntax Error (Line ${i + 1}): ${err.message}`);
        return;
      }
    }
    setImportError("");
  }, [importFileContent]);

  // Handle uploaded text files
  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (evt) => {
      const text = evt.target?.result as string;
      setImportFileContent(text || "");
      setImportSuccess("");
    };
    reader.onerror = () => {
      setImportError("Failed to read file.");
    };
    reader.readAsText(file);
  };

  // Bulk Ingestion Ingester
  const handleBulkImport = async (e: React.FormEvent) => {
    e.preventDefault();
    if (importError) return;
    if (!importFileContent.trim()) {
      setImportError("Import payload cannot be empty.");
      return;
    }
    setIsSubmitting(true);
    setImportSuccess("");

    addActivity(
      `Parsing and importing bulk mass-ingestion payload into stream "${selectedStream}"`,
      "storage",
      "info",
    );

    try {
      const batch: any[] = [];

      const lines = importFileContent.split("\n");
      for (let i = 0; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;
        try {
          const parsed = JSON.parse(line);
          batch.push({
            stream: parsed.stream || selectedStream,
            key: parsed.key || `gen_${i}_${Date.now()}`,
            value: parsed.value !== undefined ? parsed.value : parsed,
          });
        } catch {
          throw new Error(`Line ${i + 1} contains invalid JSON.`);
        }
      }

      if (batch.length === 0) {
        throw new Error("No records detected in payload.");
      }

      const res = await fetch(getDbApiUrl("/api/ingest"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(batch),
      });

      if (res.ok) {
        setImportSuccess(
          `Successfully ingested ${batch.length} records into stream "${selectedStream}"! fdatasync flushed.`,
        );
        addActivity(
          `Bulk mass-ingestion completed: successfully ingested ${batch.length} records into stream "${selectedStream}".`,
          "storage",
          "success",
        );
        setImportFileContent("");
        fetchStreams();
        loadStreamRecords();
        // Graceful automatic modal closing
        setTimeout(() => {
          setIsImportingOpen(false);
          setImportSuccess("");
        }, 1500);
      } else {
        const err = await res.text();
        setImportError(`Ingestion server rejected batch: ${err}`);
        addActivity(
          `Bulk mass-ingestion rejected by server: ${err}`,
          "storage",
          "error",
        );
      }
    } catch (e: any) {
      setImportError(e.message);
      addActivity(`Bulk ingestion failed: ${e.message}`, "storage", "error");
    } finally {
      setIsSubmitting(false);
    }
  };

  // Stream Export Handler — JSONL only
  const handleStreamExport = async () => {
    if (!selectedStream) return;
    setIsExporting(true);
    setIsExportDropdownOpen(false);

    addActivity(
      `Preparing JSONL export for stream "${selectedStream}"`,
      "query",
      "info",
    );

    try {
      const queryStr = `from("${selectedStream}") | export(jsonl)`;

      const res = await fetch(getDbApiUrl("/api/query"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query: queryStr }),
      });

      if (!res.ok) {
        const err = await res.text();
        throw new Error(err);
      }

      const body = await res.text();
      const blob = new Blob([body], { type: "text/plain;charset=utf-8;" });
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.setAttribute(
        "download",
        `${selectedStream}_export_${Date.now()}.jsonl`,
      );
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);

      addActivity(
        `Successfully exported stream "${selectedStream}" as JSONL`,
        "query",
        "success",
      );
    } catch (e: any) {
      alert(`Export failed: ${e.message}`);
      addActivity(
        `Failed to export stream "${selectedStream}": ${e.message}`,
        "query",
        "error",
      );
    } finally {
      setIsExporting(false);
    }
  };

  const totalCount = browsedRecords.length;
  const totalPages = Math.ceil(totalCount / browsedPageSize);
  const startIndex = (browsedCurrentPage - 1) * browsedPageSize;
  const endIndex = Math.min(startIndex + browsedPageSize, totalCount);
  const paginatedRecords = browsedRecords.slice(startIndex, endIndex);

  return (
    <div className="grid grid-cols-1 lg:grid-cols-4 gap-8">
      {/* STREAMS LIST */}
      <div className="lg:col-span-1 space-y-4">
        <div className="bg-white dark:bg-zinc-900 p-4 rounded-md flex flex-col justify-between">
          <div className="flex items-center justify-between mb-4 pb-2 border-b border-zinc-100 dark:border-zinc-800">
            <h4 className="font-bold text-zinc-900 dark:text-white text-sm  tracking-wider">
              Streams
            </h4>
            {userRole && userRole !== "read-only" && (
              <button
                onClick={() => {
                  setImportFileContent("");
                  setImportSuccess("");
                  setImportError("");
                  setIsImportingOpen(true);
                }}
                className="p-1.5 rounded bg-primary/10 hover:bg-primary/20 text-accent transition-colors"
                title="Import Stream Data"
              >
                <Plus className="w-4 h-4" />
              </button>
            )}
          </div>

          {filteredStreams.length === 0 ? (
            <div className="text-center py-6 text-zinc-500 font-medium text-xs">
              No active streams.
            </div>
          ) : (
            <div className="space-y-1">
              {filteredStreams.map((stream) => (
                <button
                  key={stream}
                  onClick={() => setSelectedStream(stream)}
                  className={`w-full text-left px-3 py-2 rounded text-sm font-bold transition-all flex items-center justify-between ${
                    selectedStream === stream
                      ? "bg-primary/10 text-primary"
                      : "text-zinc-500 dark:text-zinc-400 hover:bg-zinc-100 dark:hover:bg-zinc-700 hover:text-zinc-850 dark:hover:text-zinc-200"
                  }`}
                >
                  <span className="truncate">{stream}</span>
                  <ChevronRight className="w-4 h-4 opacity-50 shrink-0" />
                </button>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* STREAM RECORDS EXPLORER */}
      <div className="lg:col-span-3 space-y-6 ">
        {/* Selected stream metadata info */}
        <div className="bg-white dark:bg-zinc-900 px-6 py-4 rounded-md flex items-center justify-between">
          <div>
            <h4 className="font-bold text-zinc-900 dark:text-white leading-tight">
              Stream: &quot;{selectedStream || "No Stream Selected"}&quot;
            </h4>
          </div>

          <div className="flex items-center gap-3">
            {selectedStream && userRole === "admin" && (
              <button
                onClick={() => {
                  setDeleteConfirmText("");
                  setDeleteError("");
                  setIsDeleteModalOpen(true);
                }}
                className="py-2 px-4 rounded border border-rose-200 dark:border-rose-900 hover:bg-rose-500/10 dark:hover:bg-rose-500/15 text-rose-500 text-xs font-semibold flex items-center gap-1.5 transition-all active:scale-[0.98]  shrink-0 animate-fade-in"
                title="Delete Stream"
              >
                <Trash2 className="w-3.5 h-3.5" />
                Delete Stream
              </button>
            )}

            <div className="relative">
              <button
                disabled={!selectedStream || isExporting}
                onClick={() => setIsExportDropdownOpen(!isExportDropdownOpen)}
                className="py-2 px-4 rounded bg-primary hover:bg-primary-hover text-white text-xs font-semibold flex items-center gap-1.5 transition-all active:scale-[0.98] disabled:opacity-50 disabled:cursor-not-allowed "
              >
                {isExporting ? (
                  <>
                    <Loader2 className="w-3.5 h-3.5 animate-spin" />
                    Exporting...
                  </>
                ) : (
                  <>
                    <Download className="w-3.5 h-3.5" />
                    Export Stream
                    <ChevronDown
                      className={`w-3.5 h-3.5 transition-transform duration-200 ${
                        isExportDropdownOpen ? "rotate-180" : ""
                      }`}
                    />
                  </>
                )}
              </button>

              {isExportDropdownOpen && (
                <>
                  <div
                    className="fixed inset-0 z-35"
                    onClick={() => setIsExportDropdownOpen(false)}
                  />
                  <div className="absolute right-0 mt-2 w-48 rounded-md border border-zinc-200 dark:border-zinc-700 bg-zinc-50 dark:bg-zinc-800 shadow-2xl p-1.5 z-40 animate-fade-in ">
                    <button
                      onClick={() => handleStreamExport()}
                      className="w-full text-left px-3 py-2 rounded text-xs font-bold text-zinc-600 dark:text-zinc-300 hover:bg-zinc-100 dark:hover:bg-zinc-900 hover:text-zinc-900 dark:hover:text-white transition-all flex items-center gap-2"
                    >
                      <FileText className="w-4 h-4 text-accent" />
                      <span>JSON Lines (.jsonl)</span>
                    </button>
                  </div>
                </>
              )}
            </div>
          </div>
        </div>

        {/* Stream Data Table */}
        <div className="bg-white dark:bg-zinc-900 rounded-md overflow-hidden">
          {!selectedStream ? (
            <div className="p-12 text-center text-zinc-500 dark:text-zinc-400">
              <Database className="w-8 h-8 mx-auto mb-3 opacity-30 text-zinc-400 dark:text-zinc-500" />
              <p className="text-sm font-medium">
                Select a stream on the left panel to browse its keys.
              </p>
            </div>
          ) : browsedRecords.length === 0 ? (
            <div className="p-12 text-center text-zinc-500 dark:text-zinc-400">
              <Search className="w-8 h-8 mx-auto mb-3 opacity-30 text-zinc-400 dark:text-zinc-500" />
              <p className="text-sm font-medium">
                No records found inside stream &quot;{selectedStream}&quot;.
              </p>
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
                  <span>total records</span>
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
                      value={browsedPageSize}
                      onChange={(e) => {
                        setBrowsedPageSize(Number(e.target.value));
                        setBrowsedCurrentPage(1);
                      }}
                      className="bg-transparent text-zinc-700 dark:text-zinc-300 border border-zinc-200 dark:border-zinc-800 rounded px-2.5 py-1 outline-none font-bold cursor-pointer hover:border-zinc-300 dark:hover:border-zinc-700 transition-colors focus:border-primary"
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
                /* Records List */
                <div className="divide-y divide-zinc-100 dark:divide-zinc-800 font-mono text-xs">
                  {paginatedRecords.map((rec) => (
                    <RowExpand
                      key={rec.sequence_id}
                      record={rec}
                      resolvedTheme={resolvedTheme}
                    />
                  ))}
                </div>
              ) : (
                /* Records Table (Prettified dynamic grid view) */
                <PrettifiedGridView
                  records={paginatedRecords}
                  resolvedTheme={resolvedTheme}
                />
              )}

              {/* Pagination Controls */}
              {totalPages > 1 && (
                <div className="px-6 py-4 border-t border-zinc-100 dark:border-zinc-800 bg-zinc-50/20 dark:bg-zinc-900/5 flex items-center justify-between">
                  <button
                    disabled={browsedCurrentPage === 1}
                    onClick={() =>
                      setBrowsedCurrentPage(Math.max(1, browsedCurrentPage - 1))
                    }
                    className="px-3 py-1.5 rounded border-zinc-200 dark:border-zinc-800 bg-zinc-200 hover:bg-zinc-50 dark:hover:bg-zinc-900 text-zinc-700 dark:text-zinc-400 text-xs font-bold disabled:opacity-40 disabled:cursor-not-allowed transition-all"
                  >
                    Previous
                  </button>

                  <div className="flex items-center gap-1.5">
                    {getPageNumbers(browsedCurrentPage, totalPages).map(
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
                        const isActive = pageNum === browsedCurrentPage;
                        return (
                          <button
                            key={pageNum}
                            onClick={() => setBrowsedCurrentPage(pageNum)}
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
                    disabled={browsedCurrentPage === totalPages}
                    onClick={() =>
                      setBrowsedCurrentPage(
                        Math.min(totalPages, browsedCurrentPage + 1),
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

      {/* IMPORT STREAM DATA MODAL DIALOG */}
      {isImportingOpen && (
        <div className="fixed inset-0 bg-body-bg/95 backdrop-blur-xl flex items-center justify-center z-50 p-4 animate-fade-in">
          <div className="bg-white dark:bg-zinc-900 max-w-2xl w-full rounded-md p-6 space-y-6 relative overflow-hidden shadow-2xl bg-panel-bg">
            <div className="flex items-start justify-between">
              <div>
                <h4 className="font-bold text-zinc-900 dark:text-white text-lg">
                  Import Stream Data
                </h4>
                <p className="text-xs text-zinc-500 dark:text-zinc-400 mt-1">
                  Upload or paste bulk records to append directly into the
                  storage engine.
                </p>
              </div>
              <button
                onClick={() => setIsImportingOpen(false)}
                className="p-1 rounded hover:bg-zinc-100 dark:hover:bg-zinc-900 text-zinc-400 dark:text-zinc-500 hover:text-zinc-600 dark:hover:text-zinc-300 transition-colors"
              >
                <XCircle className="w-5 h-5" />
              </button>
            </div>

            <form onSubmit={handleBulkImport} className="space-y-4">
              {/* Drag & Drop File Upload Area */}
              <div className="space-y-1.5">
                <label className="text-xs font-bold text-zinc-500 dark:text-zinc-400">
                  Upload Payload Document
                </label>
                <div className="border border-dashed border-zinc-200 dark:border-zinc-800 rounded-md p-4 text-center hover:border-primary dark:hover:border-primary transition-colors relative cursor-pointer group">
                  <input
                    type="file"
                    accept=".jsonl,.json,.txt"
                    onChange={handleFileChange}
                    className="absolute inset-0 opacity-0 cursor-pointer"
                  />
                  <Upload className="w-8 h-8 mx-auto mb-2 text-zinc-400 dark:text-zinc-600 group-hover:text-primary transition-colors" />
                  <p className="text-xs font-semibold text-zinc-700 dark:text-zinc-300">
                    Drag and drop your file here, or{" "}
                    <span className="text-primary">browse</span>
                  </p>
                  <p className="text-[10px] text-zinc-400 dark:text-zinc-500 mt-1">
                    Supports .jsonl file
                  </p>
                </div>
              </div>

              {/* CodeMirror raw payload pasting */}
              <div className="space-y-1.5">
                <label className="text-xs font-bold text-zinc-500 dark:text-zinc-400 flex items-center justify-between">
                  <span>Pasted Document Editor</span>
                  <span className="text-[10px] text-zinc-400 dark:text-zinc-600 font-mono">
                    {
                      'Each row: {"stream": "logs", "key": "001", "value": "hello"}'
                    }
                  </span>
                </label>
                <div className="border border-zinc-200 dark:border-zinc-800 rounded-md overflow-hidden text-xs">
                  <CodeMirror
                    value={importFileContent}
                    height="160px"
                    extensions={[json()]}
                    onChange={(val) => setImportFileContent(val)}
                    theme={
                      resolvedTheme === "dark" ? gruvboxDark : atomOneLight
                    }
                    basicSetup={{
                      highlightActiveLine: false,
                      highlightActiveLineGutter: false,
                    }}
                    className="font-mono text-xs"
                  />
                </div>

                {importError && (
                  <div
                    className={`flex items-start gap-3 p-4 border rounded-2xl text-xs mb-6 animate-shake ${
                      resolvedTheme === "dark"
                        ? "bg-red-950/30 border-red-500/20 text-red-200"
                        : "bg-red-50 border-red-200 text-red-800"
                    }`}
                  >
                    <XCircle className="w-4 h-4 shrink-0" />
                    <span className="font-mono text-[11px] leading-normal">
                      {importError}
                    </span>
                  </div>
                )}

                {importSuccess && (
                  <div
                    className={`flex items-start gap-3 p-4 border rounded-2xl text-xs mb-6 animate-shake ${
                      resolvedTheme === "dark"
                        ? "bg-emerald-950/30 border-emerald-500/20 text-emerald-200"
                        : "bg-emerald-50 border-emerald-200 text-emerald-800"
                    }`}
                  >
                    <CheckCircle2 className="w-4 h-4 shrink-0" />
                    <span className="font-semibold text-[11px] leading-normal">
                      {importSuccess}
                    </span>
                  </div>
                )}
              </div>

              <div className="flex items-center justify-end gap-3 pt-4 border-t border-zinc-100 dark:border-zinc-800">
                <button
                  type="button"
                  onClick={() => setIsImportingOpen(false)}
                  className="px-4 py-2 rounded border border-zinc-200 dark:border-zinc-800 text-zinc-500 dark:text-zinc-400 hover:text-zinc-800 dark:hover:text-zinc-200 hover:bg-zinc-100 dark:hover:bg-zinc-900 text-xs font-bold transition-colors"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={
                    isSubmitting || !!importError || !importFileContent.trim()
                  }
                  className="px-6 py-2.5 rounded bg-primary hover:bg-primary-hover text-white text-xs font-bold active:scale-[0.98] transition-all disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-1.5 "
                >
                  {isSubmitting ? (
                    <>
                      <Loader2 className="w-3.5 h-3.5 animate-spin" />
                      Importing...
                    </>
                  ) : (
                    <>
                      <Sliders className="w-3.5 h-3.5" />
                      Import Data
                    </>
                  )}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* DELETE STREAM WARNING/CONFIRMATION MODAL */}
      {isDeleteModalOpen && (
        <div className="fixed inset-0 bg-body-bg/95 backdrop-blur-xl flex items-center justify-center z-50 p-4 animate-fade-in">
          <div className="bg-white dark:bg-zinc-900 max-w-md w-full rounded-md p-6 space-y-6 relative overflow-hidden shadow-2xl bg-panel-bg">
            <div className="flex items-start gap-4">
              <div className="p-3 rounded bg-rose-500/10 text-rose-500 shrink-0">
                <AlertTriangle className="w-6 h-6 animate-pulse" />
              </div>
              <div className="space-y-1">
                <h4 className="font-bold text-zinc-900 dark:text-white text-lg">
                  Delete Stream?
                </h4>
                <p className="text-xs text-zinc-500 dark:text-zinc-400">
                  This action is permanent and{" "}
                  <span className="text-rose-500 font-bold">
                    cannot be undone
                  </span>
                  . All records associated with the stream will be dropped from
                  the database.
                </p>
              </div>
            </div>

            <div className="space-y-3">
              <p className="text-xs text-zinc-650 dark:text-zinc-350 font-medium">
                To confirm deletion of stream{" "}
                <span className="font-bold text-zinc-800 dark:text-zinc-100">
                  &quot;{selectedStream}&quot;
                </span>
                , please type the stream name below:
              </p>
              <input
                type="text"
                value={deleteConfirmText}
                onChange={(e) => setDeleteConfirmText(e.target.value)}
                placeholder={selectedStream}
                className="w-full bg-transparent  text-zinc-900 dark:text-white border border-zinc-200 dark:border-zinc-800 rounded px-4 py-2.5 text-xs outline-none focus:border-rose-500 font-bold transition-all placeholder:opacity-50"
              />
              {deleteError && (
                <p className="text-[11px] text-rose-500 font-bold animate-fade-in">
                  {deleteError}
                </p>
              )}
            </div>

            <div className="flex items-center justify-end gap-3 pt-4 border-t border-zinc-100 dark:border-zinc-800">
              <button
                type="button"
                onClick={() => {
                  setIsDeleteModalOpen(false);
                  setDeleteConfirmText("");
                  setDeleteError("");
                }}
                className="px-4 py-2 rounded border border-zinc-200 dark:border-zinc-800 text-zinc-500 dark:text-zinc-400 hover:text-zinc-800 dark:hover:text-zinc-200  text-xs font-bold transition-colors"
              >
                Cancel
              </button>
              <button
                type="button"
                disabled={deleteConfirmText !== selectedStream || isDeleting}
                onClick={handleDeleteStream}
                className="px-6 py-2.5 rounded bg-rose-500 text-white text-xs font-bold hover:bg-rose-600 active:scale-[0.98] transition-all disabled:opacity-30 disabled:cursor-not-allowed disabled:hover:bg-rose-500 disabled:active:scale-100 flex items-center gap-1.5 "
              >
                {isDeleting ? (
                  <>
                    <Loader2 className="w-3.5 h-3.5 animate-spin" />
                    Deleting...
                  </>
                ) : (
                  <>
                    <Trash2 className="w-3.5 h-3.5" />
                    Delete Stream
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
