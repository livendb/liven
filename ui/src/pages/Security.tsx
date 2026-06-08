import { useState, useEffect } from "react";
import {
  Key,
  Copy,
  Check,
  AlertTriangle,
  AlertCircle,
  Trash2,
  Plus,
  RefreshCw,
  X,
  Lock,
  Unlock,
  Eye,
  EyeOff,
  Info,
} from "lucide-react";
import {
  fetchAuthKeys,
  generateAuthKey,
  revokeAuthKey,
  AuthKeyRecord,
  GenerateKeyResponse,
} from "../utils/requests";

export interface SecurityProps {
  addActivity: (
    message: string,
    category: "storage" | "query" | "server" | "stream",
    type?: "info" | "success" | "warn" | "error",
  ) => void;
  resolvedTheme: "light" | "dark";
}

export default function Security({
  addActivity,
  resolvedTheme,
}: SecurityProps) {
  // Config & Keys State
  const [securityMode, setSecurityMode] = useState<string>("auth_key");
  const [keys, setKeys] = useState<AuthKeyRecord[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string>("");

  // Modal State - Generate Key
  const [isGenModalOpen, setIsGenModalOpen] = useState<boolean>(false);
  const [newKeyId, setNewKeyId] = useState<string>("");
  const [newKeyRole, setNewKeyRole] = useState<string>("admin");
  const [genLoading, setGenLoading] = useState<boolean>(false);
  const [genError, setGenError] = useState<string>("");
  const [generatedResult, setGeneratedResult] =
    useState<GenerateKeyResponse | null>(null);
  const [showRawKey, setShowRawKey] = useState<boolean>(false);
  const [copiedRawKey, setCopiedRawKey] = useState<boolean>(false);

  // Modal State - Revoke Confirmation
  const [keyToRevoke, setKeyToRevoke] = useState<string | null>(null);
  const [revokeLoading, setRevokeLoading] = useState<boolean>(false);

  // Load configuration and active keys
  const loadConfigAndKeys = async (silent = false) => {
    if (!silent) setLoading(true);
    setError("");
    try {
      // Fetch system configuration
      const configRes = await fetch("/api/system/config");
      if (configRes.ok) {
        const configData = await configRes.json();
        setSecurityMode(configData.security?.mode || "auth_key");
      }

      // Fetch keys
      const data = await fetchAuthKeys();
      // Sort keys: active first, then alphabetically by key_id
      const sortedKeys = [...data].sort((a, b) => {
        if (a.status === "active" && b.status !== "active") return -1;
        if (a.status !== "active" && b.status === "active") return 1;
        return a.key_id.localeCompare(b.key_id);
      });
      setKeys(sortedKeys);
    } catch (err: any) {
      console.error(err);
      setError(err.message || "Failed to load security assets from cluster.");
    } finally {
      if (!silent) setLoading(false);
    }
  };

  useEffect(() => {
    loadConfigAndKeys();
  }, []);

  // Open revoke confirmation
  const handleOpenRevokeConfirm = (keyId: string) => {
    setKeyToRevoke(keyId);
  };

  // Perform revocation
  const handleRevokeKey = async () => {
    if (!keyToRevoke) return;
    setRevokeLoading(true);
    try {
      await revokeAuthKey(keyToRevoke);
      addActivity(
        `Revoked key: "${keyToRevoke}". Real-time client connections terminated.`,
        "server",
        "warn",
      );
      setKeyToRevoke(null);
      await loadConfigAndKeys(true);
    } catch (err: any) {
      console.error(err);
      addActivity(
        `Failed to revoke key "${keyToRevoke}": ${err.message || "Unknown error"}`,
        "server",
        "error",
      );
    } finally {
      setRevokeLoading(false);
    }
  };

  // Submit key generation form
  const handleGenerateKeySubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setGenError("");
    setGeneratedResult(null);

    const trimmedId = newKeyId.trim();
    if (!trimmedId) {
      setGenError("Key Identifier / Label is required.");
      return;
    }

    setGenLoading(true);
    try {
      const result = await generateAuthKey(trimmedId, newKeyRole);
      setGeneratedResult(result);
      addActivity(
        `Generated new key: "${trimmedId}" with role "${newKeyRole}".`,
        "server",
        "success",
      );
      // Keep modal open to show the raw key exactly once
    } catch (err: any) {
      console.error(err);
      setGenError(
        err.message || "Failed to generate key. Ensure ID is unique.",
      );
    } finally {
      setGenLoading(false);
    }
  };

  const handleCopyRawKey = () => {
    if (!generatedResult) return;
    navigator.clipboard.writeText(generatedResult.raw_key);
    setCopiedRawKey(true);
    setTimeout(() => setCopiedRawKey(false), 2500);
  };

  const handleCloseGenModal = () => {
    setIsGenModalOpen(false);
    setNewKeyId("");
    setNewKeyRole("admin");
    setGeneratedResult(null);
    setGenError("");
    setShowRawKey(false);
    setCopiedRawKey(false);
    loadConfigAndKeys(true);
  };

  return (
    <div className="space-y-8 animate-fade-in font-sans">
      {/* HEADER SECTION */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-6 pb-6">
        <div className="space-y-1">
          <h2 className="text-3xl font-extrabold tracking-tight text-zinc-900 dark:text-zinc-100 flex items-center gap-3">
            Security & Keys
          </h2>
        </div>

        <div className="flex items-center gap-3">
          <button
            onClick={() => loadConfigAndKeys(false)}
            className={`p-3 rounded-2xl border transition-all active:scale-[0.98] ${
              resolvedTheme === "dark"
                ? "bg-zinc-900/60 border-zinc-800 text-zinc-400 hover:text-zinc-200 hover:bg-zinc-800/60"
                : "bg-white border-zinc-200 text-zinc-600 hover:text-zinc-800 hover:bg-zinc-50"
            }`}
            title="Refresh keys ledger"
          >
            <RefreshCw className={`w-4 h-4 ${loading ? "animate-spin" : ""}`} />
          </button>
          <button
            onClick={() => setIsGenModalOpen(true)}
            className="px-5 py-3 bg-gradient-to-r from-emerald-600 to-teal-600 hover:from-emerald-500 hover:to-teal-500 text-white font-bold text-xs uppercase tracking-wider rounded-2xl shadow-lg shadow-emerald-950/10 flex items-center gap-2.5 hover:scale-[1.02] active:scale-[0.98] transition-all"
          >
            <Plus className="w-4 h-4" />
            Generate New Key
          </button>
        </div>
      </div>

      {/* SECURITY MODE BANNER */}
      {securityMode === "none" && (
        <div
          className={`p-5 rounded-2xl border flex items-start gap-4 animate-shake ${
            resolvedTheme === "dark"
              ? "bg-red-950/20 border-red-500/25 text-red-300"
              : "bg-red-50 border-red-200 text-red-800"
          }`}
        >
          <AlertTriangle className="w-6 h-6 text-red-500 shrink-0 mt-0.5 animate-bounce" />
          <div className="space-y-1">
            <h4 className="text-sm font-black uppercase tracking-wider">
              Authentication Bypass Active
            </h4>
            <p className="text-xs leading-relaxed opacity-90">
              The KondaDB server is currently configured with{" "}
              <strong>mode = &quot;none&quot;</strong>. Authentication is
              completely disabled across both the Web UI and native wire
              protocol boundaries. This configuration is highly insecure and
              should only be active for development, local diagnostics, or
              isolated testing.
            </p>
          </div>
        </div>
      )}

      {/* ERROR STATEMENT */}
      {error && (
        <div
          className={`p-5 rounded-2xl border flex items-start gap-3.5 ${
            resolvedTheme === "dark"
              ? "bg-red-950/20 border-red-500/20 text-red-300"
              : "bg-red-50 border-red-200 text-red-800"
          }`}
        >
          <AlertCircle className="w-5 h-5 shrink-0 mt-0.5" />
          <div className="space-y-1">
            <h4 className="text-sm font-bold uppercase tracking-wider">
              Connection Failure
            </h4>
            <p className="text-xs">{error}</p>
          </div>
        </div>
      )}

      {/* HIGH DENSITY KEYS TABLE CARD */}
      <div
        className={`rounded border shadow-xl overflow-hidden backdrop-blur-xl ${
          resolvedTheme === "dark"
            ? "bg-zinc-950/40 border-zinc-800/60 shadow-emerald-950/5"
            : "bg-white border-zinc-200/85 shadow-zinc-200/10"
        }`}
      >
        <div className="p-6 border-b border-zinc-200/40 dark:border-zinc-800/40 flex items-center justify-between">
          <div className="space-y-0.5">
            <h3 className="text-base font-bold text-zinc-900 dark:text-zinc-100">
              Access keys
            </h3>
            <p className="text-[11px] text-zinc-400 dark:text-zinc-500 uppercase tracking-wider font-semibold">
              Authorized credentials
            </p>
          </div>
          <span
            className={`inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-[10px] font-bold uppercase border ${
              securityMode === "none"
                ? "bg-red-500/10 border-red-500/20 text-red-400"
                : "bg-emerald-500/10 border-emerald-500/20 text-emerald-400"
            }`}
          >
            {securityMode === "none" ? (
              <Unlock className="w-3 h-3" />
            ) : (
              <Lock className="w-3 h-3" />
            )}
            Mode: {securityMode.toUpperCase()}
          </span>
        </div>

        {loading ? (
          <div className="py-24 flex flex-col items-center justify-center space-y-4">
            <RefreshCw className="w-8 h-8 text-emerald-500 animate-spin" />
            <p className="text-xs text-zinc-500 uppercase tracking-widest font-bold">
              Querying database keys...
            </p>
          </div>
        ) : keys.length === 0 ? (
          <div className="py-20 text-center space-y-3">
            <Key className="w-12 h-12 text-zinc-300 dark:text-zinc-700 mx-auto" />
            <h4 className="text-sm font-bold text-zinc-700 dark:text-zinc-300 uppercase tracking-wider">
              No Keys
            </h4>
            <p className="text-xs text-zinc-400 max-w-sm mx-auto">
              No keys exist
            </p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left border-collapse">
              <thead>
                <tr
                  className={`text-[10px] font-bold uppercase tracking-widest border-b border-zinc-200/40 dark:border-zinc-800/30 ${
                    resolvedTheme === "dark"
                      ? "bg-zinc-900/30 text-zinc-500"
                      : "bg-zinc-50 text-zinc-500"
                  }`}
                >
                  <th className="py-4 px-6 font-extrabold">Key</th>
                  <th className="py-4 px-6 font-extrabold">Capability Role</th>
                  <th className="py-4 px-6 font-extrabold">Status</th>
                  <th className="py-4 px-6 font-extrabold text-right">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className="py-4">
                {keys.map((record) => {
                  const isActive = record.status === "active";
                  return (
                    <tr
                      key={record.key_id}
                      className={`group hover:bg-zinc-500/[0.02] border-t border-zinc-200/40 dark:border-zinc-800/30 transition-colors ${
                        !isActive ? "opacity-60" : ""
                      }`}
                    >
                      {/* Key ID / Label */}
                      <td className="py-4 px-6">
                        <div className="flex items-center gap-3">
                          <div
                            className={`w-8.5 h-8.5 rounded flex items-center justify-center border transition-all ${
                              isActive
                                ? resolvedTheme === "dark"
                                  ? "bg-emerald-500/5 border-emerald-500/10 text-emerald-400 group-hover:border-emerald-500/30"
                                  : "bg-emerald-50 border-emerald-100 text-emerald-600"
                                : "bg-zinc-500/5 border-zinc-200/80 dark:border-zinc-800 text-zinc-400"
                            }`}
                          >
                            <Key className="w-4 h-4" />
                          </div>
                          <div>
                            <div className="text-sm font-bold text-zinc-900 dark:text-zinc-100 group-hover:text-emerald-500 transition-colors">
                              {record.key_id}
                            </div>
                          </div>
                        </div>
                      </td>

                      {/* Capability Role */}
                      <td className="py-4 px-6">
                        {record.role === "admin" ? (
                          <span className="inline-flex items-center px-2.5 py-1 rounded-lg text-[10px] font-black uppercase tracking-wider bg-rose-500/10 text-rose-500 border border-rose-500/20">
                            Root Admin
                          </span>
                        ) : record.role === "write-only" ? (
                          <span className="inline-flex items-center px-2.5 py-1 rounded-lg text-[10px] font-black uppercase tracking-wider bg-blue-500/10 text-blue-500 border border-blue-500/20">
                            Write-Only
                          </span>
                        ) : (
                          <span className="inline-flex items-center px-2.5 py-1 rounded-lg text-[10px] font-black uppercase tracking-wider bg-zinc-500/10 text-zinc-400 border border-zinc-500/20">
                            Read-Only
                          </span>
                        )}
                      </td>

                      {/* Status Badge */}
                      <td className="py-4 px-6">
                        {isActive ? (
                          <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[10px] font-bold uppercase tracking-wider bg-emerald-500/10 text-emerald-400 border border-emerald-500/25">
                            <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
                            Active
                          </span>
                        ) : (
                          <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[10px] font-bold uppercase tracking-wider bg-zinc-500/10 text-zinc-400 border border-zinc-500/15">
                            Revoked
                          </span>
                        )}
                      </td>

                      {/* Actions */}
                      <td className="py-4 px-6 text-right">
                        {isActive ? (
                          <button
                            onClick={() =>
                              handleOpenRevokeConfirm(record.key_id)
                            }
                            className={`p-2.5 rounded border border-red-500/10 bg-red-500/[0.02] text-red-400 hover:text-white hover:bg-red-600 hover:border-red-600 hover:shadow-lg hover:shadow-red-950/10 transition-all duration-300`}
                            title="Revoke Key & Disconnect Users"
                          >
                            <Trash2 className="w-4 h-4" />
                          </button>
                        ) : (
                          <span className="text-zinc-500 dark:text-zinc-600 text-xs font-bold uppercase select-none pr-2.5">
                            Terminated
                          </span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* MODAL 1: KEY GENERATION */}
      {isGenModalOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
          {/* Backdrop */}
          <div
            className="absolute inset-0 bg-[#04060c]/80 backdrop-blur-md transition-opacity duration-300"
            onClick={generatedResult ? undefined : handleCloseGenModal}
          />

          {/* Dialog Body */}
          <div
            className={`relative w-full max-w-lg rounded border p-6 shadow-2xl overflow-hidden transition-all transform duration-300 scale-100 ${
              resolvedTheme === "dark"
                ? "bg-zinc-950 border-emerald-500/15 shadow-emerald-950/20"
                : "bg-white border-zinc-200 shadow-zinc-300/20"
            }`}
          >
            <button
              onClick={handleCloseGenModal}
              className="absolute top-5 right-5 p-1.5 rounded-lg text-zinc-400 hover:text-zinc-200 transition-colors hover:bg-zinc-500/10"
            >
              <X className="w-4 h-4" />
            </button>

            {!generatedResult ? (
              // Phase 1: Key Creation Form
              <div className="space-y-6">
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-2xl bg-emerald-500/10 text-emerald-500 flex items-center justify-center border border-emerald-500/20">
                    <Plus className="w-5 h-5 animate-pulse" />
                  </div>
                  <div>
                    <h3 className="text-lg font-extrabold text-zinc-900 dark:text-zinc-100">
                      Create new auth key
                    </h3>
                    <p className="text-xs text-zinc-400 mt-0.5">
                      Configure access level and identify your key record
                    </p>
                  </div>
                </div>

                {genError && (
                  <div
                    className={`p-4 rounded border text-xs flex items-start gap-2.5 animate-shake ${
                      resolvedTheme === "dark"
                        ? "bg-red-950/20 border-red-500/20 text-red-300"
                        : "bg-red-50 border-red-150 text-red-800"
                    }`}
                  >
                    <AlertCircle className="w-4 h-4 shrink-0 mt-0.5" />
                    <span>{genError}</span>
                  </div>
                )}

                <form onSubmit={handleGenerateKeySubmit} className="space-y-5">
                  <div className="space-y-2">
                    <label className="text-[10px] font-bold text-zinc-400 uppercase tracking-widest block">
                      Key
                    </label>
                    <input
                      type="text"
                      placeholder="e.g., node_aggregator_prod"
                      value={newKeyId}
                      onChange={(e) =>
                        setNewKeyId(
                          e.target.value.replace(/[^a-zA-Z0-9_\-]/g, ""),
                        )
                      }
                      maxLength={64}
                      className={`w-full px-4 py-3 text-xs rounded border focus:outline-none focus:border-emerald-500 transition-colors font-bold ${
                        resolvedTheme === "dark"
                          ? "bg-zinc-900/50 border-zinc-800 text-zinc-100"
                          : "bg-zinc-50 border-zinc-200 text-zinc-950"
                      }`}
                      required
                    />
                    <span className="text-[10px] text-zinc-500 block">
                      Only alphanumeric characters, dashes, and underscores are
                      permitted. Maximum 64 characters.
                    </span>
                  </div>

                  <div className="space-y-2">
                    <label className="text-[10px] font-bold text-zinc-400 uppercase tracking-widest block">
                      Capability Role & Permission Boundary
                    </label>
                    <div className="grid grid-cols-1 gap-2.5">
                      {[
                        {
                          role: "admin",
                          title: "Root Admin",
                          desc: "Unlimited capabilities. Able to list, create, and revoke other key identities.",
                        },
                        {
                          role: "write-only",
                          title: "Write-Only Access",
                          desc: "Allows write operations like data ingestion and compaction triggers.",
                        },
                        {
                          role: "read-only",
                          title: "Read-Only Access",
                          desc: "Restricts key capabilities to query executions. Data modifications are blocked.",
                        },
                      ].map((item) => {
                        const isSelected = newKeyRole === item.role;
                        return (
                          <button
                            key={item.role}
                            type="button"
                            onClick={() => setNewKeyRole(item.role)}
                            className={`p-3 rounded border text-left flex items-start gap-3.5 transition-all ${
                              isSelected
                                ? "border-emerald-500 bg-emerald-500/[0.03] ring-1 ring-emerald-500"
                                : resolvedTheme === "dark"
                                  ? "border-zinc-800 bg-zinc-900/15 hover:border-zinc-700 text-zinc-300"
                                  : "border-zinc-200 bg-white hover:border-zinc-300 text-zinc-700"
                            }`}
                          >
                            <div
                              className={`w-5 h-5 rounded-full border flex items-center justify-center shrink-0 mt-0.5 transition-all ${
                                isSelected
                                  ? "border-emerald-500 bg-emerald-500 text-white"
                                  : "border-zinc-500"
                              }`}
                            >
                              {isSelected && (
                                <Check className="w-3 h-3 stroke-[3]" />
                              )}
                            </div>
                            <div className="space-y-0.5">
                              <span className="text-xs font-bold text-zinc-900 dark:text-zinc-100">
                                {item.title}
                              </span>
                              <p className="text-[10px] text-zinc-500 leading-normal">
                                {item.desc}
                              </p>
                            </div>
                          </button>
                        );
                      })}
                    </div>
                  </div>

                  <div className="pt-2 flex justify-end gap-3">
                    <button
                      type="button"
                      onClick={handleCloseGenModal}
                      className={`px-5 py-3 rounded text-xs font-bold uppercase tracking-wider border transition-all ${
                        resolvedTheme === "dark"
                          ? "bg-transparent border-zinc-800 text-zinc-400 hover:text-zinc-200 hover:bg-zinc-900/40"
                          : "bg-white border-zinc-200 text-zinc-500 hover:text-zinc-800 hover:bg-zinc-50"
                      }`}
                    >
                      Cancel
                    </button>
                    <button
                      type="submit"
                      disabled={genLoading}
                      className="px-6 py-3 bg-emerald-600 hover:bg-emerald-500 disabled:opacity-50 text-white font-bold text-xs uppercase tracking-wider rounded transition-all shadow-md"
                    >
                      {genLoading ? "Forging Key..." : "Authorize & Generate"}
                    </button>
                  </div>
                </form>
              </div>
            ) : (
              // Phase 2: Show Raw Key Exactly Once
              <div className="space-y-6 animate-slide-up">
                <div className="flex items-center gap-3 text-amber-500">
                  <div className="w-10 h-10 rounded-2xl bg-amber-500/10 flex items-center justify-center border border-amber-500/20">
                    <AlertTriangle className="w-5 h-5 animate-bounce" />
                  </div>
                  <div>
                    <h3 className="text-lg font-black text-zinc-900 dark:text-zinc-100">
                      CRITICAL: Save Auth Key
                    </h3>
                  </div>
                </div>

                <div
                  className={`p-4 rounded border text-xs leading-relaxed space-y-2 font-semibold ${
                    resolvedTheme === "dark"
                      ? "bg-amber-950/20 border-amber-500/25 text-amber-200"
                      : "bg-amber-50 border-amber-200 text-amber-800"
                  }`}
                >
                  <p className="font-extrabold uppercase text-[11px] flex items-center gap-1">
                    <Info className="w-3.5 h-3.5 shrink-0" />
                    If you lose or dismiss this dialog, you cannot recover this
                    key again!
                  </p>
                </div>

                <div className="space-y-2">
                  <label className="text-[10px] font-bold text-zinc-400 uppercase tracking-widest block">
                    Administrative Key Details
                  </label>
                  <div
                    className={`p-3 rounded border flex items-center justify-between text-xs font-mono font-bold ${
                      resolvedTheme === "dark"
                        ? "bg-zinc-900/30 border-zinc-800 text-zinc-300"
                        : "bg-zinc-50 border-zinc-250 text-zinc-700"
                    }`}
                  >
                    <span>ID: {generatedResult.key_id}</span>
                    <span className="uppercase text-[9px] px-2 py-0.5 rounded bg-zinc-500/20">
                      {generatedResult.role}
                    </span>
                  </div>
                </div>

                <div className="space-y-2">
                  <div className="flex items-center justify-between">
                    <button
                      onClick={handleCopyRawKey}
                      className="text-[10px] font-bold text-emerald-500 hover:underline flex items-center gap-1 uppercase"
                    >
                      {copiedRawKey ? (
                        <Check className="w-3 h-3" />
                      ) : (
                        <Copy className="w-3 h-3" />
                      )}
                      {copiedRawKey ? "Copied Plaintext!" : "Copy Key"}
                    </button>
                  </div>

                  <div className="relative">
                    <div
                      className={`p-4 rounded-2xl font-mono text-sm break-all font-bold select-all tracking-wider text-center border leading-relaxed select-all relative ${
                        resolvedTheme === "dark"
                          ? "bg-zinc-950 border-zinc-800 text-emerald-400"
                          : "bg-zinc-50 border-zinc-200 text-emerald-600"
                      }`}
                    >
                      {showRawKey
                        ? generatedResult.raw_key
                        : "•".repeat(generatedResult.raw_key.length)}
                    </div>
                    <button
                      type="button"
                      onClick={() => setShowRawKey(!showRawKey)}
                      className="absolute right-3.5 top-1/2 -translate-y-1/2 p-2 rounded-lg text-zinc-400 hover:text-zinc-200 hover:bg-zinc-500/10"
                      title={showRawKey ? "Mask characters" : "Show characters"}
                    >
                      {showRawKey ? (
                        <EyeOff className="w-4 h-4" />
                      ) : (
                        <Eye className="w-4 h-4" />
                      )}
                    </button>
                  </div>
                </div>

                <div className="pt-2 flex justify-center">
                  <button
                    onClick={handleCloseGenModal}
                    className="w-full py-3.5 bg-gradient-to-r from-emerald-600 to-teal-600 hover:from-emerald-500 hover:to-teal-500 text-white font-bold text-xs uppercase tracking-wider rounded shadow-lg hover:shadow-emerald-950/20 active:scale-[0.98] transition-all"
                  >
                    I Have Safely Saved This Key
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* MODAL 2: CONFIRM REVOCATION */}
      {keyToRevoke && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
          <div
            className="absolute inset-0 bg-[#04060c]/80 backdrop-blur-md transition-opacity duration-300"
            onClick={() => setKeyToRevoke(null)}
          />

          <div
            className={`relative w-full max-w-md rounded border p-6 shadow-2xl transform transition-all duration-300 scale-100 ${
              resolvedTheme === "dark"
                ? "bg-zinc-950 border-red-500/15 shadow-red-950/10"
                : "bg-white border-zinc-200 shadow-zinc-300/10"
            }`}
          >
            <button
              onClick={() => setKeyToRevoke(null)}
              className="absolute top-5 right-5 p-1.5 rounded-lg text-zinc-400 hover:text-zinc-200 transition-colors hover:bg-zinc-500/10"
            >
              <X className="w-4 h-4" />
            </button>

            <div className="space-y-5">
              <div className="flex items-center gap-3 text-red-500">
                <div className="w-10 h-10 rounded-2xl bg-red-500/10 flex items-center justify-center border border-red-500/20">
                  <AlertTriangle className="w-5 h-5 animate-pulse" />
                </div>
                <div>
                  <h3 className="text-base font-extrabold text-zinc-900 dark:text-zinc-100">
                    Revoke Key Credentials?
                  </h3>
                  <p className="text-[10px] text-red-500 uppercase tracking-widest font-extrabold mt-0.5">
                    Destructive action
                  </p>
                </div>
              </div>

              <div className="space-y-3">
                <p className="text-xs text-zinc-500 dark:text-zinc-400 leading-relaxed">
                  Are you absolutely certain you want to revoke the key{" "}
                  <strong>&quot;{keyToRevoke}&quot;</strong>?
                </p>
                <div
                  className={`p-4 rounded border text-[11px] leading-relaxed font-semibold ${
                    resolvedTheme === "dark"
                      ? "bg-red-950/15 border-red-500/20 text-red-300"
                      : "bg-red-50 border-red-150 text-red-800"
                  }`}
                >
                  <p className="uppercase text-[10px] font-black tracking-wider text-red-500 flex items-center gap-1 mb-1">
                    <Lock className="w-3.5 h-3.5 shrink-0" /> Connection Notice
                  </p>
                  Revoking this key terminates active client connections or
                  active query sessions authenticated with this key immediately.
                  This action is **irreversible**.
                </div>
              </div>

              <div className="pt-2 flex justify-end gap-3">
                <button
                  onClick={() => setKeyToRevoke(null)}
                  className={`px-5 py-3 rounded text-xs font-bold uppercase tracking-wider border transition-all ${
                    resolvedTheme === "dark"
                      ? "bg-transparent border-zinc-800 text-zinc-400 hover:text-zinc-250 hover:bg-zinc-900/40"
                      : "bg-white border-zinc-200 text-zinc-500 hover:text-zinc-800 hover:bg-zinc-50"
                  }`}
                >
                  Cancel
                </button>
                <button
                  onClick={handleRevokeKey}
                  disabled={revokeLoading}
                  className="px-6 py-3 bg-gradient-to-r from-red-600 to-rose-600 hover:from-red-500 hover:to-rose-500 text-white font-bold text-xs uppercase tracking-wider rounded transition-all shadow-md active:scale-[0.98]"
                >
                  {revokeLoading
                    ? "Severing Connections..."
                    : "Revoke Key & Disconnect"}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
