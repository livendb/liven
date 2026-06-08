import { Suspense, useState, useEffect, useRef } from "react";
import lightTheme from "highlight.js/styles/atom-one-light.css?inline";
import darkTheme from "highlight.js/styles/atom-one-dark.css?inline";
import { WifiOff, RefreshCw } from "lucide-react";
import { Route, Switch, useLocation } from "wouter";

// Components & Pages
import Navbar from "./components/Navbar";
import QueryGuideModal from "./components/QueryGuideModal";
import Dashboard from "./pages/Dashboard";
import QueryConsole from "./pages/QueryConsole";
import StreamExplorer from "./pages/StreamExplorer";
import Security from "./pages/Security";
import Login from "./pages/Login";

// Types, Constants & Helpers
import { Record, Metrics, ActivityLog } from "./types";
import { getDbApiUrl, setDbPort } from "./utils/api";
import { fetchAuthStatus, submitSystemLogout } from "./utils/requests";

export default function App() {
  const [location, setLocation] = useLocation();

  const [securityMode, setSecurityMode] = useState<string>("none");
  const [isAuthenticated, setIsAuthenticated] = useState<boolean>(false);
  const [authChecking, setAuthChecking] = useState<boolean>(true);

  // Register a global fetch interceptor inside useEffect to handle 401/403 redirections
  useEffect(() => {
    const originalFetch = window.fetch;
    window.fetch = async (input, init) => {
      init = init || {};
      init.credentials = "include";
      try {
        const response = await originalFetch(input, init);
        if (response.status === 401 || response.status === 403) {
          const urlStr =
            typeof input === "string"
              ? input
              : input instanceof Request
                ? input.url
                : "";
          // Exclude check-auth, login and config endpoints to prevent loop
          if (
            !urlStr.includes("/api/system/auth/status") &&
            !urlStr.includes("/api/system/auth/login") &&
            !urlStr.includes("/api/system/config")
          ) {
            setIsAuthenticated(false);
          }
        }
        return response;
      } catch (err) {
        throw err;
      }
    };
    return () => {
      window.fetch = originalFetch;
    };
  }, [setLocation]);

  // Redirection effects for unauthenticated / authenticated routing
  useEffect(() => {
    if (!authChecking) {
      if (securityMode === "auth_key" && !isAuthenticated) {
        if (location !== "/login") {
          setLocation("/login");
        }
      } else if (isAuthenticated && location === "/login") {
        setLocation("/");
      }
    }
  }, [authChecking, securityMode, isAuthenticated, location, setLocation]);

  // Derive activeTab from URL location path for sidebar/header rendering
  let activeTab: "dashboard" | "query" | "explorer" | "security" = "dashboard";
  if (location === "/query") {
    activeTab = "query";
  } else if (location === "/explorer") {
    activeTab = "explorer";
  } else if (location === "/security") {
    activeTab = "security";
  }

  const setActiveTab = (
    tab: "dashboard" | "query" | "explorer" | "security",
  ) => {
    if (tab === "dashboard") {
      setLocation("/");
    } else {
      setLocation("/" + tab);
    }
  };

  // Theme support (system, light, dark)
  const [theme, setTheme] = useState<"system" | "light" | "dark">(
    () =>
      (localStorage.getItem("konda-theme") as "system" | "light" | "dark") ||
      "system",
  );
  const [resolvedTheme, setResolvedTheme] = useState<"light" | "dark">("light");

  useEffect(() => {
    localStorage.setItem("konda-theme", theme);
    const mediaQuery = window.matchMedia("(prefers-color-scheme: dark)");

    const updateTheme = () => {
      let active: "light" | "dark" = "light";
      if (theme === "system") {
        active = mediaQuery.matches ? "dark" : "light";
      } else {
        active = theme;
      }
      setResolvedTheme(active);

      if (active === "dark") {
        document.documentElement.classList.add("dark");
      } else {
        document.documentElement.classList.remove("dark");
      }
    };

    updateTheme();

    if (theme === "system") {
      mediaQuery.addEventListener("change", updateTheme);
      return () => mediaQuery.removeEventListener("change", updateTheme);
    }
  }, [theme]);

  // Server state
  const [wsConnected, setWsConnected] = useState(false);
  const [hasAttemptedConnect, setHasAttemptedConnect] = useState(false);
  const [metrics, setMetrics] = useState<Metrics>({
    ram_usage: 0,
    disk_size: 0,
    segments: 0,
    sequence_id: 0,
    key_count: 0,
    total_streams: 0,
  });

  // Live Activities Log State
  const [activities, setActivities] = useState<ActivityLog[]>([]);
  const [activityFilter, setActivityFilter] = useState<
    "all" | "storage" | "query" | "stream" | "server"
  >("all");

  // Track operational metrics delta
  const prevMetricsRef = useRef<Metrics | null>(null);
  const queriesThisSecondRef = useRef<number>(0);
  const lastSequenceIdRef = useRef<number | null>(null);

  const addActivity = (
    message: string,
    category: "storage" | "query" | "server" | "stream",
    type: "info" | "success" | "warn" | "error" = "info",
  ) => {
    const newLog: ActivityLog = {
      id: Math.random().toString(36).substring(2, 9),
      timestamp: new Date().toLocaleTimeString(),
      type,
      category,
      message,
    };
    setActivities((prev) => [newLog, ...prev].slice(0, 100));
  };

  // Query Console states (lifted up to load templates)
  const [query, setQuery] = useState('from("logs") | limit(10)');
  const [queryResults, setQueryResults] = useState<Record[]>([]);
  const [isQueryRunning, setIsQueryRunning] = useState(false);
  const [queryStats, setQueryStats] = useState({ count: 0, timeMs: 0 });
  const [continuousStream, setContinuousStream] = useState(false);
  const [queryCurrentPage, setQueryCurrentPage] = useState<number>(1);
  const [queryPageSize, setQueryPageSize] = useState<number>(50);
  const [queryError, setQueryError] = useState("");
  const [isHelpOpen, setIsHelpOpen] = useState(false);

  // Database Stream explorer state
  const [streams, setStreams] = useState<string[]>([]);
  const [selectedStream, setSelectedStream] = useState<string>("");
  const [browsedRecords, setBrowsedRecords] = useState<Record[]>([]);
  const [browsedCurrentPage, setBrowsedCurrentPage] = useState<number>(1);
  const [browsedPageSize, setBrowsedPageSize] = useState<number>(50);

  // Historical ingestion chart points (live tracking)
  const [ingestChart, setIngestChart] = useState<number[]>(
    Array.from({ length: 120 }, () => 0),
  );
  const [queryChart, setQueryChart] = useState<number[]>(
    Array.from({ length: 120 }, () => 0),
  );
  const [dbPortVal, setDbPortVal] = useState<string>("43121");
  const [webuiPortVal, setWebuiPortVal] = useState<string>("43120");

  const fetchConfig = async () => {
    try {
      const res = await fetch("/api/system/config");
      if (res.ok) {
        const data = await res.json();
        const webui_port = data.server.webui_port.toString();
        const db_port = data.server.db_port.toString();
        setDbPortVal(db_port);
        setWebuiPortVal(webui_port);
        setDbPort(webui_port);
        setSecurityMode(data.security.mode);
        addActivity(
          `System configuration loaded. DB Port: ${db_port}, Web UI Port: ${webui_port}`,
          "server",
          "success",
        );
        return { db_port, webui_port, mode: data.security.mode };
      }
    } catch (e) {
      console.error("Failed to fetch system config:", e);
    }
    return null;
  };

  const wsRef = useRef<WebSocket | null>(null);

  // Load config and check auth on mount
  useEffect(() => {
    const initApp = async () => {
      try {
        const cfg = await fetchConfig();
        if (cfg) {
          if (cfg.mode === "auth_key") {
            try {
              const status = await fetchAuthStatus();
              if (status.authenticated) {
                setIsAuthenticated(true);
              } else {
                setIsAuthenticated(false);
                addActivity(
                  "Administrative session requires security authentication. Challenge response pending.",
                  "server",
                  "warn",
                );
              }
            } catch (err) {
              console.error("Auth status check failed:", err);
              setIsAuthenticated(false);
            }
          } else {
            setIsAuthenticated(true);
          }
        } else {
          setIsAuthenticated(true);
        }
      } catch (err) {
        console.error("App initialization failed:", err);
      } finally {
        setAuthChecking(false);
      }
    };
    initApp();
  }, []);

  // Manage WebSocket connection and fetch initial streams when authenticated
  useEffect(() => {
    if (isAuthenticated) {
      connectWS();
      fetchStreams();
    } else {
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
      setWsConnected(false);
    }

    return () => {
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, [isAuthenticated]);

  // Slide charts forward with 0s if WS disconnected
  useEffect(() => {
    let fallbackInterval: any = null;
    if (!wsConnected) {
      fallbackInterval = setInterval(() => {
        setIngestChart((prev) => [...prev.slice(1), 0]);
        setQueryChart((prev) => [...prev.slice(1), 0]);
      }, 1000);
    }
    return () => {
      if (fallbackInterval) clearInterval(fallbackInterval);
    };
  }, [wsConnected]);

  const connectWS = () => {
    const wsProtocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${wsProtocol}//${window.location.host}/ws`;

    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;

    ws.onopen = () => {
      setWsConnected(true);
      setHasAttemptedConnect(true);
      addActivity(
        "Real-time live WebSocket connection established with backend.",
        "server",
        "success",
      );
    };

    ws.onclose = () => {
      setWsConnected(false);
      setHasAttemptedConnect(true);
      addActivity(
        "Real-time live WebSocket connection lost. Retrying...",
        "server",
        "error",
      );
      if (wsRef.current === ws) {
        setTimeout(() => {
          if (wsRef.current === ws) {
            connectWS();
          }
        }, 3000);
      }
    };

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === "metrics") {
          setMetrics({
            ram_usage: msg.ram_usage,
            disk_size: msg.disk_size,
            segments: msg.segments,
            sequence_id: msg.sequence_id,
            key_count: msg.key_count,
            total_streams: msg.total_streams,
          });

          const newSeq = msg.sequence_id;
          let ingestCount = 0;
          if (lastSequenceIdRef.current !== null) {
            ingestCount = Math.max(0, newSeq - lastSequenceIdRef.current);
          }
          lastSequenceIdRef.current = newSeq;

          // Slide charts forward
          setIngestChart((prev) => [...prev.slice(1), ingestCount]);

          const queryCount = queriesThisSecondRef.current;
          queriesThisSecondRef.current = 0;
          setQueryChart((prev) => [...prev.slice(1), queryCount]);

          // Log activities when database updates or state changes
          if (ingestCount > 0) {
            addActivity(
              `Appended ${ingestCount} record(s) to storage logs (seq #${newSeq - 1})`,
              "storage",
              "success",
            );
          }

          if (prevMetricsRef.current) {
            if (prevMetricsRef.current.key_count !== msg.key_count) {
              addActivity(
                `Database active key index updated: ${msg.key_count} keys in skiplist`,
                "storage",
                "info",
              );
            }
            if (prevMetricsRef.current.segments !== msg.segments) {
              addActivity(
                `New active log segment segment_${msg.segments.toString().padStart(5, "0")}.log allocated`,
                "storage",
                "success",
              );
            }
          }
          prevMetricsRef.current = msg;
        } else if (msg.type === "query_result") {
          setQueryResults((prev) => {
            // If continuous, append and limit to last 200 items to avoid lagging
            const updated = [msg.data, ...prev];
            return updated.slice(0, 200);
          });
          setQueryStats((prev) => ({
            count: prev.count + 1,
            timeMs: prev.timeMs,
          }));
          queriesThisSecondRef.current++;
          addActivity(
            `Streaming query match on [${msg.data.stream_name}] key "${msg.data.key}"`,
            "query",
            "info",
          );
        }
      } catch (e) {
        console.error("WS parse error:", e);
      }
    };
  };

  const fetchStreams = async () => {
    try {
      const res = await fetch(getDbApiUrl("/api/streams"));
      if (res.ok) {
        const data = await res.json();
        setStreams(data);
        const nonSystemStreams = data.filter((s: string) => s !== "auth_keys");
        if (
          nonSystemStreams.length > 0 &&
          (!selectedStream || selectedStream === "auth_keys")
        ) {
          setSelectedStream(nonSystemStreams[0]);
        }
        addActivity(
          `Discovered ${data.length} active database storage stream(s): ${data.join(", ") || "none"}`,
          "stream",
          "success",
        );
      }
    } catch (e) {
      console.error("Failed to fetch streams", e);
      addActivity(
        "Failed to fetch streams list from database REST API",
        "stream",
        "error",
      );
    }
  };

  // Browse records in selected stream
  useEffect(() => {
    if (selectedStream) {
      loadStreamRecords();
    }
  }, [selectedStream]);

  const loadStreamRecords = async () => {
    try {
      // Execute a quick scan query for the specific stream
      const res = await fetch(getDbApiUrl("/api/query"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          query: `from("${selectedStream}") | limit(10000)`,
        }),
      });
      if (res.ok) {
        const recs = await res.json();
        setBrowsedRecords(recs);
        setBrowsedCurrentPage(1); // Reset page to 1 on load
        addActivity(
          `Scanned and loaded ${recs.length} records for stream "${selectedStream}"`,
          "stream",
          "info",
        );
      }
    } catch (e) {
      console.error("Failed to load records for stream", e);
      addActivity(
        `Failed to load records for stream "${selectedStream}"`,
        "stream",
        "error",
      );
    }
  };

  // Trigger Compaction
  const handleCompaction = async () => {
    addActivity(
      "Triggering database log defragmentation & compaction...",
      "storage",
      "info",
    );
    try {
      const res = await postData("/api/compact");
      if (res) {
        alert(
          "Database log compaction completed successfully! Old un-compacted logs defragmented.",
        );
        addActivity(
          "Defragmentation & compaction completed successfully. Redundant logs purged and active pointers rewritten.",
          "storage",
          "success",
        );
        fetchStreams();
        if (selectedStream) loadStreamRecords();
      } else {
        addActivity(
          "Compaction command rejected by server",
          "storage",
          "error",
        );
      }
    } catch (e: any) {
      alert("Compaction failed");
      addActivity(
        `Compaction execution failed: ${e.message}`,
        "storage",
        "error",
      );
    }
  };

  const postData = async (url: string) => {
    try {
      const res = await fetch(getDbApiUrl(url), { method: "POST" });
      if (res.ok) return true;
    } catch (e) {
      console.error(e);
    }
    return false;
  };

  if (authChecking) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-zinc-950">
        <div className="flex flex-col items-center gap-4">
          <div className="animate-spin w-8 h-8 border-2 border-emerald-500 border-t-transparent rounded-full" />
          <p className="text-xs font-semibold tracking-widest text-emerald-500 uppercase animate-pulse">
            Initializing Gateway...
          </p>
        </div>
      </div>
    );
  }

  if (location === "/login") {
    return (
      <Login
        onLoginSuccess={() => {
          setIsAuthenticated(true);
          setLocation("/");
        }}
      />
    );
  }

  return (
    <>
      <div className="flex flex-col min-h-screen  bg-gray-100 dark:bg-zinc-800 text-zinc-900 dark:text-zinc-100 transition-colors duration-300">
        <style>{resolvedTheme === "dark" ? darkTheme : lightTheme}</style>

        {hasAttemptedConnect && !wsConnected && (
          <div className="fixed inset-0 z-50 backdrop-blur-md flex items-center justify-center p-4 select-none animate-fade-in">
            <div className="bg-white dark:bg-zinc-800 rounded-xl border border-zinc-200/60 dark:border-zinc-800/40 p-6 shadow-lg max-w-xs w-full text-center flex flex-col items-center space-y-4">
              <div className="relative flex items-center justify-center w-10 h-10 rounded-full bg-zinc-100 dark:bg-zinc-800 border border-zinc-200 dark:border-zinc-700 text-zinc-500 dark:text-zinc-400">
                <WifiOff className="w-5 h-5" />
              </div>

              <div className="space-y-1">
                <h3 className="text-xs font-bold text-zinc-900 dark:text-zinc-100 tracking-wider uppercase">
                  KondaDB Offline
                </h3>
                <p className="text-[11px] text-zinc-500 dark:text-zinc-400">
                  Real-time database socket lost
                </p>
              </div>

              <code className="block w-full bg-zinc-50 dark:bg-zinc-950/40 p-2 rounded text-zinc-600 dark:text-zinc-300 font-mono text-[11px] border border-zinc-200/60 dark:border-zinc-800/40">
                kondadb start
              </code>

              <button
                onClick={() => {
                  addActivity(
                    "Re-triggering connection attempt...",
                    "server",
                    "info",
                  );
                  window.location.reload();
                }}
                className="w-full py-2 px-4 rounded-md bg-primary hover:bg-primary-hover text-white text-xs font-bold uppercase tracking-wider transition-all active:scale-[0.98] flex items-center justify-center gap-1.5 shadow-sm"
              >
                <RefreshCw className="w-3.5 h-3.5" />
                Reconnect
              </button>
            </div>
          </div>
        )}

        {/* TOP NAVBAR & FIXED SIDEBAR */}
        <Navbar
          activeTab={activeTab}
          setActiveTab={setActiveTab}
          theme={theme}
          setTheme={setTheme}
          securityMode={securityMode}
          isAuthenticated={isAuthenticated}
          onLogout={async () => {
            try {
              await submitSystemLogout();
              setIsAuthenticated(false);
              addActivity(
                "Symmetric session terminated successfully. Redirected to login.",
                "server",
                "info",
              );
            } catch (err: any) {
              console.error("Logout failed:", err);
              addActivity(
                `Failed to gracefully terminate auth session: ${err.message}`,
                "server",
                "error",
              );
            }
          }}
        />

        {/* MAIN PANEL CONTENT */}
        <main className="flex-1 flex flex-col overflow-y-auto pl-0 md:pl-64 pt-14 md:pt-0">
          <div className="flex-1 p-8 space-y-8">
            <Suspense
              fallback={
                <div className="flex items-center justify-center py-20">
                  <div className="animate-spin w-6 h-6 border-2 border-primary border-t-transparent rounded-full" />
                </div>
              }
            >
              <Switch>
                <Route path="/">
                  <Dashboard
                    metrics={metrics}
                    streams={streams}
                    wsConnected={wsConnected}
                    activities={activities}
                    setActivities={setActivities}
                    ingestChart={ingestChart}
                    queryChart={queryChart}
                    activityFilter={activityFilter}
                    setActivityFilter={setActivityFilter}
                    handleCompaction={handleCompaction}
                    dbPort={dbPortVal}
                    webuiPort={webuiPortVal}
                    resolvedTheme={resolvedTheme}
                  />
                </Route>
                <Route path="/dashboard">
                  <Dashboard
                    metrics={metrics}
                    streams={streams}
                    wsConnected={wsConnected}
                    activities={activities}
                    setActivities={setActivities}
                    ingestChart={ingestChart}
                    queryChart={queryChart}
                    activityFilter={activityFilter}
                    setActivityFilter={setActivityFilter}
                    handleCompaction={handleCompaction}
                    dbPort={dbPortVal}
                    webuiPort={webuiPortVal}
                    resolvedTheme={resolvedTheme}
                  />
                </Route>
                <Route path="/query">
                  <QueryConsole
                    query={query}
                    setQuery={setQuery}
                    queryResults={queryResults}
                    setQueryResults={setQueryResults}
                    isQueryRunning={isQueryRunning}
                    setIsQueryRunning={setIsQueryRunning}
                    queryStats={queryStats}
                    setQueryStats={setQueryStats}
                    continuousStream={continuousStream}
                    setContinuousStream={setContinuousStream}
                    queryCurrentPage={queryCurrentPage}
                    setQueryCurrentPage={setQueryCurrentPage}
                    queryPageSize={queryPageSize}
                    setQueryPageSize={setQueryPageSize}
                    queryError={queryError}
                    setQueryError={setQueryError}
                    resolvedTheme={resolvedTheme}
                    wsConnected={wsConnected}
                    wsRef={wsRef}
                    queriesThisSecondRef={queriesThisSecondRef}
                    addActivity={addActivity}
                    setIsHelpOpen={setIsHelpOpen}
                    streams={streams}
                  />
                </Route>
                <Route path="/explorer">
                  <StreamExplorer
                    streams={streams}
                    selectedStream={selectedStream}
                    setSelectedStream={setSelectedStream}
                    browsedRecords={browsedRecords}
                    setBrowsedRecords={setBrowsedRecords}
                    browsedCurrentPage={browsedCurrentPage}
                    setBrowsedCurrentPage={setBrowsedCurrentPage}
                    browsedPageSize={browsedPageSize}
                    setBrowsedPageSize={setBrowsedPageSize}
                    loadStreamRecords={loadStreamRecords}
                    fetchStreams={fetchStreams}
                    addActivity={addActivity}
                    resolvedTheme={resolvedTheme}
                  />
                </Route>
                <Route path="/security">
                  <Security
                    addActivity={addActivity}
                    resolvedTheme={resolvedTheme}
                  />
                </Route>
                <Route>
                  <div className="p-8 text-center bg-white dark:bg-gray-500 rounded-md border border-slate-200 dark:border-slate-800">
                    <h3 className="text-lg font-bold text-slate-900 dark:text-white mb-2">
                      Page Not Found
                    </h3>
                    <p className="text-sm text-slate-500 dark:text-slate-400 mb-6">
                      The requested path does not exist on this server.
                    </p>
                    <button
                      onClick={() => setLocation("/")}
                      className="py-2.5 px-5 rounded bg-primary hover:bg-primary-hover text-white text-xs font-semibold active:scale-[0.98] transition-all"
                    >
                      Return to Dashboard
                    </button>
                  </div>
                </Route>
              </Switch>
            </Suspense>
          </div>
        </main>

        {/* HELP GUIDE MODAL DIALOG */}
        <QueryGuideModal
          isOpen={isHelpOpen}
          onClose={() => setIsHelpOpen(false)}
          setQuery={setQuery}
          addActivity={addActivity}
        />
      </div>
    </>
  );
}
