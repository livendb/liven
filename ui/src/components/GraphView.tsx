import { useState, useMemo, useCallback, useRef, useEffect } from "react";
import ForceGraph3D from "react-force-graph-3d";
import type { Record as LivenRecord } from "../types";
import { ZoomIn, ZoomOut, Maximize } from "lucide-react";

interface GraphNode {
  id: string;
  label: string;
  stream: string;
  val: number;
  color: string;
  record: LivenRecord;
}

interface GraphLink {
  source: string;
  target: string;
  label: string;
}

interface GraphData {
  nodes: GraphNode[];
  links: GraphLink[];
}

const STREAM_COLORS: Record<string, string> = {
  prompts: "#10b981",
  responses: "#3b82f6",
  memory: "#8b5cf6",
  orders: "#f59e0b",
  shipments: "#06b6d4",
  deliveries: "#84cc16",
  transactions: "#ef4444",
  logins: "#ec4899",
  telemetry: "#14b8a6",
  auth: "#f97316",
  users: "#6366f1",
  events: "#0ea5e9",
};

function getStreamColor(stream: string): string {
  return STREAM_COLORS[stream] || "#6b7280";
}

function extractGraphData(records: LivenRecord[]): GraphData {
  const nodesMap = new Map<string, GraphNode>();
  const linksMap = new Map<string, GraphLink>();

  for (const record of records) {
    nodesMap.set(record.key, {
      id: record.key,
      label: record.key,
      stream: record.stream_name,
      val: 5,
      color: getStreamColor(record.stream_name),
      record,
    });

    let valueObj: any = record.value;
    if (valueObj && typeof valueObj === "object" && "String" in valueObj) {
      try {
        valueObj = JSON.parse(valueObj.String);
      } catch {
        continue;
      }
    }

    if (!valueObj || typeof valueObj !== "object") continue;

    const userId = valueObj.user_id;
    if (userId && typeof userId === "string") {
      const userIdKey = `user:${userId}`;
      if (!nodesMap.has(userIdKey)) {
        nodesMap.set(userIdKey, {
          id: userIdKey,
          label: userId,
          stream: "users",
          val: 3,
          color: getStreamColor("users"),
          record: record,
        });
      }

      const linkKey = `${record.key}→${userIdKey}`;
      if (!linksMap.has(linkKey)) {
        linksMap.set(linkKey, {
          source: record.key,
          target: userIdKey,
          label: "belongs_to",
        });
      }
    }

    for (let i = 1; i <= valueObj.correlated_count; i++) {
      const correlated = valueObj[`correlated_${i}`];
      if (correlated && correlated.login_id) {
        const loginKey = correlated.login_id;
        if (!nodesMap.has(loginKey)) {
          nodesMap.set(loginKey, {
            id: loginKey,
            label: loginKey,
            stream: "logins",
            val: 2,
            color: getStreamColor("logins"),
            record: record,
          });
        }

        const linkKey = `${record.key}→${loginKey}`;
        if (!linksMap.has(linkKey)) {
          linksMap.set(linkKey, {
            source: record.key,
            target: loginKey,
            label: `correlated (${correlated.device})`,
          });
        }
      }
    }
  }

  const userLogins = new Map<string, Set<string>>();
  for (const link of linksMap.values()) {
    if (
      link.label.startsWith("correlated") &&
      link.target.startsWith("login_")
    ) {
      for (const [nodeId, _] of nodesMap) {
        if (
          nodeId.startsWith("user:") &&
          linksMap.has(`${link.source}→${nodeId}`)
        ) {
          if (!userLogins.has(nodeId)) userLogins.set(nodeId, new Set());
          userLogins.get(nodeId)!.add(link.target);
        }
      }
    }
  }

  for (const [userId, logins] of userLogins) {
    for (const loginId of logins) {
      const linkKey = `${userId}→${loginId}`;
      if (!linksMap.has(linkKey)) {
        linksMap.set(linkKey, {
          source: userId,
          target: loginId,
          label: "has_login",
        });
      }
    }
  }

  return {
    nodes: Array.from(nodesMap.values()),
    links: Array.from(linksMap.values()),
  };
}

interface GraphViewProps {
  records: LivenRecord[];
}

function getNodeColor(node: any): string {
  return STREAM_COLORS[node.group] || "#6b7280";
}

function getNodeSize(node: any): number {
  switch (node.group) {
    case "transactions":
      return 8;
    case "users":
      return 5;
    case "logins":
      return 3;
    default:
      return 4;
  }
}

// Node detail component with scrollable content and formatted JSON display
function NodeDetailPanel({
  node,
  onClose,
}: {
  node: any;
  onClose: () => void;
}) {
  // Format JSON object as readable HTML
  const formatJSONAsHTML = (obj: Record<string, any>): React.ReactNode => {
    return (
      <div className="space-y-2 bg-gray-50 dark:bg-gray-800/30 rounded-lg p-3 font-mono text-xs">
        {Object.entries(obj).map(([key, value]) => {
          let displayValue: React.ReactNode = value;

          // If value is an object, recursively format it
          if (value && typeof value === "object" && !Array.isArray(value)) {
            displayValue = formatJSONAsHTML(value);
          }
          // If value is an array
          else if (Array.isArray(value)) {
            displayValue = (
              <div className="pl-4">
                {value.map((item, idx) => (
                  <div key={idx} className="mb-1">
                    {typeof item === "object"
                      ? formatJSONAsHTML(item)
                      : String(item)}
                  </div>
                ))}
              </div>
            );
          }
          // If value is a string that looks like JSON, parse it
          else if (
            typeof value === "string" &&
            (value.startsWith("{") || value.startsWith("["))
          ) {
            try {
              const parsed = JSON.parse(value);
              displayValue = formatJSONAsHTML(parsed);
            } catch {
              displayValue = (
                <span className="text-gray-600 dark:text-gray-400 break-all">
                  {value}
                </span>
              );
            }
          }
          // Regular value
          else {
            displayValue = (
              <span className="text-gray-600 dark:text-gray-400 break-all">
                {String(value)}
              </span>
            );
          }

          return (
            <div
              key={key}
              className="border-l-2 border-purple-300 dark:border-purple-700 pl-3"
            >
              <div className="flex items-start gap-2">
                <span className="text-purple-600 dark:text-purple-400 font-semibold min-w-[80px]">
                  {key}:
                </span>
                <div className="flex-1">{displayValue}</div>
              </div>
            </div>
          );
        })}
      </div>
    );
  };

  const formatValue = (
    value: any,
  ): { label: string; display: string | React.ReactNode } => {
    if (value === null || value === undefined) {
      return { label: "Value", display: "null" };
    }

    if (typeof value === "string") {
      try {
        const parsed = JSON.parse(value);
        if (typeof parsed === "object") {
          return { label: "Data", display: formatObject(parsed) };
        }
        return { label: "Value", display: value };
      } catch {
        return { label: "Value", display: value };
      }
    }

    if (typeof value === "object") {
      return { label: "Data", display: formatObject(value) };
    }

    if (typeof value === "number") {
      return { label: "Amount", display: `$${value.toLocaleString()}` };
    }

    if (typeof value === "boolean") {
      return { label: "Status", display: value ? "✅ True" : "❌ False" };
    }

    return { label: "Value", display: String(value) };
  };

  const formatObject = (obj: Record<string, any>): React.ReactNode => {
    const excludeFields = ["correlated_count", "timestamp", "txn_id"];

    // Separate regular fields from correlated fields and JSON objects
    const regularEntries: [string, any][] = [];
    const correlatedEntries: [string, any][] = [];
    const jsonEntries: [string, any][] = [];

    Object.entries(obj).forEach(([key, val]) => {
      if (excludeFields.includes(key)) return;

      // Check if value is an object or parsable JSON string
      let isJsonObject = false;
      let parsedVal = val;

      if (typeof val === "string") {
        try {
          const parsed = JSON.parse(val);
          if (typeof parsed === "object" && parsed !== null) {
            isJsonObject = true;
            parsedVal = parsed;
          }
        } catch {
          // Not JSON
        }
      } else if (typeof val === "object" && val !== null) {
        isJsonObject = true;
      }

      if (key.toLowerCase().startsWith("correlated_")) {
        correlatedEntries.push([key, isJsonObject ? parsedVal : val]);
      } else if (isJsonObject) {
        jsonEntries.push([key, parsedVal]);
      } else {
        regularEntries.push([key, val]);
      }
    });

    return (
      <div className="space-y-4">
        {/* Regular fields */}
        {regularEntries.length > 0 && (
          <div className="space-y-2">
            {regularEntries.map(([key, val]) => {
              let displayValue = val;
              if (typeof val === "object") {
                displayValue = JSON.stringify(val);
              }
              if (
                typeof val === "number" &&
                (key === "amount" || key.includes("price"))
              ) {
                displayValue = `$${val.toLocaleString()}`;
              }
              return (
                <div
                  key={key}
                  className="border-b border-gray-200 dark:border-gray-800 pb-2 last:border-0"
                >
                  <span className="text-gray-400 dark:text-gray-500 text-xs uppercase tracking-wide block mb-1">
                    {key}:
                  </span>
                  <span className="text-gray-700 dark:text-gray-300 text-sm break-all block">
                    {String(displayValue)}
                  </span>
                </div>
              );
            })}
          </div>
        )}

        {/* JSON Object fields (formatted as HTML) */}
        {jsonEntries.length > 0 && (
          <div>
            {jsonEntries.map(([key, val]) => (
              <div key={key}>
                <span className="text-gray-400 dark:text-gray-500 text-xs uppercase tracking-wide block mb-2">
                  {key}:
                </span>
                {formatJSONAsHTML(val)}
              </div>
            ))}
          </div>
        )}

        {/* Correlated fields as compact tags */}
        {correlatedEntries.length > 0 && (
          <div>
            <span className="text-gray-400 dark:text-gray-500 text-xs uppercase tracking-wide block mb-2">
              Correlated Events:
            </span>
            <div className="flex flex-wrap gap-2">
              {correlatedEntries.map(([key, val]) => {
                const isObject = typeof val === "object" && val !== null;
                const device = isObject ? val.device : null;
                const loginId = isObject ? val.login_id : null;
                const ip = isObject ? val.ip : null;

                const tooltipParts = [];
                if (loginId) tooltipParts.push(`login: ${loginId}`);
                if (ip) tooltipParts.push(`ip: ${ip}`);
                const tooltipText = tooltipParts.join(" • ");

                return (
                  <div
                    key={key}
                    className="group relative inline-flex items-center gap-1.5 px-2 py-1 rounded-md bg-gray-100 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-xs cursor-help"
                    title={tooltipText || key}
                  >
                    <span className="font-mono text-purple-500 dark:text-purple-400 text-[10px]">
                      {key}
                    </span>
                    {device && (
                      <span className="text-blue-500 dark:text-blue-400">
                        {device}
                      </span>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderNodeValue = () => {
    if (!node.value) {
      return (
        <div className="text-gray-500 dark:text-gray-400 text-sm italic">
          No additional data available
        </div>
      );
    }

    const { label, display } = formatValue(node.value);
    return (
      <div>
        <span className="text-gray-400 dark:text-gray-500 text-xs uppercase tracking-wide block mb-2">
          {label}:
        </span>
        <div className="max-h-64 overflow-y-auto pr-1">{display}</div>
      </div>
    );
  };

  const getNodeIcon = (group: string): string => {
    switch (group) {
      case "transactions":
        return "💳";
      case "users":
        return "👤";
      case "logins":
        return "🔐";
      case "prompts":
        return "💬";
      case "responses":
        return "📝";
      case "memory":
        return "🧠";
      default:
        return "📦";
    }
  };

  return (
    <div className="absolute bottom-6 right-6 z-20 bg-white/95 dark:bg-gray-900/95 backdrop-blur-sm rounded-xl border border-gray-200 dark:border-gray-700 shadow-xl max-w-sm w-96 transition-all duration-200 animate-in slide-in-from-bottom-2 flex flex-col">
      {/* Fixed Header with Close Button */}
      <div className="flex items-center justify-between p-4 border-b border-gray-200 dark:border-gray-800 shrink-0">
        <div className="flex items-center gap-2">
          <span className="text-xl">{getNodeIcon(node.group)}</span>
          <h3
            className="font-bold"
            style={{ color: node.color || getNodeColor(node) }}
          >
            {node.group?.toUpperCase() || "NODE"}
          </h3>
        </div>
        <button
          onClick={onClose}
          className="text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-full p-1.5 transition-colors"
          aria-label="Close"
        >
          <svg
            className="w-4 h-4"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M6 18L18 6M6 6l12 12"
            />
          </svg>
        </button>
      </div>

      {/* Scrollable Content Area - Only this scrolls */}
      <div
        className="p-4 space-y-3 overflow-y-auto flex-1"
        style={{ maxHeight: "calc(60vh - 80px)" }}
      >
        {/* ID */}
        <div>
          <span className="text-gray-400 dark:text-gray-500 text-xs uppercase tracking-wide">
            ID:
          </span>
          <p className="text-gray-600 dark:text-gray-300 font-mono text-sm break-all mt-0.5">
            {node.id}
          </p>
        </div>

        {/* Name */}
        <div>
          <span className="text-gray-400 dark:text-gray-500 text-xs uppercase tracking-wide">
            Name:
          </span>
          <p className="text-gray-800 dark:text-gray-200 font-medium text-sm mt-0.5">
            {node.name}
          </p>
        </div>

        {/* Stream/Badge */}
        {node.group && (
          <div className="flex items-center gap-2 pt-1">
            <span className="text-xs px-2 py-0.5 rounded-full bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-300">
              {node.group}
            </span>
          </div>
        )}

        {/* Value/Data Section */}
        <div className="pt-2 border-t border-gray-200 dark:border-gray-800">
          {renderNodeValue()}
        </div>
      </div>
    </div>
  );
}

export default function GraphView({ records }: GraphViewProps) {
  const fgRef = useRef<any>();
  const containerRef = useRef<HTMLDivElement>(null);
  const [selectedNode, setSelectedNode] = useState<any>(null);
  const [dimensions, setDimensions] = useState({ width: 800, height: 500 });
  const stabilizedRef = useRef(false);

  const graphData = useMemo(() => {
    return extractGraphData(records);
  }, [records]);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    let timeoutId: NodeJS.Timeout;
    const updateDimensions = () => {
      clearTimeout(timeoutId);
      timeoutId = setTimeout(() => {
        setDimensions({
          width: el.clientWidth || 800,
          height: el.clientHeight || 600,
        });
      }, 100);
    };

    updateDimensions();
    const ro = new ResizeObserver(() => updateDimensions());
    ro.observe(el);
    return () => {
      clearTimeout(timeoutId);
      ro.disconnect();
    };
  }, []);

  useEffect(() => {
    stabilizedRef.current = false;
  }, [graphData]);

  const handleEngineStop = useCallback(() => {
    if (!fgRef.current || stabilizedRef.current) return;
    stabilizedRef.current = true;
  }, []);

  const handleNodeClick = useCallback(
    (node: any) => {
      if (!fgRef.current || !node) return;

      let val = records.filter((item) => item.key == node.id)[0]?.value;
      let parsedValue = null;

      if (val) {
        if (val.hasOwnProperty("String")) {
          try {
            parsedValue = JSON.parse(val.String);
          } catch {
            parsedValue = val.String;
          }
        } else if (val.hasOwnProperty("Int")) {
          parsedValue = val.Int;
        } else if (val.hasOwnProperty("UInt")) {
          parsedValue = val.UInt;
        } else if (val.hasOwnProperty("Float")) {
          parsedValue = val.Float;
        } else if (val.hasOwnProperty("Bool")) {
          parsedValue = val.Bool;
        } else if (val.hasOwnProperty("Null")) {
          parsedValue = null;
        } else if (val.hasOwnProperty("Binary")) {
          parsedValue = "[Binary Data]";
        } else if (val.hasOwnProperty("Array")) {
          parsedValue = val.Array;
        } else if (val.hasOwnProperty("Vector")) {
          parsedValue = `[Vector: ${val.Vector.length} dimensions]`;
        } else {
          parsedValue = val;
        }

        setSelectedNode({ ...node, value: parsedValue });
      } else {
        setSelectedNode(node);
      }

      const distance = 40;
      const distRatio = 1 + distance / Math.hypot(node.x, node.y, node.z);
      fgRef.current.cameraPosition(
        { x: node.x * distRatio, y: node.y * distRatio, z: node.z * distRatio },
        node,
        500,
      );
    },
    [records],
  );

  // Zoom controls - using camera distance for 3D
  const handleZoomIn = useCallback(() => {
    if (!fgRef.current) return;
    const camera = fgRef.current.camera();
    const currentPosition = camera.position;
    // Move camera closer (reduce distance)
    fgRef.current.cameraPosition(
      {
        x: currentPosition.x * 0.8,
        y: currentPosition.y * 0.8,
        z: currentPosition.z * 0.8,
      },
      { x: 0, y: 0, z: 0 },
      200,
    );
  }, []);

  const handleZoomOut = useCallback(() => {
    if (!fgRef.current) return;
    const camera = fgRef.current.camera();
    const currentPosition = camera.position;
    // Move camera further (increase distance)
    fgRef.current.cameraPosition(
      {
        x: currentPosition.x * 1.2,
        y: currentPosition.y * 1.2,
        z: currentPosition.z * 1.2,
      },
      { x: 0, y: 0, z: 0 },
      200,
    );
  }, []);

  const handleZoomToFit = useCallback(() => {
    if (!fgRef.current) return;
    // Calculate bounding box of all nodes to determine optimal camera distance
    const nodes = formattedGraphData?.nodes || [];
    if (nodes.length === 0) return;

    let minX = Infinity,
      maxX = -Infinity;
    let minY = Infinity,
      maxY = -Infinity;
    let minZ = Infinity,
      maxZ = -Infinity;

    nodes.forEach((node: any) => {
      if (node.x !== undefined) {
        minX = Math.min(minX, node.x);
        maxX = Math.max(maxX, node.x);
        minY = Math.min(minY, node.y);
        maxY = Math.max(maxY, node.y);
        minZ = Math.min(minZ, node.z);
        maxZ = Math.max(maxZ, node.z);
      }
    });

    // If nodes don't have positions yet, use center
    const centerX = (minX + maxX) / 2 || 0;
    const centerY = (minY + maxY) / 2 || 0;
    const centerZ = (minZ + maxZ) / 2 || 0;

    // Calculate distance needed to fit all nodes
    const width = maxX - minX || 100;
    const height = maxY - minY || 100;
    const depth = maxZ - minZ || 100;
    const maxDimension = Math.max(width, height, depth);

    // Camera distance proportional to graph size + padding
    const distance = maxDimension * 1.5 + 50;

    fgRef.current.cameraPosition(
      { x: centerX, y: centerY, z: centerZ + distance },
      { x: centerX, y: centerY, z: centerZ },
      500,
    );
  }, [graphData]);

  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === "Escape" && selectedNode) {
        setSelectedNode(null);
      }
    };
    window.addEventListener("keydown", handleEscape);
    return () => window.removeEventListener("keydown", handleEscape);
  }, [selectedNode]);

  if (graphData.nodes.length === 0 || dimensions.width === 0) {
    return null;
  }

  const formattedGraphData = {
    nodes: graphData.nodes.map((node) => ({
      id: node.id,
      name: node.label,
      val: node.val,
      color: node.color,
      group: node.stream,
    })),
    links: graphData.links.map((link) => ({
      source: link.source,
      target: link.target,
      label: link.label,
    })),
  };

  return (
    <div
      ref={containerRef}
      className="w-full h-[500px] overflow-hidden bg-white dark:bg-zinc-900/50 relative"
    >
      <ForceGraph3D
        ref={fgRef}
        graphData={formattedGraphData}
        width={dimensions.width}
        height={dimensions.height}
        backgroundColor="rgba(0,0,0,0)"
        nodeLabel="name"
        nodeColor={getNodeColor}
        nodeVal={getNodeSize}
        linkLabel="label"
        linkWidth={1.5}
        linkDirectionalArrowLength={4}
        linkDirectionalArrowRelPos={1}
        linkColor={() => "#a1a1aa"}
        onNodeClick={handleNodeClick}
        onEngineStop={handleEngineStop}
        cooldownTicks={Infinity}
        cooldownTime={3000}
        d3AlphaDecay={0.03}
        d3VelocityDecay={0.5}
        d3AlphaMin={0.005}
        warmupTicks={1000}
        enableNodeDrag={true}
        enableNavigationControls={true}
        showNavInfo={true}
      />

      {/* Zoom Controls */}
      <div className="absolute top-4 right-4 z-10 flex flex-col gap-2">
        <button
          onClick={handleZoomIn}
          className="bg-white/90 dark:bg-gray-900/90 backdrop-blur-sm p-2 rounded-lg border border-gray-200 dark:border-gray-700 shadow-md hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors"
          aria-label="Zoom In"
          title="Zoom In"
        >
          <ZoomIn className="w-5 h-5 text-gray-700 dark:text-gray-300" />
        </button>
        <button
          onClick={handleZoomOut}
          className="bg-white/90 dark:bg-gray-900/90 backdrop-blur-sm p-2 rounded-lg border border-gray-200 dark:border-gray-700 shadow-md hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors"
          aria-label="Zoom Out"
          title="Zoom Out"
        >
          <ZoomOut className="w-5 h-5 text-gray-700 dark:text-gray-300" />
        </button>
        <button
          onClick={handleZoomToFit}
          className="bg-white/90 dark:bg-gray-900/90 backdrop-blur-sm p-2 rounded-lg border border-gray-200 dark:border-gray-700 shadow-md hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors"
          aria-label="Zoom to Fit"
          title="Zoom to Fit"
        >
          <Maximize className="w-5 h-5 text-gray-700 dark:text-gray-300" />
        </button>
      </div>

      {/* Stats overlay */}
      <div className="absolute bottom-4 left-4 z-10 bg-white/80 dark:bg-gray-900/80 backdrop-blur-sm rounded-lg px-3 py-1.5 text-xs text-gray-600 dark:text-gray-300 border border-gray-200 dark:border-gray-800">
        <span>
          Nodes: {formattedGraphData.nodes.length} | Links:{" "}
          {formattedGraphData.links.length}
        </span>
      </div>

      {/* Selected node detail panel */}
      {selectedNode && (
        <NodeDetailPanel
          node={selectedNode}
          onClose={() => setSelectedNode(null)}
        />
      )}
    </div>
  );
}
