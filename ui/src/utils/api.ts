// VivoDB API & Utility Helpers

export let dbPort = typeof window !== "undefined" ? (window.location.port || "43120") : "43120";

export function setDbPort(port: string) {
  dbPort = port;
}

export const getDbApiUrl = (path: string): string => {
  return path;
};

// Helper function to recursively parse any stringified JSON
export function parseStringifiedJson(val: any): any {
  if (typeof val === "string") {
    const trimmed = val.trim();
    if (
      (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
      (trimmed.startsWith("[") && trimmed.endsWith("]"))
    ) {
      try {
        const parsed = JSON.parse(trimmed);
        return parseStringifiedJson(parsed);
      } catch {
        return val;
      }
    }
    return val;
  }
  if (Array.isArray(val)) {
    return val.map(parseStringifiedJson);
  }
  if (val !== null && typeof val === "object") {
    const res: { [key: string]: any } = {};
    for (const key of Object.keys(val)) {
      res[key] = parseStringifiedJson(val[key]);
    }
    return res;
  }
  return val;
}

// Helper formatting for bytes
export const formatBytes = (bytes: number): string => {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
};
