// Client types for VivoDB SPA

export interface Record {
  sequence_id: number;
  timestamp: number;
  type_tag: number;
  flags: number;
  stream_name: string;
  key: string;
  value: any;
}

export interface Metrics {
  ram_usage: number;
  disk_size: number;
  segments: number;
  sequence_id: number;
  key_count: number;
  total_streams: number;
}

export interface ActivityLog {
  id: string;
  timestamp: string;
  type: "info" | "success" | "warn" | "error";
  category: "storage" | "query" | "server" | "stream";
  message: string;
}
