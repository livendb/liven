import { getDbApiUrl } from "./api";

// 1. Submit Symmetric Auth Key Login
export async function submitSystemLogin(token: string): Promise<{ status: string; user_id: string; permissions: string }> {
  const res = await fetch(getDbApiUrl("/api/system/auth/login"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ token }),
    credentials: "include", // Ensures that the secure session cookie gets set
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Authentication failed. Invalid auth key.");
  }

  return res.json();
}

// 1.5. Submit Session Auth Logout
export async function submitSystemLogout(): Promise<{ status: string; message: string }> {
  const res = await fetch(getDbApiUrl("/api/system/auth/logout"), {
    method: "POST",
    credentials: "include", // Transmits and handles the cleared session cookie
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Logout failed.");
  }

  return res.json();
}


// 2. Fetch Cookie-based Session Auth Status
export async function fetchAuthStatus(): Promise<{ authenticated: boolean; user_id?: string }> {
  const res = await fetch(getDbApiUrl("/api/system/auth/status"), {
    method: "GET",
    credentials: "include", // Ensures the session cookie is transmitted
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Failed to fetch authentication status.");
  }

  return res.json();
}

export interface AuthKeyRecord {
  key_id: string;
  role: string;
  auth_key: string; // BLAKE3 hex storage hash
  status: string; // 'active', 'revoked'
}

export interface GenerateKeyResponse {
  key_id: string;
  role: string;
  auth_key_hash: string;
  status: string;
  raw_key: string;
}

// 3. Fetch Listed Symmetric Auth Keys (Admin only)
export async function fetchAuthKeys(): Promise<AuthKeyRecord[]> {
  const res = await fetch(getDbApiUrl("/api/system/auth/keys"), {
    method: "GET",
    credentials: "include",
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Failed to fetch keys.");
  }

  return res.json();
}

// 4. Generate a New Symmetric Auth Key
export async function generateAuthKey(
  keyId: string,
  role: string
): Promise<GenerateKeyResponse> {
  const res = await fetch(getDbApiUrl("/api/system/auth/keys"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ key_id: keyId, role }),
    credentials: "include",
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Failed to generate auth key.");
  }

  return res.json();
}

// 4.5. Revoke an Existing Auth Key
export async function revokeAuthKey(keyId: string): Promise<{ status: string }> {
  const res = await fetch(getDbApiUrl("/api/system/auth/keys/revoke"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ key_id: keyId }),
    credentials: "include",
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Failed to revoke key.");
  }

  return res.json();
}

// 5. System Config Fetching
export async function fetchSystemConfig(): Promise<{ security_mode: string; allow_local_auto_generation?: boolean }> {
  const res = await fetch(getDbApiUrl("/api/system/config"));
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Failed to fetch system configuration.");
  }
  return res.json();
}

// 6. Fetch Listed Streams
export async function fetchStreams(): Promise<string[]> {
  const res = await fetch(getDbApiUrl("/api/streams"), {
    credentials: "include",
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Failed to fetch streams.");
  }
  return res.json();
}

// 7. Query execution helper
export async function executeQuery(queryStr: string): Promise<any[]> {
  const res = await fetch(getDbApiUrl("/api/query"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query: queryStr }),
    credentials: "include",
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Query execution failed.");
  }
  return res.json();
}

// 8. Ingestion helper
export async function executeIngest(streamName: string, records: any[]): Promise<{ count: number }> {
  const res = await fetch(getDbApiUrl("/api/ingest"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ stream: streamName, records }),
    credentials: "include",
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Ingest failed.");
  }
  return res.json();
}

// 9. Compaction trigger helper
export async function executeCompact(streamName: string): Promise<any> {
  const res = await fetch(getDbApiUrl(`/api/compact?stream=${encodeURIComponent(streamName)}`), {
    method: "POST",
    credentials: "include",
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Compaction failed.");
  }
  return res.json();
}
