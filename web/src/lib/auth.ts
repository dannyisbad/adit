const STORAGE_KEY = "adit.authToken";

export function getDaemonAuthToken(): string | null {
  try {
    const token = window.localStorage.getItem(STORAGE_KEY);
    return typeof token === "string" && token.trim().length > 0 ? token.trim() : null;
  } catch {
    return null;
  }
}

export function setDaemonAuthToken(token: string): void {
  const normalized = token.trim();
  if (!normalized) {
    clearDaemonAuthToken();
    return;
  }

  try {
    window.localStorage.setItem(STORAGE_KEY, normalized);
  } catch {
    // Ignore storage failures; callers can still decide how to recover.
  }
}

export function clearDaemonAuthToken(): void {
  try {
    window.localStorage.removeItem(STORAGE_KEY);
  } catch {
    // Ignore storage failures.
  }
}

export function hasSavedDaemonAuthToken(): boolean {
  return getDaemonAuthToken() !== null;
}
