import { useState, useEffect, useCallback, useRef } from "react";
import { api } from "../lib/api";
import { hashColor, initials } from "../lib/utils";
import type { ContactRecord } from "../lib/types";

export function ContactsList() {
  const [contacts, setContacts] = useState<ContactRecord[]>([]);
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(true);
  const [total, setTotal] = useState(0);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  useEffect(() => {
    api.listContacts()
      .then((res) => {
        setContacts(res.contacts);
        setTotal(res.count);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const handleSearch = useCallback((value: string) => {
    setSearch(value);
    clearTimeout(debounceRef.current);

    if (!value.trim()) {
      api.listContacts()
        .then((res) => {
          setContacts(res.contacts);
          setTotal(res.count);
        })
        .catch(() => {});
      return;
    }

    debounceRef.current = setTimeout(() => {
      api.searchContacts(value.trim())
        .then((res) => setContacts(res.contacts))
        .catch(() => {});
    }, 250);
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-16">
        <div className="w-6 h-6 rounded-full border-2 border-border border-t-accent animate-spin" />
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full">
      {/* Search */}
      <div className="px-5 py-3 border-b border-border bg-sidebar">
        <div className="relative max-w-2xl mx-auto">
          <svg
            className="absolute left-3 top-1/2 -translate-y-1/2 text-text-secondary pointer-events-none"
            width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor"
            strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
          >
            <circle cx="11" cy="11" r="8" />
            <line x1="21" y1="21" x2="16.65" y2="16.65" />
          </svg>
          <input
            type="text"
            value={search}
            onChange={(e) => handleSearch(e.target.value)}
            placeholder={`Search ${total.toLocaleString()} contacts\u2026`}
            className="w-full pl-9 pr-3 py-2 rounded-lg bg-base border border-border text-sm text-text-primary placeholder:text-text-secondary outline-none focus:border-accent transition-colors"
          />
        </div>
      </div>

      {/* List */}
      <div className="flex-1 overflow-y-auto">
        {contacts.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-16 px-6">
            <svg
              className="text-text-secondary/40 mb-3"
              width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor"
              strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"
            >
              <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" />
              <circle cx="12" cy="7" r="4" />
            </svg>
            <p className="text-sm text-text-secondary text-center">
              {search ? "No contacts match your search." : "No contacts synced yet."}
            </p>
            {!search && (
              <p className="text-xs text-text-secondary/70 text-center mt-1">
                Connect your iPhone and sync to see contacts here.
              </p>
            )}
          </div>
        ) : (
          <div className="max-w-2xl mx-auto">
            {search && (
              <p
                className="px-5 pt-3 pb-1 text-[11px] text-text-secondary"
                style={{ fontFamily: "var(--font-mono)" }}
              >
                {contacts.length} result{contacts.length !== 1 ? "s" : ""}
              </p>
            )}
            <div className="divide-y divide-border/40">
              {contacts.map((contact, i) => (
                <ContactRow
                  key={contact.uniqueIdentifier ?? `${contact.displayName}-${i}`}
                  contact={contact}
                  index={i}
                />
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function ContactRow({ contact, index }: { contact: ContactRecord; index: number }) {
  const name = contact.displayName || "Unknown";
  const color = hashColor(name);
  const ini = initials(name);
  const phone = contact.phones[0];
  const email = contact.emails[0];

  return (
    <div
      className="flex items-center gap-3.5 px-5 py-3 hover:bg-surface/50 transition-colors animate-card-in"
      style={{ animationDelay: `${Math.min(index * 15, 300)}ms` }}
    >
      <div
        className="w-9 h-9 rounded-full flex items-center justify-center shrink-0 text-white text-xs font-medium"
        style={{ backgroundColor: color }}
      >
        {ini}
      </div>
      <div className="min-w-0 flex-1">
        <p className="text-sm font-medium text-text-primary truncate">{name}</p>
        {phone && (
          <p
            className="text-[11px] text-text-secondary truncate mt-0.5"
            style={{ fontFamily: "var(--font-mono)" }}
          >
            {phone.raw}
            {phone.type && phone.type !== "unknown" && (
              <span className="text-text-secondary/60"> &middot; {phone.type}</span>
            )}
          </p>
        )}
        {!phone && email && (
          <p
            className="text-[11px] text-text-secondary truncate mt-0.5"
            style={{ fontFamily: "var(--font-mono)" }}
          >
            {email}
          </p>
        )}
      </div>
      {contact.phones.length > 1 && (
        <span
          className="text-[10px] text-text-secondary/70 bg-surface rounded px-1.5 py-0.5 shrink-0"
          style={{ fontFamily: "var(--font-mono)" }}
        >
          +{contact.phones.length - 1}
        </span>
      )}
    </div>
  );
}
