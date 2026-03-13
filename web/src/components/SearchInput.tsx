import { useRef, useEffect } from "react";

interface SearchInputProps {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  focusTrigger?: number;
}

export function SearchInput({
  value,
  onChange,
  placeholder = "Search",
  focusTrigger,
}: SearchInputProps) {
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (focusTrigger && focusTrigger > 0) {
      inputRef.current?.focus();
    }
  }, [focusTrigger]);

  return (
    <div className="relative px-3 py-2">
      <input
        ref={inputRef}
        type="text"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        className="w-full rounded-lg bg-surface px-3 py-2 text-sm text-text-primary placeholder:text-text-secondary border border-border outline-none focus:border-accent transition-colors"
        style={{ fontFamily: "var(--font-body)" }}
      />
      {value && (
        <button
          onClick={() => onChange("")}
          aria-label="Clear search"
          className="absolute right-5 top-1/2 -translate-y-1/2 text-text-secondary hover:text-text-primary text-sm"
        >
          &times;
        </button>
      )}
    </div>
  );
}
