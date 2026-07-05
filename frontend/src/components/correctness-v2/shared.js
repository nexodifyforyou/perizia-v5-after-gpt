import React from 'react';
import { ChevronDown } from 'lucide-react';

// Shared presentational primitives for the Correctness V2 area. These are pure
// display helpers reused by both the admin panel and the customer report view.
// Keeping them here avoids divergent copies drifting apart.

export const compactText = (value, fallback = '-') => {
  if (value === null || value === undefined || value === '') return fallback;
  if (typeof value === 'boolean') return value ? 'Si' : 'No';
  if (typeof value === 'number') return Number.isFinite(value) ? value.toLocaleString('it-IT') : fallback;
  if (Array.isArray(value)) return value.map((item) => compactText(item, '')).filter(Boolean).join(', ') || fallback;
  if (typeof value === 'object') {
    return (
      value.amount_display ||
      value.formatted ||
      value.label ||
      value.label_it ||
      value.value ||
      value.status_label ||
      value.status ||
      fallback
    );
  }
  return String(value);
};

export const pagesText = (pages) => {
  const normalized = [...new Set((Array.isArray(pages) ? pages : [])
    .map((p) => Number(p))
    .filter((p) => Number.isFinite(p) && p > 0))]
    .sort((a, b) => a - b);
  return normalized.length ? `p. ${normalized.join(', ')}` : '';
};

export const DetailBlock = ({ title, children, defaultOpen = false, testId }) => (
  <details
    open={defaultOpen}
    data-testid={testId}
    className="rounded-lg border border-zinc-800 bg-zinc-950/70 p-3"
  >
    <summary className="flex cursor-pointer list-none items-center justify-between gap-3 text-sm font-semibold text-zinc-100">
      <span>{title}</span>
      <ChevronDown className="h-4 w-4 text-zinc-500" />
    </summary>
    <div className="mt-3">{children}</div>
  </details>
);

export const TextList = ({ items, emptyText = 'Nessuna voce.' }) => {
  const normalized = Array.isArray(items) ? items : [];
  if (!normalized.length) return <p className="text-sm text-zinc-500">{emptyText}</p>;
  return (
    <ul className="space-y-2">
      {normalized.map((item, idx) => {
        const text = compactText(item?.text || item?.detail || item?.action || item?.summary || item, '');
        const pages = pagesText(item?.evidence_pages);
        if (!text) return null;
        return (
          <li key={`${text.slice(0, 30)}-${idx}`} className="rounded-md border border-zinc-800 bg-zinc-950 p-3 text-sm text-zinc-200">
            <p className="break-words">{text}</p>
            {pages && <p className="mt-1 font-mono text-xs text-gold">{pages}</p>}
          </li>
        );
      })}
    </ul>
  );
};
