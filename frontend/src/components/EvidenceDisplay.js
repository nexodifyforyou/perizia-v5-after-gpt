import React from 'react';
import { FileText, Quote } from 'lucide-react';

// Evidence Badge Component - shows page reference inline
export const EvidenceBadge = ({ evidence }) => {
  if (!evidence || !Array.isArray(evidence) || evidence.length === 0) {
    return (
      <span className="inline-flex items-center gap-1 text-xs text-zinc-600 ml-2">
        <FileText className="w-3 h-3" />
        <span>Non trovato nel documento</span>
      </span>
    );
  }

  const pages = [...new Set(evidence.map(e => e.page).filter(Boolean))].sort((a, b) => a - b);
  
  return (
    <span className="inline-flex items-center gap-1 text-xs text-gold ml-2">
      <FileText className="w-3 h-3" />
      <span>Pag. {pages.join(', ')}</span>
    </span>
  );
};

// Evidence Detail Component - shows full evidence with quotes
export const EvidenceDetail = ({ evidence, className = '' }) => {
  if (!evidence || !Array.isArray(evidence) || evidence.length === 0) {
    return null;
  }

  return (
    <div className={`mt-2 space-y-2 ${className}`}>
      {evidence.map((e, idx) => (
        <div key={idx} className="flex items-start gap-2 p-2 bg-zinc-950/50 rounded border border-zinc-800/50">
          <Quote className="w-4 h-4 text-gold flex-shrink-0 mt-0.5" />
          <div className="flex-1 min-w-0">
            {e.page && (
              <span className="inline-flex items-center gap-1 text-xs font-mono text-gold mr-2">
                <FileText className="w-3 h-3" />
                Pagina {e.page}
              </span>
            )}
            {e.anchor && (
              <span className="text-xs text-zinc-500">[{e.anchor}]</span>
            )}
            {e.quote && (
              <p className="text-xs text-zinc-400 mt-1 italic">"{e.quote}"</p>
            )}
          </div>
        </div>
      ))}
    </div>
  );
};

// Evidence Card Component - wraps content with evidence display
export const EvidenceCard = ({ title, value, evidence, children, className = '' }) => {
  const hasEvidence = evidence && Array.isArray(evidence) && evidence.length > 0;
  
  return (
    <div className={`p-4 bg-zinc-950 rounded-lg ${className}`}>
      <div className="flex items-start justify-between">
        <div className="flex-1">
          <p className="text-xs font-mono text-zinc-500 mb-1 flex items-center">
            {title}
            {hasEvidence && <EvidenceBadge evidence={evidence} />}
          </p>
          {value !== undefined && (
            <p className="text-zinc-100">{value}</p>
          )}
          {children}
        </div>
      </div>
      {hasEvidence && <EvidenceDetail evidence={evidence} />}
    </div>
  );
};

// Data Value with Evidence - for simple key-value pairs with page refs
export const DataValueWithEvidence = ({ label, value, evidence, valueClassName = 'text-zinc-100' }) => {
  const displayValue = value === null || value === undefined || value === '' ? 'N/A' : 
                       typeof value === 'object' ? (value.value || value.formatted || value.status || JSON.stringify(value)) : 
                       String(value);
  
  const hasEvidence = evidence && Array.isArray(evidence) && evidence.length > 0;
  const pages = hasEvidence ? [...new Set(evidence.map(e => e.page).filter(Boolean))].sort((a, b) => a - b) : [];

  return (
    <div className="p-4 bg-zinc-950 rounded-lg">
      <div className="flex items-center justify-between mb-1">
        <p className="text-xs font-mono text-zinc-500">{label}</p>
        {hasEvidence && (
          <span className="text-xs font-mono text-gold flex items-center gap-1">
            <FileText className="w-3 h-3" />
            p. {pages.join(', ')}
          </span>
        )}
      </div>
      <p className={`font-medium ${valueClassName}`}>{displayValue}</p>
      {hasEvidence && evidence[0]?.quote && (
        <p className="text-xs text-zinc-500 mt-2 italic border-l-2 border-gold/30 pl-2">
          "{evidence[0].quote.substring(0, 150)}{evidence[0].quote.length > 150 ? '...' : ''}"
        </p>
      )}
    </div>
  );
};

export default EvidenceBadge;
