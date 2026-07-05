import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  AlertTriangle,
  CheckCircle2,
  FileText,
  Loader2,
  Play,
  RotateCw,
  Search,
  ShieldAlert,
} from 'lucide-react';
import { Button } from '../ui/button';
import { Badge } from '../ui/badge';
import { Input } from '../ui/input';
import { compactText, pagesText, DetailBlock, TextList } from './shared';
import {
  getCorrectnessV2CustomerReport,
  getCorrectnessV2Job,
  getLatestCorrectnessV2Job,
  startCorrectnessV2,
} from '../../lib/api/perizia';

const POLL_INTERVAL_MS = 2500;
const MAX_POLL_ATTEMPTS = 240;
const LOT_RENDER_LIMIT = 40;
const EVIDENCE_RENDER_LIMIT = 100;

const REPORT_TERMINAL_STATUSES = new Set([
  'REPORT_READY',
  'LOT_SELECTION_REQUIRED',
  'NEEDS_MANUAL_REVIEW',
  'CONTRACT_VALIDATION_FAILED',
  'CONTRACT_READY',
  'PDF_QUALITY_BLOCKED',
  'FAILED_ANALYSIS',
  'FAILED_CONTRACT_BUILD',
  'FAILED_GROUNDING',
  'FAILED_NARRATION_NO_REPORT',
  'FAILED_NARRATION_USED_DETERMINISTIC_TEXT',
  'JOB_STALLED',
  'CANCELLED',
  'FAILED',
]);

const FAILURE_STATUSES = new Set([
  'PDF_QUALITY_BLOCKED',
  'FAILED_ANALYSIS',
  'FAILED_CONTRACT_BUILD',
  'FAILED_GROUNDING',
  'FAILED_NARRATION_NO_REPORT',
  'JOB_STALLED',
  'CANCELLED',
  'FAILED',
]);

const MONEY_SECTION_LABELS = {
  valuation_chain: 'Catena di valore',
  auction_terms: 'Condizioni di vendita',
  buyer_side_costs: "Costi a carico dell'acquirente",
  procedure_cancelled_formalities: 'Formalità cancellate dalla procedura',
  market_comparatives: 'Comparativi di mercato',
  context_values: 'Dati economici di contesto',
  uncertain_money: 'Importi da verificare',
};

// Italian labels for raw artifact keys shown in customer-facing grids.
const FIELD_LABELS = {
  tribunale: 'Tribunale',
  procedura_rge: 'Procedura / RGE',
  lotto: 'Lotto',
  address: 'Indirizzo',
  property_type: 'Tipologia',
  ownership_right: 'Diritto',
  multi_lot: 'Più lotti',
  lot_count: 'Numero lotti',
  lot_ids: 'Lotti',
  selected_lot: 'Lotto selezionato',
  bene_count: 'Numero beni',
  multi_bene: 'Più beni nel lotto',
  bene_ids: 'Beni',
};

const HIDDEN_GRID_KEYS = new Set(['evidence_pages', 'detected_bene_count', 'bene_count_source']);

const QUALITY_ESITO_TONES = {
  Coperto: 'border-emerald-500/40 bg-emerald-500/10 text-emerald-300',
  Parziale: 'border-amber-500/40 bg-amber-500/10 text-amber-300',
  'Da verificare': 'border-amber-500/40 bg-amber-500/10 text-amber-200',
  Mancante: 'border-red-500/40 bg-red-500/10 text-red-300',
  'Non materiale': 'border-zinc-700 bg-zinc-800/60 text-zinc-400',
};

const QUALITY_ROW_RENDER_LIMIT = 200;

const isCanceledError = (error) => (
  error?.code === 'ERR_CANCELED' ||
  error?.name === 'CanceledError' ||
  error?.message === 'canceled'
);

const isFailureStatus = (status) => (
  FAILURE_STATUSES.has(status) || String(status || '').startsWith('FAILED')
);

const isTerminalStatus = (status) => (
  REPORT_TERMINAL_STATUSES.has(status) || isFailureStatus(status)
);

const isRunningStatus = (status) => Boolean(status) && !isTerminalStatus(status);

const artifactKeys = (job) => Object.keys(job?.artifacts_saved || {}).sort();

const reportCanBeFetched = (job) => {
  if (!job?.job_id) return false;
  const status = job.status;
  return Boolean(
    job.customer_report_generated ||
    job.artifacts_saved?.customer_report ||
    status === 'REPORT_READY' ||
    status === 'LOT_SELECTION_REQUIRED' ||
    status === 'NEEDS_MANUAL_REVIEW' ||
    status === 'CONTRACT_VALIDATION_FAILED' ||
    isFailureStatus(status)
  );
};

const StatusBadge = ({ status }) => {
  const tone = status === 'REPORT_READY'
    ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-300'
    : status === 'LOT_SELECTION_REQUIRED'
      ? 'border-amber-500/40 bg-amber-500/10 text-amber-300'
      : isFailureStatus(status) || status === 'CONTRACT_VALIDATION_FAILED' || status === 'NEEDS_MANUAL_REVIEW'
        ? 'border-red-500/40 bg-red-500/10 text-red-300'
        : 'border-sky-500/40 bg-sky-500/10 text-sky-300';
  return (
    <Badge variant="outline" className={`font-mono uppercase ${tone}`}>
      {status || 'IDLE'}
    </Badge>
  );
};

const LoadingLine = ({ children }) => (
  <div className="flex items-center gap-2 text-sm text-zinc-400">
    <Loader2 className="h-4 w-4 animate-spin" />
    {children}
  </div>
);

const KeyValueGrid = ({ data }) => {
  const entries = Object.entries(data || {}).filter(([key, value]) => {
    if (HIDDEN_GRID_KEYS.has(key)) return false;
    if (value === null || value === undefined || value === '') return false;
    if (Array.isArray(value)) return value.length > 0;
    return true;
  });
  if (!entries.length) return <p className="text-sm text-zinc-500">Nessun dato disponibile.</p>;
  return (
    <dl className="grid grid-cols-1 gap-3 md:grid-cols-2">
      {entries.map(([key, value]) => (
        <div key={key} className="min-w-0 rounded-md border border-zinc-800 bg-zinc-950 p-3">
          <dt className="text-[11px] uppercase text-zinc-500">{FIELD_LABELS[key] || key.replace(/_/g, ' ')}</dt>
          <dd className="mt-1 break-words text-sm text-zinc-200">{compactText(value)}</dd>
        </div>
      ))}
    </dl>
  );
};

const CorrectnessV2Status = ({ job, polling, pollAttempts, reportLoading, error }) => {
  if (!job && !error) return null;
  return (
    <div data-testid="cv2-status" className="space-y-3 rounded-lg border border-zinc-800 bg-zinc-950 p-4">
      <div className="flex flex-wrap items-center gap-3">
        <StatusBadge status={job?.status} />
        {polling && <span className="text-xs text-zinc-500">Polling ogni 2.5s, tentativo {pollAttempts}/{MAX_POLL_ATTEMPTS}</span>}
        {reportLoading && <LoadingLine>Caricamento customer_report.json...</LoadingLine>}
      </div>
      {job?.current_stage && (
        <p className="text-sm text-zinc-400">Stage: <span className="font-mono text-zinc-300">{job.current_stage}</span></p>
      )}
      {job?.message && <p className="text-sm text-zinc-300">{job.message}</p>}
      {(job?.reason_code || job?.reason_human || job?.troubleshoot_message) && (
        <div className="rounded-md border border-amber-500/20 bg-amber-500/5 p-3 text-sm text-amber-100">
          {job.reason_code && <p className="font-mono text-xs uppercase text-amber-300">{job.reason_code}</p>}
          {job.reason_human && <p className="mt-1">{job.reason_human}</p>}
          {job.troubleshoot_message && <p className="mt-2 text-amber-200/80">{job.troubleshoot_message}</p>}
          {Array.isArray(job.next_steps) && job.next_steps.length > 0 && (
            <ul className="mt-2 list-disc space-y-1 pl-5 text-amber-200/80">
              {job.next_steps.map((step, idx) => <li key={`${step}-${idx}`}>{step}</li>)}
            </ul>
          )}
        </div>
      )}
      {error && <p className="text-sm text-red-300">{error}</p>}
    </div>
  );
};

const CorrectnessV2RunButton = ({ onRun, disabled, hasJob, starting }) => (
  <Button
    type="button"
    data-testid="run-correctness-v2-button"
    onClick={onRun}
    disabled={disabled}
    className="bg-gold text-zinc-950 hover:bg-gold-dim disabled:opacity-60"
  >
    {starting ? <Loader2 className="h-4 w-4 animate-spin" /> : hasJob ? <RotateCw className="h-4 w-4" /> : <Play className="h-4 w-4" />}
    {starting ? 'Avvio...' : hasJob ? 'Run again' : 'Run Correctness V2'}
  </Button>
);

const lotSearchText = (lot) => [
  lot?.lot_id,
  lot?.label,
  lot?.address,
  lot?.property_type,
  lot?.ownership_right,
  lot?.occupancy_summary,
  ...(lot?.money_summary || lot?.key_money || []).map((row) => compactText(row, '')),
].join(' ').toLowerCase();

const moneyRowsForLot = (lot) => {
  if (Array.isArray(lot?.money_summary) && lot.money_summary.length) return lot.money_summary;
  if (Array.isArray(lot?.key_money) && lot.key_money.length) return lot.key_money;
  return [];
};

const CorrectnessV2LotSelector = ({ job, report, onSelectLot, disabled }) => {
  const [query, setQuery] = useState('');
  const lots = useMemo(() => {
    const reportLots = report?.lot_selection?.lots;
    if (Array.isArray(reportLots) && reportLots.length) return reportLots;
    return Array.isArray(job?.available_lots) ? job.available_lots : [];
  }, [job, report]);
  const filteredLots = useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (!needle) return lots;
    return lots.filter((lot) => lotSearchText(lot).includes(needle));
  }, [lots, query]);
  const visibleLots = filteredLots.slice(0, LOT_RENDER_LIMIT);

  if (!lots.length) {
    return (
      <div data-testid="cv2-lot-selector" className="rounded-lg border border-amber-500/30 bg-amber-500/5 p-4 text-sm text-amber-200">
        Lot selection required, but no lot list was returned.
      </div>
    );
  }

  return (
    <section data-testid="cv2-lot-selector" className="space-y-4 rounded-lg border border-zinc-800 bg-zinc-900 p-4">
      <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
        <div>
          <h3 className="text-lg font-semibold text-zinc-100">Selezione lotto</h3>
          <p className="text-sm text-zinc-500">{report?.lot_selection?.message || job?.reason_human || 'Selezionare un lotto da analizzare.'}</p>
        </div>
        <div className="relative md:w-80">
          <Search className="pointer-events-none absolute left-3 top-2.5 h-4 w-4 text-zinc-500" />
          <Input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Filtra lotti"
            className="border-zinc-700 bg-zinc-950 pl-9 text-zinc-100"
          />
        </div>
      </div>

      {filteredLots.length > LOT_RENDER_LIMIT && (
        <p className="text-xs text-zinc-500">
          Mostrati i primi {LOT_RENDER_LIMIT} di {filteredLots.length} lotti filtrati.
        </p>
      )}

      <div className="grid grid-cols-1 gap-3 xl:grid-cols-2">
        {visibleLots.map((lot, idx) => {
          const lotId = compactText(lot?.lot_id, String(idx + 1));
          const moneyRows = moneyRowsForLot(lot);
          return (
            <article key={`${lotId}-${idx}`} className="rounded-lg border border-zinc-800 bg-zinc-950 p-4">
              <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                <div className="min-w-0">
                  <h4 className="break-words text-base font-semibold text-zinc-100">{compactText(lot?.label, `Lotto ${lotId}`)}</h4>
                  <div className="mt-2 space-y-1 text-sm text-zinc-400">
                    {lot?.address && <p>Indirizzo: {compactText(lot.address)}</p>}
                    {lot?.property_type && <p>Tipo: {compactText(lot.property_type)}</p>}
                    {lot?.confidence && <p>Confidence: {compactText(lot.confidence)}</p>}
                    {Array.isArray(lot?.notes) && lot.notes.length > 0 && <p>Note: {lot.notes.slice(0, 2).join('; ')}</p>}
                  </div>
                </div>
                <Button
                  type="button"
                  data-testid={`cv2-lot-analyze-${lotId}`}
                  onClick={() => onSelectLot(lotId)}
                  disabled={disabled}
                  className="shrink-0 bg-gold text-zinc-950 hover:bg-gold-dim"
                >
                  Analyze this lot
                </Button>
              </div>

              {moneyRows.length > 0 && (
                <div className="mt-3 rounded-md border border-zinc-800 bg-zinc-900/60 p-3">
                  <p className="text-[11px] font-mono uppercase text-zinc-500">Key money</p>
                  <ul className="mt-2 space-y-1 text-sm text-zinc-300">
                    {moneyRows.slice(0, 3).map((row, rowIdx) => (
                      <li key={`${row?.label || 'money'}-${rowIdx}`} className="flex justify-between gap-3">
                        <span className="break-words">{compactText(row?.label || row?.kind, 'Importo')}</span>
                        <span className="shrink-0 font-mono text-gold">{compactText(row?.amount_display || row?.amount, '')}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              <details className="mt-3 text-sm">
                <summary className="cursor-pointer text-zinc-500">Dettagli lotto</summary>
                <div className="mt-2 space-y-2 text-zinc-400">
                  {lot?.ownership_right && <p>Diritto: {compactText(lot.ownership_right)}</p>}
                  {lot?.occupancy_summary && <p>Occupazione: {compactText(lot.occupancy_summary)}</p>}
                  {pagesText(lot?.evidence_pages || lot?.page_evidence) && (
                    <p className="font-mono text-xs text-gold">{pagesText(lot?.evidence_pages || lot?.page_evidence)}</p>
                  )}
                  {moneyRows.length > 3 && (
                    <ul className="space-y-1">
                      {moneyRows.slice(3).map((row, rowIdx) => (
                        <li key={`${row?.label || 'extra-money'}-${rowIdx}`}>
                          {compactText(row?.label || row?.kind, 'Importo')}: {compactText(row?.amount_display || row?.amount, '')}
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              </details>
            </article>
          );
        })}
      </div>
    </section>
  );
};

const MoneySections = ({ sections }) => {
  const moneySections = sections || {};
  return (
    <section className="space-y-3">
      <h3 className="text-lg font-semibold text-zinc-100">Money sections</h3>
      <div className="grid grid-cols-1 gap-3 xl:grid-cols-2">
        {Object.entries(MONEY_SECTION_LABELS).map(([key, label]) => {
          const rows = Array.isArray(moneySections[key]) ? moneySections[key] : [];
          return (
            <div
              key={key}
              data-testid={`cv2-money-section-${key}`}
              className="min-h-[120px] rounded-lg border border-zinc-800 bg-zinc-950 p-4"
            >
              <div className="mb-3 flex items-center justify-between gap-3">
                <h4 className="text-sm font-semibold text-zinc-100">{label}</h4>
                <span className="font-mono text-xs text-zinc-500">{rows.length}</span>
              </div>
              {rows.length ? (
                <div className="overflow-x-auto">
                  <table className="w-full min-w-[420px] text-left text-sm">
                    <thead>
                      <tr className="border-b border-zinc-800 text-xs text-zinc-500">
                        <th className="pb-2 pr-3 font-medium">Voce</th>
                        <th className="pb-2 pr-3 font-medium">Importo</th>
                        <th className="pb-2 font-medium">Pagine</th>
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((row, idx) => (
                        <tr key={`${row?.label || key}-${idx}`} className="border-b border-zinc-900 last:border-0">
                          <td className="py-2 pr-3 align-top text-zinc-200">
                            <span className="break-words">{compactText(row?.label || row?.kind, 'Importo')}</span>
                            {row?.included_in_valuation && (
                              <span className="ml-2 inline-block rounded border border-sky-500/40 bg-sky-500/10 px-1.5 py-0.5 text-[10px] uppercase text-sky-300">
                                Già nella catena di valore
                              </span>
                            )}
                            {row?.status_label && !row?.included_in_valuation && (
                              <p className="mt-1 text-xs text-zinc-500">{compactText(row.status_label)}</p>
                            )}
                            {row?.notes && <p className="mt-1 text-xs text-zinc-500">{compactText(row.notes)}</p>}
                          </td>
                          <td className="py-2 pr-3 align-top font-mono text-gold">{compactText(row?.amount_display || row?.amount, '-')}</td>
                          <td className="py-2 align-top font-mono text-xs text-zinc-500">{pagesText(row?.evidence_pages)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="text-sm text-zinc-500">Nessuna voce.</p>
              )}
            </div>
          );
        })}
      </div>
    </section>
  );
};

const RiskSections = ({ sections }) => {
  const riskSections = Array.isArray(sections) ? sections : [];
  if (!riskSections.length) return <TextList items={[]} emptyText="Nessun rischio confermato nel customer_report." />;
  return (
    <section className="space-y-3">
      <h3 className="text-lg font-semibold text-zinc-100">Risk sections</h3>
      {riskSections.map((section, idx) => {
        const items = Array.isArray(section?.items) ? section.items : [];
        return (
          <DetailBlock key={`${section?.section_id || section?.title || 'risk'}-${idx}`} title={`${compactText(section?.title, 'Rischi')} (${items.length})`}>
            {items.length ? (
              <ul className="space-y-2">
                {items.map((item, itemIdx) => (
                  <li key={`${item?.area || 'risk'}-${itemIdx}`} className="rounded-md border border-zinc-800 bg-zinc-900/70 p-3 text-sm text-zinc-200">
                    <div className="flex flex-wrap items-center gap-2">
                      {item?.severity_label && <Badge variant="outline" className="border-zinc-700 text-zinc-300">{item.severity_label}</Badge>}
                      {item?.status_label && <span className="text-zinc-400">{item.status_label}</span>}
                    </div>
                    <p className="mt-2 font-medium">{compactText(item?.area || item?.title, 'Segnalazione')}</p>
                    {item?.summary && <p className="mt-1 text-zinc-400">{compactText(item.summary)}</p>}
                    {item?.cost_display && <p className="mt-1 font-mono text-gold">{item.cost_display}</p>}
                    {pagesText(item?.evidence_pages) && <p className="mt-1 font-mono text-xs text-gold">{pagesText(item.evidence_pages)}</p>}
                  </li>
                ))}
              </ul>
            ) : (
              <p className="text-sm text-zinc-500">Nessuna voce.</p>
            )}
          </DetailBlock>
        );
      })}
    </section>
  );
};

const BeniSections = ({ sections }) => {
  const beni = Array.isArray(sections) ? sections : [];
  if (!beni.length) return <TextList items={[]} emptyText="Nessuna sezione beni separata." />;
  return (
    <section className="space-y-3">
      <h3 className="text-lg font-semibold text-zinc-100">Beni</h3>
      <div className="grid grid-cols-1 gap-3 xl:grid-cols-2">
        {beni.map((bene, idx) => (
          <div key={`${bene?.bene_id || idx}`} className="rounded-lg border border-zinc-800 bg-zinc-950 p-4">
            <h4 className="font-semibold text-zinc-100">{compactText(bene?.title, `Bene ${idx + 1}`)}</h4>
            {bene?.address && <p className="mt-1 text-sm text-zinc-400">{compactText(bene.address)}</p>}
            {bene?.note && <p className="mt-2 text-sm text-zinc-500">{compactText(bene.note)}</p>}
            {Array.isArray(bene?.accessories) && bene.accessories.length > 0 && (
              <div className="mt-3" data-testid={`cv2-bene-accessories-${bene?.bene_id || idx}`}>
                <p className="text-[11px] font-mono uppercase text-zinc-500">Accessori / pertinenze</p>
                <ul className="mt-2 space-y-1 text-sm text-zinc-300">
                  {bene.accessories.map((acc, accIdx) => (
                    <li key={`${acc?.label || 'acc'}-${accIdx}`} className="flex flex-wrap items-baseline gap-2">
                      <span className="capitalize">{compactText(acc?.label, 'Accessorio')}</span>
                      {pagesText(acc?.evidence_pages) && (
                        <span className="font-mono text-xs text-gold">{pagesText(acc.evidence_pages)}</span>
                      )}
                    </li>
                  ))}
                </ul>
              </div>
            )}
            {Array.isArray(bene?.risks) && bene.risks.length > 0 && (
              <div className="mt-3">
                <p className="text-[11px] font-mono uppercase text-zinc-500">Rischi</p>
                <TextList items={bene.risks.map((risk) => ({ text: `${compactText(risk.area, 'Voce')}: ${compactText(risk.summary, '')}`, evidence_pages: risk.evidence_pages }))} />
              </div>
            )}
            {Array.isArray(bene?.checklist) && bene.checklist.length > 0 && (
              <div className="mt-3">
                <p className="text-[11px] font-mono uppercase text-zinc-500">Checklist</p>
                <TextList items={bene.checklist} />
              </div>
            )}
          </div>
        ))}
      </div>
    </section>
  );
};

// Customer-facing evidence: page + human topic + VERBATIM perizia excerpt.
// Raw internal keys (technical_compliance[4], ...) live ONLY in the collapsed
// admin debug block below, never in this list.
const LEGACY_TOPIC_LABELS = {
  case_identity: 'Dati principali',
  occupancy: 'Stato di occupazione',
  money: 'Valori e costi',
  technical_compliance: 'Conformità e documenti tecnici',
  legal_formalities: 'Formalità e cancellazioni',
  risk_classification: 'Rischi e segnalazioni',
};

// "technical_compliance[2]:conformità urbanistica" -> "conformità urbanistica";
// bare machine keys map to their section's Italian label. Never shows brackets.
const humanizeLegacyRef = (ref) => {
  const s = String(ref || '');
  const colon = s.indexOf(':');
  if (colon >= 0 && colon < s.length - 1) return s.slice(colon + 1).trim();
  const prefix = s.split('[')[0].trim();
  return LEGACY_TOPIC_LABELS[prefix] || prefix.replace(/_/g, ' ');
};

// Older reports carry only the raw evidence_index: build a humanized customer
// view from it so evidence never disappears (raw keys stay admin-only).
const legacyCustomerFallback = (legacyEvidence) => (
  (Array.isArray(legacyEvidence) ? legacyEvidence : []).map((entry) => {
    const topics = [...new Set((entry?.referenced_by || []).map(humanizeLegacyRef).filter(Boolean))];
    return {
      page: entry?.page,
      topic: topics.join(', ') || 'Riferimento in perizia',
      perizia_excerpt: null,
      note: `Estratto non disponibile per questo report; verificare pagina ${compactText(entry?.page, '?')}.`,
      coverage_status: 'excerpt_missing',
    };
  })
);

const EvidenceIndex = ({ customerEvidence, adminEvidence, legacyEvidence }) => {
  const customerItems = Array.isArray(customerEvidence) ? customerEvidence : [];
  const items = customerItems.length ? customerItems : legacyCustomerFallback(legacyEvidence);
  const visible = items.slice(0, EVIDENCE_RENDER_LIMIT);
  const adminItems = Array.isArray(adminEvidence) && adminEvidence.length
    ? adminEvidence
    : (Array.isArray(legacyEvidence) ? legacyEvidence : []);
  return (
    <section className="space-y-3">
      <DetailBlock title={`Indice delle evidenze (${items.length})`} testId="cv2-evidence-index" defaultOpen>
        {visible.length ? (
          <ul className="space-y-2">
            {visible.map((entry, idx) => (
              <li key={`${entry?.page || '?'}-${entry?.topic || idx}`} className="rounded-md border border-zinc-800 bg-zinc-900/70 p-3 text-sm">
                <p className="text-zinc-100">
                  <span className="font-mono text-gold">p. {compactText(entry?.page, '?')}</span>
                  <span className="mx-2 text-zinc-600">—</span>
                  <span className="font-medium">{compactText(entry?.topic, 'Tema')}</span>
                </p>
                {entry?.perizia_excerpt ? (
                  <p className="mt-2 border-l-2 border-zinc-700 pl-3 italic text-zinc-300">
                    Estratto perizia: “{compactText(entry.perizia_excerpt)}{entry?.excerpt_truncated ? '…' : ''}”
                  </p>
                ) : (
                  <p className="mt-2 text-amber-200/80">
                    {compactText(entry?.note, 'Estratto non disponibile automaticamente; verificare la pagina indicata.')}
                  </p>
                )}
                {entry?.report_section && (
                  <p className="mt-1 text-xs text-zinc-500">Sezione: {compactText(entry.report_section)}</p>
                )}
              </li>
            ))}
          </ul>
        ) : (
          <p className="text-sm text-zinc-500">Nessuna evidenza indicizzata.</p>
        )}
        {items.length > EVIDENCE_RENDER_LIMIT && (
          <p className="mt-3 text-xs text-zinc-500">
            Altre {items.length - EVIDENCE_RENDER_LIMIT} righe non mostrate nella preview.
          </p>
        )}
      </DetailBlock>
      {adminItems.length > 0 && (
        <DetailBlock title={`Debug evidenze (admin) (${adminItems.length})`} testId="cv2-evidence-admin-debug">
          <div className="grid grid-cols-1 gap-2 md:grid-cols-2">
            {adminItems.slice(0, EVIDENCE_RENDER_LIMIT).map((entry, idx) => (
              <div key={`admin-${entry?.page || idx}`} className="rounded-md border border-zinc-800 bg-zinc-900/70 p-3 text-sm">
                <p className="font-mono text-gold">p. {compactText(entry?.page, '?')}</p>
                <p className="mt-1 break-words font-mono text-xs text-zinc-500">
                  {compactText(entry?.raw_keys || entry?.referenced_by, '')}
                </p>
                {entry?.artifact_source && (
                  <p className="mt-1 font-mono text-[10px] text-zinc-600">{compactText(entry.artifact_source)}</p>
                )}
              </div>
            ))}
          </div>
        </DetailBlock>
      )}
    </section>
  );
};

const OccupancySection = ({ section }) => {
  if (!section || (!section.status && !section.title_info)) return null;
  return (
    <section data-testid="cv2-occupancy-section" className="space-y-3">
      <h3 className="text-lg font-semibold text-zinc-100">Stato di occupazione</h3>
      <div className="rounded-lg border border-zinc-800 bg-zinc-950 p-4 text-sm text-zinc-200">
        {section.status_label && (
          <p><span className="text-zinc-500">Stato: </span>{compactText(section.status_label)}</p>
        )}
        {section.title_info && <p className="mt-2">{compactText(section.title_info)}</p>}
        {section.opponibility && <p className="mt-2 text-zinc-300">{compactText(section.opponibility)}</p>}
        {Array.isArray(section.registration_dates) && section.registration_dates.length > 0 && (
          <p className="mt-2 text-zinc-400">Registrazione contratto: {section.registration_dates.join(', ')}</p>
        )}
        {Array.isArray(section.expiry_dates) && section.expiry_dates.length > 0 && (
          <p className="mt-1 text-zinc-400">Scadenza contratto: {section.expiry_dates.join(', ')}</p>
        )}
        {Array.isArray(section.risks) && section.risks.length > 0 && (
          <ul className="mt-3 list-disc space-y-1 pl-5 text-amber-200/90">
            {section.risks.map((risk, idx) => <li key={`occ-risk-${idx}`}>{compactText(risk)}</li>)}
          </ul>
        )}
        {pagesText(section.evidence_pages) && (
          <p className="mt-2 font-mono text-xs text-gold">{pagesText(section.evidence_pages)}</p>
        )}
      </div>
    </section>
  );
};

const ComplianceSection = ({ items }) => {
  const list = Array.isArray(items) ? items : [];
  if (!list.length) return null;
  return (
    <section data-testid="cv2-compliance-section" className="space-y-3">
      <h3 className="text-lg font-semibold text-zinc-100">Conformità e documenti tecnici</h3>
      <div className="grid grid-cols-1 gap-3 xl:grid-cols-2">
        {list.map((item, idx) => (
          <div key={`${item?.area || 'area'}-${idx}`} className="rounded-lg border border-zinc-800 bg-zinc-950 p-4 text-sm">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <p className="font-medium text-zinc-100">{compactText(item?.area, 'Area')}</p>
              <Badge variant="outline" className="border-zinc-700 text-zinc-300">{compactText(item?.status_label, 'Da verificare')}</Badge>
            </div>
            {item?.notes && <p className="mt-2 text-zinc-400">{compactText(item.notes)}</p>}
            <div className="mt-2 flex flex-wrap gap-3 text-xs text-zinc-400">
              {item?.cost_display && <span className="font-mono text-gold">{item.cost_display}</span>}
              {item?.timing && <span>Tempi: {compactText(item.timing)}</span>}
              {pagesText(item?.evidence_pages) && <span className="font-mono text-gold">{pagesText(item.evidence_pages)}</span>}
            </div>
          </div>
        ))}
      </div>
    </section>
  );
};

const FormalitiesSection = ({ items }) => {
  const list = Array.isArray(items) ? items : [];
  if (!list.length) return null;
  return (
    <section data-testid="cv2-formalities-section" className="space-y-3">
      <h3 className="text-lg font-semibold text-zinc-100">Formalità e cancellazioni</h3>
      <ul className="space-y-2">
        {list.map((item, idx) => (
          <li key={`form-${idx}`} className="rounded-lg border border-zinc-800 bg-zinc-950 p-4 text-sm">
            <div className="flex flex-wrap items-center gap-2">
              <Badge variant="outline" className="border-zinc-700 uppercase text-zinc-300">{compactText(item?.type_label || item?.type, 'Formalità')}</Badge>
              <span className="text-zinc-300">{compactText(item?.status_label)}</span>
            </div>
            {item?.description && <p className="mt-2 text-zinc-400">{compactText(item.description)}</p>}
            {item?.amount_display && (
              <p className="mt-2 font-mono text-gold">{item.amount_display}</p>
            )}
            {item?.amount_note && <p className="mt-1 text-xs text-zinc-500">{compactText(item.amount_note)}</p>}
            {pagesText(item?.evidence_pages) && (
              <p className="mt-1 font-mono text-xs text-gold">{pagesText(item.evidence_pages)}</p>
            )}
          </li>
        ))}
      </ul>
    </section>
  );
};

const SurfacesSection = ({ items }) => {
  const list = Array.isArray(items) ? items : [];
  if (!list.length) return null;
  return (
    <section data-testid="cv2-surfaces-section" className="space-y-3">
      <h3 className="text-lg font-semibold text-zinc-100">Superfici e dati catastali</h3>
      <div className="grid grid-cols-1 gap-2 md:grid-cols-2 xl:grid-cols-3">
        {list.map((item, idx) => (
          <div key={`surf-${idx}`} className="rounded-md border border-zinc-800 bg-zinc-950 p-3 text-sm">
            <p className="text-[11px] uppercase text-zinc-500">{compactText(item?.label, 'Dato')}</p>
            <p className="mt-1 break-words text-zinc-200">{compactText(item?.value)}</p>
            {item?.note && <p className="mt-1 text-xs text-amber-200/80">{compactText(item.note)}</p>}
            {pagesText(item?.evidence_pages) && (
              <p className="mt-1 font-mono text-xs text-gold">{pagesText(item.evidence_pages)}</p>
            )}
          </div>
        ))}
      </div>
    </section>
  );
};

const QualityControlSection = ({ qualityControl }) => {
  if (!qualityControl) return null;
  const rows = Array.isArray(qualityControl.rows) ? qualityControl.rows : [];
  const visible = rows.slice(0, QUALITY_ROW_RENDER_LIMIT);
  const failed = qualityControl.coverage_status === 'FAIL';
  return (
    <section data-testid="cv2-quality-control" className="space-y-3">
      <div className="flex flex-wrap items-center gap-3">
        <h3 className="text-lg font-semibold text-zinc-100">Controllo qualità pagina per pagina</h3>
        <Badge
          variant="outline"
          className={failed
            ? 'border-red-500/40 bg-red-500/10 text-red-300'
            : qualityControl.coverage_status === 'WARNING'
              ? 'border-amber-500/40 bg-amber-500/10 text-amber-300'
              : 'border-emerald-500/40 bg-emerald-500/10 text-emerald-300'}
        >
          Copertura: {compactText(qualityControl.coverage_status, '-')}
        </Badge>
        {Number.isFinite(qualityControl.satisfaction_score) && (
          <span className="text-xs text-zinc-500">
            Punteggio qualità: <span className="font-mono text-zinc-300">{qualityControl.satisfaction_score}/100</span>
          </span>
        )}
      </div>
      {failed && (
        <div data-testid="cv2-quality-blocked" className="rounded-lg border border-red-500/40 bg-red-500/10 p-4 text-sm text-red-100">
          Il controllo qualità ha rilevato omissioni critiche: il report non è certificato come completo.
        </div>
      )}
      <DetailBlock title={`Tabella di controllo (${rows.length} righe)`} testId="cv2-quality-table" defaultOpen={failed}>
        {visible.length ? (
          <div className="overflow-x-auto">
            <table className="w-full min-w-[640px] text-left text-sm">
              <thead>
                <tr className="border-b border-zinc-800 text-xs text-zinc-500">
                  <th className="pb-2 pr-3 font-medium">Pagina</th>
                  <th className="pb-2 pr-3 font-medium">Dato rilevante nella perizia</th>
                  <th className="pb-2 pr-3 font-medium">Presente nel report</th>
                  <th className="pb-2 pr-3 font-medium">Esito</th>
                  <th className="pb-2 font-medium">Note</th>
                </tr>
              </thead>
              <tbody>
                {visible.map((row, idx) => (
                  <tr key={`qc-${idx}`} className="border-b border-zinc-900 align-top last:border-0">
                    <td className="py-2 pr-3 font-mono text-gold">{compactText(row?.pagina, '-')}</td>
                    <td className="py-2 pr-3 text-zinc-200">
                      <span>{compactText(row?.dato, '-')}</span>
                      {row?.ruolo && <p className="mt-0.5 text-[11px] text-zinc-500">Ruolo: {compactText(row.ruolo)}</p>}
                    </td>
                    <td className="py-2 pr-3 text-zinc-300">{row?.presente ? 'Sì' : 'No'}</td>
                    <td className="py-2 pr-3">
                      <Badge variant="outline" className={QUALITY_ESITO_TONES[row?.esito] || 'border-zinc-700 text-zinc-300'}>
                        {compactText(row?.esito, '-')}
                      </Badge>
                    </td>
                    <td className="py-2 text-xs text-zinc-500">{compactText(row?.note, '')}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="text-sm text-zinc-500">Nessuna riga di controllo.</p>
        )}
        {rows.length > QUALITY_ROW_RENDER_LIMIT && (
          <p className="mt-3 text-xs text-zinc-500">
            Altre {rows.length - QUALITY_ROW_RENDER_LIMIT} righe disponibili in page_by_page_audit.json.
          </p>
        )}
      </DetailBlock>
    </section>
  );
};

const ManualReviewBox = ({ job, report }) => {
  const flags = Array.isArray(report?.manual_review_flags) ? report.manual_review_flags : [];
  const summary = Array.isArray(report?.executive_summary) ? report.executive_summary : [];
  return (
    <section data-testid="cv2-manual-review" className="space-y-3 rounded-lg border border-red-500/30 bg-red-500/5 p-4">
      <div className="flex items-center gap-2">
        <ShieldAlert className="h-5 w-5 text-red-300" />
        <h3 className="text-lg font-semibold text-red-100">{compactText(report?.title, 'Revisione manuale necessaria')}</h3>
      </div>
      <p className="text-sm text-red-100/80">{compactText(report?.subtitle || job?.reason_human, 'Nessun report verificato e stato generato.')}</p>
      <TextList items={summary} emptyText="Nessuna sintesi disponibile." />
      {flags.length > 0 && (
        <DetailBlock title={`Manual review flags (${flags.length})`} defaultOpen>
          <TextList items={flags.map((flag) => ({ text: `${compactText(flag.kind || flag.code, 'flag')}: ${compactText(flag.detail, '')}`, evidence_pages: flag.evidence_pages }))} />
        </DetailBlock>
      )}
    </section>
  );
};

const ContractReadyBox = ({ job }) => (
  <section className="rounded-lg border border-amber-500/30 bg-amber-500/5 p-4">
    <div className="flex items-center gap-2">
      <FileText className="h-5 w-5 text-amber-300" />
      <h3 className="text-lg font-semibold text-amber-100">Contract ready</h3>
    </div>
    <p className="mt-2 text-sm text-amber-100/80">
      Il contratto verificato e pronto, ma customer_report.json non e ancora disponibile.
    </p>
    {artifactKeys(job).length > 0 && (
      <div className="mt-3">
        <p className="text-[11px] font-mono uppercase text-amber-200/70">Artifacts</p>
        <div className="mt-2 flex flex-wrap gap-2">
          {artifactKeys(job).map((key) => (
            <Badge key={key} variant="outline" className="border-amber-500/30 text-amber-200">{key}</Badge>
          ))}
        </div>
      </div>
    )}
  </section>
);

const CorrectnessV2CustomerReport = ({ report }) => {
  if (!report) return null;
  return (
    <article data-testid="cv2-report" className="space-y-6 rounded-lg border border-zinc-800 bg-zinc-900 p-4">
      <header className="space-y-3 border-b border-zinc-800 pb-4">
        <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
          <div>
            <p className="text-[11px] font-mono uppercase text-gold">Correctness Mode V2 preview/admin</p>
            <h2 className="mt-1 text-2xl font-serif font-bold text-zinc-100">{compactText(report.title, 'Customer report')}</h2>
            {report.subtitle && <p className="mt-1 text-sm text-zinc-400">{compactText(report.subtitle)}</p>}
          </div>
          <StatusBadge status={report.report_status} />
        </div>
      </header>

      <section className="space-y-3">
        <h3 className="text-lg font-semibold text-zinc-100">Dati principali</h3>
        <KeyValueGrid data={report.case_identity} />
      </section>

      <section className="space-y-3">
        <h3 className="text-lg font-semibold text-zinc-100">Struttura lotto / beni</h3>
        <KeyValueGrid data={report.lot_structure} />
      </section>

      <BeniSections sections={report.beni_sections} />

      <section className="space-y-3">
        <h3 className="text-lg font-semibold text-zinc-100">Sintesi esecutiva</h3>
        <TextList items={report.executive_summary} />
      </section>

      <section className="space-y-3">
        <h3 className="text-lg font-semibold text-zinc-100">Dati chiave</h3>
        <TextList items={(report.key_facts || []).map((fact) => ({ text: `${compactText(fact.label, 'Dato')}: ${compactText(fact.value_display || fact.value, '-')}`, evidence_pages: fact.evidence_pages }))} />
      </section>

      <OccupancySection section={report.occupancy_section} />
      <RiskSections sections={report.risk_sections} />
      <MoneySections sections={report.money_sections} />
      <ComplianceSection items={report.compliance_section} />
      <FormalitiesSection items={report.formalities_section} />
      <SurfacesSection items={report.surfaces_section} />

      <section className="space-y-3">
        <h3 className="text-lg font-semibold text-zinc-100">Checklist acquirente</h3>
        <TextList items={report.buyer_checklist} />
      </section>

      <section data-testid="cv2-manual-review-flags" className="space-y-3">
        <h3 className="text-lg font-semibold text-zinc-100">Punti da verificare</h3>
        <TextList items={(report.manual_review_flags || []).map((flag) => ({ text: `${compactText(flag.kind_label, 'Punto da verificare')}: ${compactText(flag.detail, '')}`, evidence_pages: flag.evidence_pages }))} />
      </section>

      <QualityControlSection qualityControl={report.quality_control} />

      <EvidenceIndex
        customerEvidence={report.customer_evidence_index}
        adminEvidence={report.admin_evidence_index}
        legacyEvidence={report.evidence_index}
      />

      {report.disclaimer && (
        <footer className="rounded-lg border border-zinc-800 bg-zinc-950 p-4 text-xs leading-5 text-zinc-500">
          {report.disclaimer}
        </footer>
      )}
    </article>
  );
};

const CorrectnessV2Panel = ({ analysisId, isAdmin }) => {
  const [job, setJob] = useState(null);
  const [report, setReport] = useState(null);
  const [loadingLatest, setLoadingLatest] = useState(false);
  const [starting, setStarting] = useState(false);
  const [polling, setPolling] = useState(false);
  const [pollAttempts, setPollAttempts] = useState(0);
  const [reportLoading, setReportLoading] = useState(false);
  const [error, setError] = useState('');

  const mountedRef = useRef(false);
  const startLockRef = useRef(false);
  const latestReportJobRef = useRef(null);
  const startControllerRef = useRef(null);

  const fetchReportForJob = useCallback(async (statusJob, signal) => {
    if (!reportCanBeFetched(statusJob)) return;
    const jobId = statusJob.job_id;
    if (latestReportJobRef.current === jobId) return;

    setReportLoading(true);
    try {
      const response = await getCorrectnessV2CustomerReport(analysisId, jobId, { signal });
      if (!mountedRef.current) return;
      latestReportJobRef.current = jobId;
      setReport(response.data);
    } catch (err) {
      if (isCanceledError(err) || !mountedRef.current) return;
      if (statusJob.status === 'REPORT_READY' || statusJob.status === 'LOT_SELECTION_REQUIRED') {
        setError('customer_report.json non disponibile per questo job.');
      }
    } finally {
      if (mountedRef.current) setReportLoading(false);
    }
  }, [analysisId]);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      if (startControllerRef.current) startControllerRef.current.abort();
    };
  }, []);

  useEffect(() => {
    if (!isAdmin || !analysisId) return undefined;
    const controller = new AbortController();
    setLoadingLatest(true);
    getLatestCorrectnessV2Job(analysisId, { signal: controller.signal })
      .then((response) => {
        if (!mountedRef.current) return;
        const latest = response.data;
        setJob(latest);
        if (isTerminalStatus(latest?.status)) {
          fetchReportForJob(latest, controller.signal);
        }
      })
      .catch((err) => {
        if (isCanceledError(err) || err?.response?.status === 404) return;
        if (mountedRef.current) setError('Impossibile leggere l ultimo job Correctness V2.');
      })
      .finally(() => {
        if (mountedRef.current) setLoadingLatest(false);
      });
    return () => controller.abort();
  }, [analysisId, fetchReportForJob, isAdmin]);

  useEffect(() => {
    if (!isAdmin || !analysisId || !job?.job_id || !isRunningStatus(job.status)) return undefined;

    let stopped = false;
    let timeoutId = null;
    let attempts = 0;
    let currentController = null;
    setPolling(true);
    setPollAttempts(0);

    const schedule = () => {
      timeoutId = window.setTimeout(pollOnce, POLL_INTERVAL_MS);
    };

    const pollOnce = async () => {
      if (stopped) return;
      attempts += 1;
      setPollAttempts(attempts);
      currentController = new AbortController();
      try {
        const response = await getCorrectnessV2Job(analysisId, job.job_id, { signal: currentController.signal });
        if (stopped || !mountedRef.current) return;
        const nextJob = response.data;
        setJob(nextJob);
        if (isTerminalStatus(nextJob?.status)) {
          setPolling(false);
          fetchReportForJob(nextJob, currentController.signal);
          return;
        }
        if (attempts >= MAX_POLL_ATTEMPTS) {
          setPolling(false);
          setError('Timeout polling Correctness V2: il job non ha raggiunto uno stato terminale.');
          return;
        }
        schedule();
      } catch (err) {
        if (stopped || isCanceledError(err) || !mountedRef.current) return;
        if (attempts >= MAX_POLL_ATTEMPTS) {
          setPolling(false);
          setError('Polling Correctness V2 interrotto dopo troppi errori.');
          return;
        }
        schedule();
      }
    };

    schedule();
    return () => {
      stopped = true;
      if (timeoutId) window.clearTimeout(timeoutId);
      if (currentController) currentController.abort();
    };
  }, [analysisId, fetchReportForJob, isAdmin, job?.job_id, job?.status]);

  const beginJob = useCallback(async (options = {}) => {
    if (!isAdmin || !analysisId || startLockRef.current || isRunningStatus(job?.status)) return;
    startLockRef.current = true;
    setStarting(true);
    setError('');
    setReport(null);
    latestReportJobRef.current = null;

    const controller = new AbortController();
    startControllerRef.current = controller;
    try {
      if (!options.selected_lot_id && !options.analyze_all) {
        try {
          const latestResponse = await getLatestCorrectnessV2Job(analysisId, { signal: controller.signal });
          const latest = latestResponse.data;
          if (isRunningStatus(latest?.status)) {
            if (mountedRef.current) setJob(latest);
            return;
          }
        } catch (latestErr) {
          if (isCanceledError(latestErr)) return;
          if (latestErr?.response?.status !== 404) {
            throw latestErr;
          }
        }
      }

      const response = await startCorrectnessV2(analysisId, options, { signal: controller.signal });
      if (!mountedRef.current) return;
      const startedJob = response.data;
      setJob(startedJob);
      if (isTerminalStatus(startedJob?.status)) {
        fetchReportForJob(startedJob, controller.signal);
      }
    } catch (err) {
      if (!isCanceledError(err) && mountedRef.current) {
        const detail = err?.response?.data?.detail;
        setError(compactText(detail?.reason_human || detail || err?.message, 'Avvio Correctness V2 fallito.'));
      }
    } finally {
      if (mountedRef.current) {
        setStarting(false);
      }
      startLockRef.current = false;
      startControllerRef.current = null;
    }
  }, [analysisId, fetchReportForJob, isAdmin, job?.status]);

  const handleRun = useCallback(() => beginJob({}), [beginJob]);
  const handleSelectLot = useCallback((lotId) => beginJob({ selected_lot_id: lotId }), [beginJob]);

  if (!isAdmin) return null;

  const status = job?.status;
  const activeJob = isRunningStatus(status);
  const showManualReview = status === 'NEEDS_MANUAL_REVIEW' || status === 'CONTRACT_VALIDATION_FAILED' || isFailureStatus(status);
  // A report whose own quality control failed is NEVER rendered as clean, even
  // if a stale/buggy status still claims REPORT_READY (defense in depth).
  const coverageFailed = report?.quality_control?.coverage_status === 'FAIL';
  const canRenderFullReport = status === 'REPORT_READY' && report?.report_status === 'REPORT_READY' && !coverageFailed;

  return (
    <section className="mb-8 space-y-4 rounded-lg border border-gold/30 bg-zinc-900/80 p-4 sm:p-5">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <div className="flex flex-wrap items-center gap-2">
            <p className="text-[11px] font-mono uppercase text-gold">Admin preview</p>
            <StatusBadge status={status || 'IDLE'} />
          </div>
          <h2 className="mt-2 text-xl font-serif font-bold text-zinc-100">Correctness Mode V2</h2>
          <p className="mt-1 max-w-2xl text-sm text-zinc-500">
            Preview isolata dal report legacy. Il contenuto viene renderizzato solo da customer_report.json.
          </p>
        </div>
        <CorrectnessV2RunButton
          onRun={handleRun}
          disabled={starting || loadingLatest || activeJob}
          hasJob={Boolean(job)}
          starting={starting || loadingLatest}
        />
      </div>

      {loadingLatest && <LoadingLine>Ricerca ultimo job Correctness V2...</LoadingLine>}
      <CorrectnessV2Status
        job={job}
        polling={polling}
        pollAttempts={pollAttempts}
        reportLoading={reportLoading}
        error={error}
      />

      {status === 'LOT_SELECTION_REQUIRED' && (
        <CorrectnessV2LotSelector
          job={job}
          report={report}
          onSelectLot={handleSelectLot}
          disabled={starting || activeJob}
        />
      )}

      {status === 'CONTRACT_READY' && !report && <ContractReadyBox job={job} />}

      {showManualReview && (
        <ManualReviewBox job={job} report={report} />
      )}

      {showManualReview && report?.quality_control && (
        <QualityControlSection qualityControl={report.quality_control} />
      )}

      {coverageFailed && !showManualReview && (
        <div data-testid="cv2-coverage-failed" className="rounded-lg border border-red-500/40 bg-red-500/10 p-4 text-sm text-red-100">
          Controllo qualità non superato: il report non viene mostrato come completo.
          Consultare coverage_audit.json e quality_standard_report.json.
        </div>
      )}
      {coverageFailed && !showManualReview && report?.quality_control && (
        <QualityControlSection qualityControl={report.quality_control} />
      )}

      {canRenderFullReport ? (
        <CorrectnessV2CustomerReport report={report} />
      ) : status === 'REPORT_READY' && reportLoading ? (
        <LoadingLine>Preparazione preview report...</LoadingLine>
      ) : status === 'REPORT_READY' && !report ? (
        <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 p-4 text-sm text-amber-200">
          REPORT_READY ricevuto, ma customer_report.json non e ancora caricato.
        </div>
      ) : null}

      {!job && !loadingLatest && !error && (
        <div className="flex items-start gap-3 rounded-lg border border-zinc-800 bg-zinc-950 p-4 text-sm text-zinc-400">
          <CheckCircle2 className="mt-0.5 h-4 w-4 text-zinc-500" />
          <p>Nessun job V2 trovato per questa analisi.</p>
        </div>
      )}

      {activeJob && (
        <div className="flex items-start gap-3 rounded-lg border border-sky-500/30 bg-sky-500/5 p-4 text-sm text-sky-200">
          <AlertTriangle className="mt-0.5 h-4 w-4" />
          <p>Job in corso: il pulsante resta disabilitato per evitare avvii duplicati.</p>
        </div>
      )}
    </section>
  );
};

export {
  CorrectnessV2CustomerReport,
  CorrectnessV2LotSelector,
  CorrectnessV2Status,
  MoneySections,
  RiskSections,
  BeniSections,
  OccupancySection,
  ComplianceSection,
  FormalitiesSection,
  SurfacesSection,
  QualityControlSection,
  isRunningStatus,
  isTerminalStatus,
};

export default CorrectnessV2Panel;
