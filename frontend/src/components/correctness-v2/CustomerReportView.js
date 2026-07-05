import React, { useState } from 'react';
import { AlertTriangle, ArrowLeft, CheckCircle2, Info, Loader2, Search } from 'lucide-react';
import { Button } from '../ui/button';
import { Input } from '../ui/input';
import { compactText, pagesText, DetailBlock, TextList } from './shared';
import {
  RiskSections,
  BeniSections,
  OccupancySection,
  ComplianceSection,
  FormalitiesSection,
} from './CorrectnessV2Panel';
import { useCorrectnessV2CustomerView } from './useCustomerView';

const EVIDENCE_RENDER_LIMIT = 60;
const LOT_RENDER_LIMIT = 40;

// Executive decision level -> tone. The level is computed server-side in
// customer_view.derive_decision; the customer never sees internal codes.
const DECISION_TONES = {
  attenzione: {
    box: 'border-red-500/40 bg-red-500/10',
    title: 'text-red-100',
    icon: AlertTriangle,
    iconClass: 'text-red-300',
  },
  da_verificare: {
    box: 'border-amber-500/40 bg-amber-500/10',
    title: 'text-amber-100',
    icon: Info,
    iconClass: 'text-amber-300',
  },
  pronto_con_avvertenze: {
    box: 'border-emerald-500/40 bg-emerald-500/10',
    title: 'text-emerald-100',
    icon: CheckCircle2,
    iconClass: 'text-emerald-300',
  },
};

// ---------------------------------------------------------------------------
// Executive decision box (customer language only)
// ---------------------------------------------------------------------------
const CustomerDecisionBox = ({ decision }) => {
  if (!decision) return null;
  const tone = DECISION_TONES[decision.level] || DECISION_TONES.da_verificare;
  const Icon = tone.icon;
  const drivers = Array.isArray(decision.drivers) ? decision.drivers : [];
  return (
    <section data-testid="cv2-customer-decision" className={`space-y-3 rounded-lg border ${tone.box} p-4`}>
      <div className="flex items-start gap-3">
        <Icon className={`mt-0.5 h-5 w-5 shrink-0 ${tone.iconClass}`} />
        <div className="min-w-0">
          <p className={`text-base font-semibold ${tone.title}`}>{compactText(decision.headline, decision.label)}</p>
          {decision.reason && <p className="mt-1 text-sm text-zinc-200">{compactText(decision.reason)}</p>}
        </div>
      </div>
      {drivers.length > 0 && (
        <ul className="list-disc space-y-1 pl-9 text-sm text-zinc-300">
          {drivers.map((driver, idx) => (
            <li key={`driver-${idx}`}>{compactText(driver)}</li>
          ))}
        </ul>
      )}
    </section>
  );
};

// ---------------------------------------------------------------------------
// Property summary (case identity + lot structure in customer language)
// ---------------------------------------------------------------------------
const PROPERTY_LABELS = {
  tribunale: 'Tribunale',
  procedura_rge: 'Procedura / RGE',
  lotto: 'Lotto',
  address: 'Indirizzo',
  property_type: 'Tipologia',
  ownership_right: 'Diritto',
};

const CustomerPropertySummary = ({ caseIdentity, lotStructure }) => {
  const identity = caseIdentity && typeof caseIdentity === 'object' ? caseIdentity : {};
  const lot = lotStructure && typeof lotStructure === 'object' ? lotStructure : {};
  const rows = [];
  Object.entries(PROPERTY_LABELS).forEach(([key, label]) => {
    if (identity[key] !== undefined && identity[key] !== null && identity[key] !== '') {
      rows.push([label, compactText(identity[key])]);
    }
  });
  if (lot.selected_lot) rows.push(['Lotto selezionato', compactText(lot.selected_lot)]);
  if (Number(lot.bene_count) > 1) rows.push(['Numero beni nel lotto', compactText(lot.bene_count)]);
  if (!rows.length) return null;
  return (
    <section data-testid="cv2-customer-summary" className="space-y-3">
      <h3 className="text-lg font-semibold text-zinc-100">Riepilogo immobile</h3>
      <dl className="grid grid-cols-1 gap-3 md:grid-cols-2">
        {rows.map(([label, value]) => (
          <div key={label} className="min-w-0 rounded-md border border-zinc-800 bg-zinc-950 p-3">
            <dt className="text-[11px] uppercase text-zinc-500">{label}</dt>
            <dd className="mt-1 break-words text-sm text-zinc-200">{value}</dd>
          </div>
        ))}
      </dl>
    </section>
  );
};

// ---------------------------------------------------------------------------
// Money summary (final chain + buyer costs + context; no admin buckets)
// ---------------------------------------------------------------------------
const MoneyRows = ({ rows }) => (
  <ul className="space-y-2">
    {rows.map((row, idx) => (
      <li key={`${row?.label || 'money'}-${idx}`} className="rounded-md border border-zinc-800 bg-zinc-950 p-3 text-sm">
        <div className="flex flex-wrap items-baseline justify-between gap-2">
          <span className="break-words text-zinc-200">{compactText(row?.label || row?.kind, 'Importo')}</span>
          <span className="shrink-0 font-mono text-gold">{compactText(row?.amount_display || row?.amount, '-')}</span>
        </div>
        {row?.included_in_valuation && (
          <span className="mt-1 inline-block rounded border border-sky-500/40 bg-sky-500/10 px-1.5 py-0.5 text-[10px] uppercase text-sky-300">
            Già incluso nel valore finale
          </span>
        )}
        {row?.notes && <p className="mt-1 text-xs text-zinc-500">{compactText(row.notes)}</p>}
        {pagesText(row?.evidence_pages) && <p className="mt-1 font-mono text-xs text-gold">{pagesText(row.evidence_pages)}</p>}
      </li>
    ))}
  </ul>
);

const MoneyGroup = ({ title, rows, note }) => {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length) return null;
  return (
    <div className="space-y-2">
      <p className="text-sm font-semibold text-zinc-100">{title}</p>
      {note && <p className="text-xs text-zinc-500">{note}</p>}
      <MoneyRows rows={list} />
    </div>
  );
};

const CustomerMoneySummary = ({ money }) => {
  const sections = money && typeof money === 'object' ? money : {};
  const hasAny = ['valuation_chain', 'auction_terms', 'buyer_side_costs', 'procedure_cancelled_formalities', 'uncertain_money']
    .some((key) => Array.isArray(sections[key]) && sections[key].length);
  if (!hasAny) return null;
  return (
    <section data-testid="cv2-customer-money" className="space-y-4">
      <h3 className="text-lg font-semibold text-zinc-100">Sintesi economica</h3>
      <MoneyGroup title="Catena di valore" rows={sections.valuation_chain} />
      <MoneyGroup title="Condizioni di vendita" rows={sections.auction_terms} />
      <MoneyGroup
        title="Costi a carico dell'acquirente"
        rows={sections.buyer_side_costs}
        note="Se una voce è contrassegnata come «già inclusa nel valore finale», non va sommata di nuovo."
      />
      <MoneyGroup
        title="Formalità cancellate dalla procedura"
        rows={sections.procedure_cancelled_formalities}
        note="Importi cancellati dalla procedura: non sono debiti a carico dell'acquirente, salvo diversa indicazione nella perizia."
      />
      <MoneyGroup
        title="Importi da verificare"
        rows={sections.uncertain_money}
        note="Il ruolo di questi importi non è chiaro nella perizia e va verificato."
      />
    </section>
  );
};

// ---------------------------------------------------------------------------
// Evidence (page + topic + verbatim excerpt only; never raw internal keys)
// ---------------------------------------------------------------------------
const CustomerEvidence = ({ evidence }) => {
  const items = Array.isArray(evidence) ? evidence : [];
  if (!items.length) return null;
  const visible = items.slice(0, EVIDENCE_RENDER_LIMIT);
  return (
    <DetailBlock title={`Evidenze in perizia (${items.length})`} testId="cv2-customer-evidence" defaultOpen>
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
          </li>
        ))}
      </ul>
      {items.length > EVIDENCE_RENDER_LIMIT && (
        <p className="mt-3 text-xs text-zinc-500">Altre {items.length - EVIDENCE_RENDER_LIMIT} evidenze non mostrate.</p>
      )}
    </DetailBlock>
  );
};

// ---------------------------------------------------------------------------
// Customer lot selector (choose which already-analyzed lot report to view).
// Selecting a lot re-fetches the sanitized report for that lot; it never starts
// a pipeline run (that is admin-only).
// ---------------------------------------------------------------------------
const lotSearchText = (lot) => [
  lot?.lot_id,
  lot?.label,
  lot?.address,
  lot?.property_type,
  lot?.ownership_right,
  lot?.occupancy_summary,
].join(' ').toLowerCase();

const CustomerLotSelector = ({ selection, onSelectLot, disabled }) => {
  const [query, setQuery] = useState('');
  const lots = Array.isArray(selection?.lots) ? selection.lots : [];
  const needle = query.trim().toLowerCase();
  const filtered = needle ? lots.filter((lot) => lotSearchText(lot).includes(needle)) : lots;
  const visible = filtered.slice(0, LOT_RENDER_LIMIT);

  if (!lots.length) {
    return (
      <div data-testid="cv2-customer-lot-selector" className="rounded-lg border border-amber-500/30 bg-amber-500/5 p-4 text-sm text-amber-200">
        Sono presenti più lotti, ma l'elenco non è disponibile al momento.
      </div>
    );
  }

  return (
    <section data-testid="cv2-customer-lot-selector" className="space-y-4 rounded-lg border border-zinc-800 bg-zinc-900 p-4">
      <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
        <div>
          <h3 className="text-lg font-semibold text-zinc-100">Scegli il lotto da consultare</h3>
          <p className="text-sm text-zinc-500">{compactText(selection?.message, 'Seleziona un lotto per vedere il relativo report.')}</p>
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

      <div className="grid grid-cols-1 gap-3 xl:grid-cols-2">
        {visible.map((lot, idx) => {
          const lotId = compactText(lot?.lot_id, String(idx + 1));
          const moneyRows = Array.isArray(lot?.money_summary) ? lot.money_summary : [];
          return (
            <article key={`${lotId}-${idx}`} className="rounded-lg border border-zinc-800 bg-zinc-950 p-4">
              <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                <div className="min-w-0">
                  <h4 className="break-words text-base font-semibold text-zinc-100">{compactText(lot?.label, `Lotto ${lotId}`)}</h4>
                  <div className="mt-2 space-y-1 text-sm text-zinc-400">
                    {lot?.address && <p>Indirizzo: {compactText(lot.address)}</p>}
                    {lot?.property_type && <p>Tipo: {compactText(lot.property_type)}</p>}
                    {lot?.ownership_right && <p>Diritto: {compactText(lot.ownership_right)}</p>}
                    {lot?.occupancy_summary && <p>Occupazione: {compactText(lot.occupancy_summary)}</p>}
                  </div>
                </div>
                <Button
                  type="button"
                  data-testid={`cv2-customer-lot-view-${lotId}`}
                  onClick={() => onSelectLot(lotId)}
                  disabled={disabled}
                  className="shrink-0 bg-gold text-zinc-950 hover:bg-gold-dim"
                >
                  Vedi report lotto
                </Button>
              </div>
              {moneyRows.length > 0 && (
                <ul className="mt-3 space-y-1 text-sm text-zinc-300">
                  {moneyRows.slice(0, 3).map((row, rowIdx) => (
                    <li key={`${row?.label || 'money'}-${rowIdx}`} className="flex justify-between gap-3">
                      <span className="break-words">{compactText(row?.label || row?.kind, 'Importo')}</span>
                      <span className="shrink-0 font-mono text-gold">{compactText(row?.amount_display || row?.amount, '')}</span>
                    </li>
                  ))}
                </ul>
              )}
            </article>
          );
        })}
      </div>
    </section>
  );
};

// ---------------------------------------------------------------------------
// Report body
// ---------------------------------------------------------------------------
const CustomerReportBody = ({ report, onBackToLots, showBack }) => {
  const keyFacts = Array.isArray(report?.key_facts) ? report.key_facts : [];
  return (
    <article data-testid="cv2-customer-report" className="space-y-6 rounded-lg border border-zinc-800 bg-zinc-900 p-4">
      <header className="space-y-3 border-b border-zinc-800 pb-4">
        {showBack && (
          <button
            type="button"
            onClick={onBackToLots}
            data-testid="cv2-customer-back-to-lots"
            className="inline-flex items-center gap-1 text-xs text-zinc-400 hover:text-zinc-200"
          >
            <ArrowLeft className="h-3.5 w-3.5" /> Torna alla scelta del lotto
          </button>
        )}
        <div>
          <h2 className="text-2xl font-serif font-bold text-zinc-100">{compactText(report?.title, 'Report cliente')}</h2>
          {report?.subtitle && <p className="mt-1 text-sm text-zinc-400">{compactText(report.subtitle)}</p>}
        </div>
      </header>

      <CustomerDecisionBox decision={report?.decision} />
      <CustomerPropertySummary caseIdentity={report?.case_identity} lotStructure={report?.lot_structure} />

      {Array.isArray(report?.executive_summary) && report.executive_summary.length > 0 && (
        <section className="space-y-3">
          <h3 className="text-lg font-semibold text-zinc-100">Sintesi</h3>
          <TextList items={report.executive_summary} />
        </section>
      )}

      {keyFacts.length > 0 && (
        <section className="space-y-3">
          <h3 className="text-lg font-semibold text-zinc-100">Dati chiave</h3>
          <TextList items={keyFacts.map((fact) => ({
            text: `${compactText(fact.label, 'Dato')}: ${compactText(fact.value_display || fact.value, '-')}`,
            evidence_pages: fact.evidence_pages,
          }))} />
        </section>
      )}

      <OccupancySection section={report?.occupancy_section} />
      <CustomerMoneySummary money={report?.money_sections} />
      <RiskSections sections={report?.risk_sections} />
      <ComplianceSection items={report?.compliance_section} />
      <FormalitiesSection items={report?.formalities_section} />
      <BeniSections sections={report?.beni_sections} />

      {Array.isArray(report?.buyer_checklist) && report.buyer_checklist.length > 0 && (
        <section className="space-y-3">
          <h3 className="text-lg font-semibold text-zinc-100">Cosa verificare prima di procedere</h3>
          <TextList items={report.buyer_checklist} />
        </section>
      )}

      <CustomerEvidence evidence={report?.customer_evidence_index} />

      {report?.disclaimer && (
        <footer className="rounded-lg border border-zinc-800 bg-zinc-950 p-4 text-xs leading-5 text-zinc-500">
          {report.disclaimer}
        </footer>
      )}
    </article>
  );
};

// ---------------------------------------------------------------------------
// Top-level customer view: renders the sanitized report.
//
// When a `state` prop is supplied (from the shared useCorrectnessV2CustomerView
// hook lifted to the page) it is used directly, so the sanitized endpoint is
// fetched exactly once and the page's "is a safe V2 report available?" decision
// never diverges from what is rendered here. When no `state` is supplied the
// component self-fetches (kept for standalone use / unit tests).
// ---------------------------------------------------------------------------
const CustomerReportView = ({ analysisId, state: externalState }) => {
  const internalState = useCorrectnessV2CustomerView(analysisId, { enabled: !externalState });
  const state = externalState || internalState;
  const { loading, error, payload, report, isLotSelection, selectedLotId, selectLot, backToLots } = state;

  if (loading && !payload) {
    return (
      <div className="flex items-center gap-2 text-sm text-zinc-400">
        <Loader2 className="h-4 w-4 animate-spin" /> Caricamento report cliente...
      </div>
    );
  }

  if (error) {
    return <div className="rounded-lg border border-red-500/30 bg-red-500/5 p-4 text-sm text-red-200">{error}</div>;
  }

  if (!report) {
    return (
      <div data-testid="cv2-customer-unavailable" className="flex items-start gap-3 rounded-lg border border-zinc-800 bg-zinc-950 p-4 text-sm text-zinc-400">
        <Info className="mt-0.5 h-4 w-4 text-zinc-500" />
        <p>Il report cliente non è ancora disponibile per questa analisi.</p>
      </div>
    );
  }

  return (
    <div data-testid="cv2-customer-view" className="space-y-4">
      {loading && (
        <div className="flex items-center gap-2 text-xs text-zinc-500">
          <Loader2 className="h-3.5 w-3.5 animate-spin" /> Aggiornamento...
        </div>
      )}
      {isLotSelection ? (
        <CustomerLotSelector
          selection={report.lot_selection}
          onSelectLot={selectLot}
          disabled={loading}
        />
      ) : (
        <CustomerReportBody
          report={report}
          onBackToLots={backToLots}
          showBack={Boolean(selectedLotId)}
        />
      )}
    </div>
  );
};

export {
  CustomerDecisionBox,
  CustomerMoneySummary,
  CustomerPropertySummary,
  CustomerEvidence,
  CustomerLotSelector,
};

export default CustomerReportView;
