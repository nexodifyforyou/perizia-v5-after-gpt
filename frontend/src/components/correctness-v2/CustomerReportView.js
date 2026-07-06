import React, { useMemo, useState } from 'react';
import {
  AlertTriangle,
  ArrowLeft,
  Building2,
  CheckCircle2,
  ChevronDown,
  ClipboardList,
  Coins,
  Info,
  KeyRound,
  Loader2,
  Quote,
  Receipt,
  ScrollText,
  Search,
  ShieldCheck,
} from 'lucide-react';
import { Button } from '../ui/button';
import { Input } from '../ui/input';
import { compactText, pagesText, DetailBlock } from './shared';
import { useCorrectnessV2CustomerView } from './useCustomerView';

// ---------------------------------------------------------------------------
// Customer report: decision-oriented reading flow rendered ONLY from the
// sanitized customer payload. Every section is data-driven and hides itself
// when its data is absent; nothing here is specific to a particular perizia.
//
// Flow: decision hero -> cosa stai comprando -> occupazione -> numeri
// principali -> costi acquirente -> formalità cancellate -> conformità ->
// cosa verificare -> prove dalla perizia.
// ---------------------------------------------------------------------------

const LOT_RENDER_LIMIT = 40;
const EVIDENCE_PREVIEW_LIMIT = 7;
const EVIDENCE_FULL_RENDER_LIMIT = 200;
const CHECKLIST_PREVIEW_LIMIT = 8;

// Accent-insensitive normalization for generic text matching (never for display).
const normText = (value) => String(value ?? '')
  .normalize('NFKD')
  .replace(/[\u0300-\u036f]/g, '')
  .toLowerCase()
  .trim();

// One short useful sentence out of a (possibly long/noisy) excerpt: prefer a
// sentence carrying an amount or number, clamp the length, never invent text.
const shortExcerpt = (text, maxLen = 200) => {
  const clean = String(text ?? '').replace(/\s+/g, ' ').trim();
  if (!clean) return '';
  const sentences = clean.match(/[^.!?;]+[.!?;]?/g) || [clean];
  const trimmed = sentences.map((s) => s.trim()).filter((s) => s.length > 3);
  const pick = trimmed.find((s) => /[€\d]/.test(s)) || trimmed[0] || clean;
  if (pick.length <= maxLen) return pick;
  return `${pick.slice(0, maxLen).trimEnd()}…`;
};

// ---------------------------------------------------------------------------
// Tone system (shared visual language of the customer report)
// ---------------------------------------------------------------------------
const BADGE_TONES = {
  good: 'border-emerald-500/40 bg-emerald-500/10 text-emerald-300',
  caution: 'border-amber-500/40 bg-amber-500/10 text-amber-200',
  danger: 'border-red-500/40 bg-red-500/10 text-red-300',
  info: 'border-sky-500/40 bg-sky-500/10 text-sky-300',
  neutral: 'border-zinc-700 bg-zinc-800/60 text-zinc-300',
};

const ToneBadge = ({ tone = 'neutral', children }) => (
  <span className={`inline-flex items-center rounded-full border px-2.5 py-0.5 text-[11px] font-medium ${BADGE_TONES[tone] || BADGE_TONES.neutral}`}>
    {children}
  </span>
);

const PageRefs = ({ pages, className = '' }) => {
  const label = pagesText(pages);
  if (!label) return null;
  return <span className={`font-mono text-[11px] text-gold/80 ${className}`}>{label}</span>;
};

// Section shell: icon chip + title + optional hint, consistent spacing.
const Section = ({ icon: Icon, title, hint, children, testId }) => (
  <section data-testid={testId} className="space-y-3">
    <header className="flex items-start gap-3">
      <span className="mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-lg border border-zinc-700/60 bg-zinc-800/70">
        <Icon className="h-4 w-4 text-gold" />
      </span>
      <div className="min-w-0">
        <h3 className="text-base font-semibold text-zinc-100">{title}</h3>
        {hint && <p className="mt-0.5 text-xs leading-5 text-zinc-500">{hint}</p>}
      </div>
    </header>
    <div className="space-y-3">{children}</div>
  </section>
);

// ---------------------------------------------------------------------------
// A. Decision hero
// ---------------------------------------------------------------------------
const DECISION_TONES = {
  attenzione: {
    box: 'border-red-500/40 bg-gradient-to-br from-red-500/15 via-red-950/20 to-transparent',
    bar: 'bg-red-400',
    title: 'text-red-100',
    icon: AlertTriangle,
    iconClass: 'text-red-300',
    chip: 'danger',
  },
  da_verificare: {
    box: 'border-amber-500/40 bg-gradient-to-br from-amber-500/15 via-amber-950/20 to-transparent',
    bar: 'bg-amber-400',
    title: 'text-amber-100',
    icon: Info,
    iconClass: 'text-amber-300',
    chip: 'caution',
  },
  pronto_con_avvertenze: {
    box: 'border-emerald-500/40 bg-gradient-to-br from-emerald-500/15 via-emerald-950/20 to-transparent',
    bar: 'bg-emerald-400',
    title: 'text-emerald-100',
    icon: CheckCircle2,
    iconClass: 'text-emerald-300',
    chip: 'good',
  },
};

const CustomerDecisionBox = ({ decision }) => {
  if (!decision) return null;
  const tone = DECISION_TONES[decision.level] || DECISION_TONES.da_verificare;
  const Icon = tone.icon;
  const drivers = (Array.isArray(decision.drivers) ? decision.drivers : []).slice(0, 6);
  return (
    <section
      data-testid="cv2-customer-decision"
      className={`relative overflow-hidden rounded-2xl border p-5 pl-6 ${tone.box}`}
    >
      <span className={`absolute inset-y-0 left-0 w-1 ${tone.bar}`} aria-hidden="true" />
      <div className="flex items-start gap-3.5">
        <span className="mt-0.5 flex h-9 w-9 shrink-0 items-center justify-center rounded-full border border-zinc-700/50 bg-zinc-950/40">
          <Icon className={`h-5 w-5 ${tone.iconClass}`} />
        </span>
        <div className="min-w-0">
          {decision.label && <ToneBadge tone={tone.chip}>{compactText(decision.label)}</ToneBadge>}
          <p className={`mt-2 text-lg font-semibold leading-snug ${tone.title}`}>
            {compactText(decision.headline, decision.label)}
          </p>
          {decision.reason && (
            <p className="mt-1.5 text-sm leading-6 text-zinc-300">{compactText(decision.reason)}</p>
          )}
        </div>
      </div>
      {drivers.length > 0 && (
        <ul className="mt-4 grid grid-cols-1 gap-1.5 pl-1 sm:grid-cols-2">
          {drivers.map((driver, idx) => (
            <li key={`driver-${idx}`} className="flex items-start gap-2 text-sm text-zinc-300">
              <span className={`mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full ${tone.bar}`} aria-hidden="true" />
              <span className="min-w-0">{compactText(driver)}</span>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
};

// ---------------------------------------------------------------------------
// B. Cosa stai comprando (case identity + lot + beni, rendered ONCE)
// ---------------------------------------------------------------------------
const PROPERTY_LABELS = {
  tribunale: 'Tribunale',
  procedura_rge: 'Procedura / RGE',
  lotto: 'Lotto',
  address: 'Indirizzo',
  property_type: 'Tipologia',
  ownership_right: 'Diritto',
};

// Key facts already covered by the identity grid / occupancy / money sections
// are suppressed here so the same fact never renders twice on the page.
const dedupedExtraFacts = (report) => {
  const keyFacts = Array.isArray(report?.key_facts) ? report.key_facts : [];
  if (!keyFacts.length) return [];
  const identityLabels = new Set(Object.values(PROPERTY_LABELS).map(normText));
  identityLabels.add(normText('Lotto selezionato'));
  const hasOccupancy = Boolean(report?.occupancy_section?.status || report?.occupancy_section?.status_label);
  const money = report?.money_sections || {};
  const moneyAmounts = new Set(
    ['valuation_chain', 'auction_terms', 'buyer_side_costs']
      .flatMap((key) => (Array.isArray(money[key]) ? money[key] : []))
      .map((row) => normText(row?.amount_display || row?.amount))
      .filter(Boolean)
  );
  return keyFacts.filter((fact) => {
    const label = normText(fact?.label);
    if (!label || identityLabels.has(label)) return false;
    if (hasOccupancy && label.includes('occupaz')) return false;
    const value = normText(fact?.value_display || fact?.value);
    if (value && moneyAmounts.has(value)) return false;
    return true;
  });
};

const AccessoryChips = ({ accessories }) => {
  const list = Array.isArray(accessories) ? accessories : [];
  if (!list.length) return null;
  return (
    <div className="mt-3">
      <p className="text-[11px] uppercase tracking-wide text-zinc-500">Accessori e pertinenze indicati in perizia</p>
      <div className="mt-1.5 flex flex-wrap gap-1.5">
        {list.map((acc, idx) => (
          <span
            key={`${acc?.label || 'acc'}-${idx}`}
            className="inline-flex items-baseline gap-1.5 rounded-full border border-zinc-700 bg-zinc-900 px-2.5 py-1 text-xs capitalize text-zinc-200"
          >
            {compactText(acc?.label, 'Accessorio')}
            <PageRefs pages={acc?.evidence_pages} />
          </span>
        ))}
      </div>
    </div>
  );
};

const CustomerPropertySection = ({ report }) => {
  const identity = report?.case_identity && typeof report.case_identity === 'object' ? report.case_identity : {};
  const lot = report?.lot_structure && typeof report.lot_structure === 'object' ? report.lot_structure : {};
  const beni = Array.isArray(report?.beni_sections) ? report.beni_sections : [];

  const rows = [];
  Object.entries(PROPERTY_LABELS).forEach(([key, label]) => {
    if (identity[key] !== undefined && identity[key] !== null && identity[key] !== '') {
      rows.push([label, compactText(identity[key])]);
    }
  });
  if (lot.selected_lot && !identity.lotto) rows.push(['Lotto selezionato', compactText(lot.selected_lot)]);
  const extraFacts = dedupedExtraFacts(report);

  const mainBene = beni.length === 1 && beni[0]?.is_main_property ? beni[0] : null;
  const multiBeni = !mainBene && beni.length > 0 ? beni : null;

  if (!rows.length && !extraFacts.length && !beni.length) return null;

  return (
    <Section
      icon={Building2}
      title="Cosa stai comprando"
      hint="Identificazione del bene secondo la perizia."
      testId="cv2-customer-summary"
    >
      {(rows.length > 0 || extraFacts.length > 0) && (
        <dl className="grid grid-cols-1 gap-2.5 sm:grid-cols-2 lg:grid-cols-3">
          {rows.map(([label, value]) => (
            <div key={label} className="min-w-0 rounded-lg border border-zinc-800 bg-zinc-950/70 p-3">
              <dt className="text-[11px] uppercase tracking-wide text-zinc-500">{label}</dt>
              <dd className="mt-1 break-words text-sm text-zinc-200">{value}</dd>
            </div>
          ))}
          {extraFacts.map((fact, idx) => (
            <div key={`extra-${idx}`} className="min-w-0 rounded-lg border border-zinc-800 bg-zinc-950/70 p-3">
              <dt className="text-[11px] uppercase tracking-wide text-zinc-500">{compactText(fact?.label, 'Dato')}</dt>
              <dd className="mt-1 break-words text-sm text-zinc-200">{compactText(fact?.value_display || fact?.value, '-')}</dd>
            </div>
          ))}
        </dl>
      )}

      {Number(lot.bene_count) > 1 && (
        <p className="text-sm text-zinc-400">
          Il lotto comprende <span className="font-semibold text-zinc-200">{compactText(lot.bene_count)} beni</span>
          {lot.selected_lot ? <> (lotto {compactText(lot.selected_lot)})</> : null}.
        </p>
      )}

      {mainBene && (
        <div className="rounded-lg border border-zinc-800 bg-zinc-950/70 p-4">
          <p className="text-sm font-medium text-zinc-100">{compactText(mainBene.title, 'Bene principale')}</p>
          {mainBene.address && <p className="mt-1 text-sm text-zinc-400">{compactText(mainBene.address)}</p>}
          <AccessoryChips accessories={mainBene.accessories} />
        </div>
      )}

      {multiBeni && (
        <div className="grid grid-cols-1 gap-2.5 lg:grid-cols-2">
          {multiBeni.map((bene, idx) => {
            const risks = Array.isArray(bene?.risks) ? bene.risks : [];
            const checklist = Array.isArray(bene?.checklist) ? bene.checklist : [];
            return (
              <div key={`${bene?.bene_id || idx}`} className="rounded-lg border border-zinc-800 bg-zinc-950/70 p-4">
                <p className="text-sm font-medium text-zinc-100">{compactText(bene?.title, `Bene ${idx + 1}`)}</p>
                {bene?.address && <p className="mt-1 text-sm text-zinc-400">{compactText(bene.address)}</p>}
                {bene?.note && <p className="mt-2 text-xs leading-5 text-zinc-500">{compactText(bene.note)}</p>}
                <AccessoryChips accessories={bene?.accessories} />
                {(risks.length > 0 || checklist.length > 0) && (
                  <details className="mt-3 text-sm">
                    <summary className="cursor-pointer text-xs text-zinc-500 hover:text-zinc-300">
                      Dettagli segnalati per questo bene ({risks.length + checklist.length})
                    </summary>
                    <ul className="mt-2 space-y-1.5 text-zinc-300">
                      {risks.map((risk, rIdx) => (
                        <li key={`bene-risk-${rIdx}`} className="flex items-start gap-2">
                          <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-amber-300" />
                          <span>
                            {compactText(risk?.area, 'Segnalazione')}
                            {risk?.summary ? `: ${compactText(risk.summary)}` : ''}
                            {' '}<PageRefs pages={risk?.evidence_pages} />
                          </span>
                        </li>
                      ))}
                      {checklist.map((item, cIdx) => (
                        <li key={`bene-check-${cIdx}`} className="flex items-start gap-2">
                          <ClipboardList className="mt-0.5 h-3.5 w-3.5 shrink-0 text-zinc-500" />
                          <span>
                            {compactText(item?.detail || item?.action || item?.text, '')}
                            {' '}<PageRefs pages={item?.evidence_pages} />
                          </span>
                        </li>
                      ))}
                    </ul>
                  </details>
                )}
              </div>
            );
          })}
        </div>
      )}
    </Section>
  );
};

// ---------------------------------------------------------------------------
// C. Occupazione / disponibilità
// ---------------------------------------------------------------------------
const occupancyTone = (section) => {
  const status = normText(`${section?.status || ''} ${section?.status_label || ''}`);
  if (status.includes('liber')) return 'good';
  if (status.includes('occupat')) return 'caution';
  return 'neutral';
};

// Generic customer-language meaning line, derived only from the normalized
// occupancy status class (never from perizia-specific content).
const occupancyMeaning = (tone) => {
  if (tone === 'good') {
    return "Secondo la perizia l'immobile risulta libero: in linea di massima la disponibilità non dipende dalla liberazione di occupanti, salvo verifiche in sede di trasferimento.";
  }
  if (tone === 'caution') {
    return "L'immobile risulta occupato: prima di procedere è importante capire chi lo occupa, con quale titolo e quali potrebbero essere i tempi di liberazione.";
  }
  return "Lo stato di occupazione non è chiaramente definito nella perizia e va verificato prima di procedere.";
};

const CustomerOccupancySection = ({ section }) => {
  if (!section || (!section.status && !section.status_label && !section.title_info)) return null;
  const tone = occupancyTone(section);
  const risks = (Array.isArray(section.risks) ? section.risks : []).slice(0, 3);
  return (
    <Section
      icon={KeyRound}
      title="Occupazione e disponibilità"
      testId="cv2-customer-occupancy"
    >
      <div className="rounded-lg border border-zinc-800 bg-zinc-950/70 p-4">
        <div className="flex flex-wrap items-center gap-2">
          {(section.status_label || section.status) && (
            <ToneBadge tone={tone}>{compactText(section.status_label || section.status)}</ToneBadge>
          )}
          <PageRefs pages={section.evidence_pages} />
        </div>
        <p className="mt-3 text-sm leading-6 text-zinc-300">{occupancyMeaning(tone)}</p>
        {section.title_info && (
          <p className="mt-2 text-sm leading-6 text-zinc-400">{compactText(section.title_info)}</p>
        )}
        {section.opponibility && (
          <p className="mt-2 text-sm leading-6 text-zinc-400">{compactText(section.opponibility)}</p>
        )}
        {(Array.isArray(section.registration_dates) && section.registration_dates.length > 0) && (
          <p className="mt-2 text-xs text-zinc-500">Registrazione contratto: {section.registration_dates.join(', ')}</p>
        )}
        {(Array.isArray(section.expiry_dates) && section.expiry_dates.length > 0) && (
          <p className="mt-1 text-xs text-zinc-500">Scadenza contratto: {section.expiry_dates.join(', ')}</p>
        )}
        {risks.length > 0 && (
          <ul className="mt-3 space-y-1.5">
            {risks.map((risk, idx) => (
              <li key={`occ-risk-${idx}`} className="flex items-start gap-2 text-sm text-amber-200/90">
                <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                <span>{compactText(risk)}</span>
              </li>
            ))}
          </ul>
        )}
      </div>
    </Section>
  );
};

// ---------------------------------------------------------------------------
// D. Numeri principali (valuation chain as a visual flow)
// ---------------------------------------------------------------------------
const ChainRow = ({ row, isFinal }) => {
  const isDeduction = normText(row?.kind) === 'deduction';
  return (
    <li
      className={`flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1 rounded-lg border p-3.5 ${
        isFinal
          ? 'border-gold/50 bg-gradient-to-r from-gold/10 to-transparent'
          : isDeduction
            ? 'border-zinc-800 bg-zinc-950/50'
            : 'border-zinc-800 bg-zinc-950/80'
      }`}
    >
      <span className="min-w-0 break-words text-sm text-zinc-300">
        {isDeduction && <span className="mr-1.5 font-mono text-amber-300">−</span>}
        {compactText(row?.label || row?.kind, 'Importo')}
        {' '}<PageRefs pages={row?.evidence_pages} />
      </span>
      <span
        className={`shrink-0 font-mono ${
          isFinal
            ? 'text-lg font-bold text-gold'
            : isDeduction
              ? 'text-sm text-amber-200'
              : 'text-sm text-zinc-100'
        }`}
      >
        {compactText(row?.amount_display || row?.amount, '—')}
      </span>
      {row?.included_in_valuation && (
        <span className="w-full">
          <ToneBadge tone="info">Già incluso nel valore finale</ToneBadge>
        </span>
      )}
      {row?.notes && <p className="w-full text-xs leading-5 text-zinc-500">{compactText(row.notes)}</p>}
    </li>
  );
};

const CustomerMoneySection = ({ money }) => {
  const sections = money && typeof money === 'object' ? money : {};
  const chain = Array.isArray(sections.valuation_chain) ? sections.valuation_chain : [];
  const auction = Array.isArray(sections.auction_terms) ? sections.auction_terms : [];
  if (!chain.length && !auction.length) return null;
  return (
    <Section
      icon={Coins}
      title="Numeri principali"
      hint="Come la perizia arriva al valore di vendita: dal valore di mercato alle detrazioni."
      testId="cv2-customer-money"
    >
      {chain.length > 0 && (
        <ol className="space-y-2">
          {chain.map((row, idx) => (
            <ChainRow key={`${row?.label || 'chain'}-${idx}`} row={row} isFinal={idx === chain.length - 1} />
          ))}
        </ol>
      )}
      {auction.length > 0 && (
        <div className="grid grid-cols-1 gap-2.5 sm:grid-cols-2">
          {auction.map((row, idx) => (
            <div key={`auction-${idx}`} className="rounded-lg border border-sky-500/25 bg-sky-500/5 p-3.5">
              <p className="text-[11px] uppercase tracking-wide text-sky-300/80">{compactText(row?.label, 'Condizione di vendita')}</p>
              <p className="mt-1 font-mono text-base font-semibold text-zinc-100">
                {compactText(row?.amount_display || row?.amount, '—')}
              </p>
              <PageRefs pages={row?.evidence_pages} />
            </div>
          ))}
        </div>
      )}
    </Section>
  );
};

// ---------------------------------------------------------------------------
// E. Costi per l'acquirente + formalità cancellate dalla procedura
// ---------------------------------------------------------------------------
const CustomerCostsSection = ({ money }) => {
  const sections = money && typeof money === 'object' ? money : {};
  const buyerCosts = Array.isArray(sections.buyer_side_costs) ? sections.buyer_side_costs : [];
  const uncertain = Array.isArray(sections.uncertain_money) ? sections.uncertain_money : [];
  if (!buyerCosts.length && !uncertain.length) return null;
  return (
    <Section
      icon={Receipt}
      title="Costi a carico dell'acquirente"
      hint="Solo le voci che la perizia indica come costi reali o potenziali per chi acquista."
      testId="cv2-customer-costs"
    >
      {buyerCosts.length > 0 && (
        <ul className="space-y-2">
          {buyerCosts.map((row, idx) => (
            <li key={`cost-${idx}`} className="rounded-lg border border-amber-500/20 bg-amber-500/5 p-3.5">
              <div className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1">
                <span className="min-w-0 break-words text-sm text-zinc-200">
                  {compactText(row?.label || row?.kind, 'Costo')}
                  {' '}<PageRefs pages={row?.evidence_pages} />
                </span>
                <span className="shrink-0 font-mono text-sm font-semibold text-gold">
                  {compactText(row?.amount_display || row?.amount, '—')}
                </span>
              </div>
              {row?.included_in_valuation && (
                <div className="mt-1.5">
                  <ToneBadge tone="info">Già incluso nel valore finale</ToneBadge>
                  <p className="mt-1 text-xs text-zinc-500">Non va sommato di nuovo al prezzo.</p>
                </div>
              )}
              {row?.notes && <p className="mt-1.5 text-xs leading-5 text-zinc-500">{compactText(row.notes)}</p>}
            </li>
          ))}
        </ul>
      )}
      {uncertain.length > 0 && (
        <div className="rounded-lg border border-amber-500/25 bg-zinc-950/70 p-3.5">
          <p className="text-sm font-medium text-amber-200">Importi da chiarire</p>
          <p className="mt-0.5 text-xs leading-5 text-zinc-500">
            La perizia cita questi importi senza chiarirne il ruolo: vanno verificati prima di considerarli (o escluderli) come costi.
          </p>
          <ul className="mt-2 space-y-1.5">
            {uncertain.map((row, idx) => (
              <li key={`unc-${idx}`} className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-0.5 text-sm">
                <span className="min-w-0 break-words text-zinc-300">
                  {compactText(row?.label || row?.kind, 'Importo')}
                  {' '}<PageRefs pages={row?.evidence_pages} />
                </span>
                <span className="shrink-0 font-mono text-amber-200">{compactText(row?.amount_display || row?.amount, '—')}</span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </Section>
  );
};

const CustomerFormalitiesSection = ({ money, formalities }) => {
  const cancelled = Array.isArray(money?.procedure_cancelled_formalities)
    ? money.procedure_cancelled_formalities
    : [];
  const detailed = Array.isArray(formalities) ? formalities : [];
  if (!cancelled.length && !detailed.length) return null;
  // The detailed formality entries are the richer source; the money rows are a
  // fallback so the information never disappears on older payloads.
  const detailRows = detailed.length ? detailed : cancelled;
  const count = detailRows.length;
  return (
    <Section
      icon={ScrollText}
      title="Formalità e cancellazioni"
      testId="cv2-customer-formalities"
    >
      <div className="rounded-lg border border-sky-500/20 bg-sky-500/5 p-4">
        <p className="text-sm leading-6 text-zinc-300">
          La perizia indica <span className="font-semibold text-zinc-100">{count}</span>{' '}
          {count === 1 ? 'formalità' : 'formalità'} (ad esempio ipoteche o pignoramenti) di cui è prevista la
          cancellazione a cura della procedura. Gli importi indicati{' '}
          <span className="font-medium text-sky-200">non sono automaticamente debiti a carico dell'acquirente</span>,
          salvo diversa indicazione nella perizia.
        </p>
      </div>
      <DetailBlock title={`Dettaglio formalità (${count})`} testId="cv2-customer-formalities-detail">
        <ul className="space-y-2">
          {detailRows.map((item, idx) => (
            <li key={`form-${idx}`} className="rounded-lg border border-zinc-800 bg-zinc-950/70 p-3.5 text-sm">
              <div className="flex flex-wrap items-center gap-2">
                <ToneBadge tone="neutral">{compactText(item?.type_label || item?.type || item?.kind, 'Formalità')}</ToneBadge>
                {item?.status_label && <span className="text-xs text-zinc-400">{compactText(item.status_label)}</span>}
              </div>
              <p className="mt-2 break-words leading-6 text-zinc-300">
                {compactText(item?.description || item?.label, '')}
              </p>
              <div className="mt-1.5 flex flex-wrap items-baseline gap-x-4 gap-y-1">
                {(item?.amount_display || item?.amount) && (
                  <span className="font-mono text-zinc-200">{compactText(item?.amount_display || item?.amount)}</span>
                )}
                <PageRefs pages={item?.evidence_pages} />
              </div>
              {item?.amount_note && <p className="mt-1 text-xs leading-5 text-zinc-500">{compactText(item.amount_note)}</p>}
              {item?.notes && <p className="mt-1 text-xs leading-5 text-zinc-500">{compactText(item.notes)}</p>}
            </li>
          ))}
        </ul>
      </DetailBlock>
    </Section>
  );
};

// ---------------------------------------------------------------------------
// F. Conformità e documenti tecnici
// ---------------------------------------------------------------------------
const complianceTone = (item) => {
  const cls = normText(item?.classification);
  if (cls === 'conforming') return { tone: 'good', fallback: 'Conforme' };
  if (cls === 'regularizable') return { tone: 'caution', fallback: 'Regolarizzabile' };
  if (cls === 'non_conforming' || cls === 'not_regularizable') return { tone: 'danger', fallback: 'Non conforme' };
  if (cls === 'uncertain') return { tone: 'caution', fallback: 'Da verificare' };
  const label = normText(item?.status_label);
  if (label) {
    if (label.includes('non conform')) return { tone: 'danger', fallback: 'Non conforme' };
    if (label.includes('conform')) return { tone: 'good', fallback: 'Conforme' };
    if (label.includes('regolarizz')) return { tone: 'caution', fallback: 'Regolarizzabile' };
    if (label.includes('verificare')) return { tone: 'caution', fallback: 'Da verificare' };
  }
  return { tone: 'neutral', fallback: 'Non chiaro / non disponibile' };
};

// A short verbatim support line for a compliance card, taken from the customer
// evidence index when an excerpt exists on one of the card's own pages.
const findSupportExcerpt = (evidence, item) => {
  const items = Array.isArray(evidence) ? evidence : [];
  const pages = new Set((Array.isArray(item?.evidence_pages) ? item.evidence_pages : []).map(Number));
  if (!pages.size) return null;
  const area = normText(item?.area);
  const candidates = items.filter((entry) => entry?.perizia_excerpt && pages.has(Number(entry?.page)));
  if (!candidates.length) return null;
  const topicMatch = candidates.find((entry) => {
    const topic = normText(entry?.topic);
    return topic && area && (area.includes(topic) || topic.includes(area));
  });
  return topicMatch || candidates.find((entry) => normText(entry?.report_section).includes('conformita')) || null;
};

const CustomerComplianceSection = ({ items, evidence }) => {
  const list = Array.isArray(items) ? items : [];
  if (!list.length) return null;
  return (
    <Section
      icon={ShieldCheck}
      title="Conformità e documenti tecnici"
      hint="Urbanistica, catasto, edilizia, impianti e certificazioni secondo la perizia."
      testId="cv2-customer-compliance"
    >
      <div className="grid grid-cols-1 gap-2.5 xl:grid-cols-2">
        {list.map((item, idx) => {
          const { tone, fallback } = complianceTone(item);
          const support = findSupportExcerpt(evidence, item);
          return (
            <article key={`${item?.area || 'area'}-${idx}`} className="flex flex-col rounded-lg border border-zinc-800 bg-zinc-950/70 p-4">
              <div className="flex flex-wrap items-start justify-between gap-2">
                <p className="min-w-0 break-words text-sm font-medium text-zinc-100">{compactText(item?.area, 'Area')}</p>
                <ToneBadge tone={tone}>{compactText(item?.status_label, fallback)}</ToneBadge>
              </div>
              {item?.notes && (
                <p className="mt-2 text-sm leading-6 text-zinc-400">{shortExcerpt(item.notes, 260)}</p>
              )}
              {(item?.cost_display || item?.timing || pagesText(item?.evidence_pages)) && (
                <div className="mt-2.5 flex flex-wrap items-baseline gap-x-4 gap-y-1 text-xs">
                  {item?.cost_display && (
                    <span className="font-mono font-semibold text-gold">{item.cost_display}</span>
                  )}
                  {item?.timing && <span className="text-zinc-400">Tempi: {compactText(item.timing)}</span>}
                  <PageRefs pages={item?.evidence_pages} />
                </div>
              )}
              {support && (
                <p className="mt-2.5 border-l-2 border-zinc-700 pl-3 text-xs italic leading-5 text-zinc-400">
                  p. {compactText(support.page, '?')} — “{shortExcerpt(support.perizia_excerpt)}”
                </p>
              )}
            </article>
          );
        })}
      </div>
    </Section>
  );
};

// Risk findings not already shown as compliance cards, kept out of the main
// flow (collapsed) so the page stays readable but nothing is lost.
const CustomerOtherFindings = ({ riskSections, complianceItems }) => {
  const sections = Array.isArray(riskSections) ? riskSections : [];
  const complianceAreas = new Set(
    (Array.isArray(complianceItems) ? complianceItems : []).map((item) => normText(item?.area)).filter(Boolean)
  );
  const leftovers = [];
  sections.forEach((section) => {
    (Array.isArray(section?.items) ? section.items : []).forEach((item) => {
      const area = normText(item?.area);
      if (area && complianceAreas.has(area)) return;
      leftovers.push(item);
    });
  });
  if (!leftovers.length) return null;
  const severityTone = (item) => {
    const sev = normText(item?.severity);
    if (sev === 'grave') return 'danger';
    if (sev === 'media') return 'caution';
    if (sev === 'da_verificare') return 'caution';
    return 'neutral';
  };
  return (
    <DetailBlock title={`Altre segnalazioni della perizia (${leftovers.length})`} testId="cv2-customer-other-findings">
      <ul className="space-y-2">
        {leftovers.map((item, idx) => (
          <li key={`finding-${idx}`} className="rounded-lg border border-zinc-800 bg-zinc-950/70 p-3.5 text-sm">
            <div className="flex flex-wrap items-center gap-2">
              {(item?.severity_label || item?.status_label) && (
                <ToneBadge tone={severityTone(item)}>{compactText(item?.severity_label || item?.status_label)}</ToneBadge>
              )}
              <span className="font-medium text-zinc-100">{compactText(item?.area || item?.title, 'Segnalazione')}</span>
            </div>
            {item?.summary && <p className="mt-1.5 leading-6 text-zinc-400">{compactText(item.summary)}</p>}
            <div className="mt-1.5 flex flex-wrap items-baseline gap-x-4 gap-y-1 text-xs">
              {item?.cost_display && <span className="font-mono text-gold">{item.cost_display}</span>}
              <PageRefs pages={item?.evidence_pages} />
            </div>
          </li>
        ))}
      </ul>
    </DetailBlock>
  );
};

// ---------------------------------------------------------------------------
// G. Cosa verificare prima di procedere
// ---------------------------------------------------------------------------
const checklistText = (item) => {
  if (typeof item === 'string') return item;
  const action = compactText(item?.action, '');
  const detail = compactText(item?.detail || item?.text || item?.summary, '');
  if (action && detail) return `${action}: ${detail}`;
  return action || detail;
};

const CustomerChecklistSection = ({ checklist }) => {
  const raw = Array.isArray(checklist) ? checklist : [];
  const seen = new Set();
  const items = [];
  raw.forEach((item) => {
    const text = checklistText(item);
    const key = normText(text);
    if (!text || seen.has(key)) return;
    seen.add(key);
    items.push({ text, evidence_pages: item?.evidence_pages, blocking: Boolean(item?.blocks_saleability) });
  });
  if (!items.length) return null;
  const visible = items.slice(0, CHECKLIST_PREVIEW_LIMIT);
  const rest = items.slice(CHECKLIST_PREVIEW_LIMIT);
  return (
    <Section
      icon={ClipboardList}
      title="Cosa verificare prima di procedere"
      hint="I passi concreti suggeriti dai punti emersi in perizia."
      testId="cv2-customer-checklist"
    >
      <ol className="space-y-2">
        {visible.map((item, idx) => (
          <li key={`check-${idx}`} className="flex items-start gap-3 rounded-lg border border-zinc-800 bg-zinc-950/70 p-3.5">
            <span className="mt-0.5 flex h-6 w-6 shrink-0 items-center justify-center rounded-full border border-gold/40 bg-gold/10 font-mono text-xs text-gold">
              {idx + 1}
            </span>
            <div className="min-w-0 text-sm leading-6 text-zinc-200">
              <span className="break-words">{item.text}</span>
              {item.blocking && <span className="ml-2 align-middle"><ToneBadge tone="danger">Bloccante</ToneBadge></span>}
              {' '}<PageRefs pages={item.evidence_pages} />
            </div>
          </li>
        ))}
      </ol>
      {rest.length > 0 && (
        <DetailBlock title={`Altre verifiche (${rest.length})`} testId="cv2-customer-checklist-more">
          <ul className="space-y-1.5">
            {rest.map((item, idx) => (
              <li key={`check-more-${idx}`} className="flex items-start gap-2 text-sm leading-6 text-zinc-300">
                <span className="mt-2 h-1.5 w-1.5 shrink-0 rounded-full bg-zinc-600" aria-hidden="true" />
                <span className="min-w-0 break-words">
                  {item.text}
                  {' '}<PageRefs pages={item.evidence_pages} />
                </span>
              </li>
            ))}
          </ul>
        </DetailBlock>
      )}
    </Section>
  );
};

// ---------------------------------------------------------------------------
// H. Prove dalla perizia (curated preview + full collapsed list)
// ---------------------------------------------------------------------------
// Curated preview: prefer entries with a verbatim excerpt, spread across the
// report sections they support (diversity), then fill with excerpt-less
// entries only if there is room. Purely payload-driven.
const buildEvidencePreview = (items, max = EVIDENCE_PREVIEW_LIMIT) => {
  const list = Array.isArray(items) ? items : [];
  const withExcerpt = list.filter((entry) => entry?.perizia_excerpt);
  const withoutExcerpt = list.filter((entry) => !entry?.perizia_excerpt);
  const groups = new Map();
  withExcerpt.forEach((entry) => {
    const key = normText(entry?.report_section || entry?.topic || '');
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(entry);
  });
  const picked = [];
  let added = true;
  while (picked.length < max && added) {
    added = false;
    for (const bucket of groups.values()) {
      if (bucket.length && picked.length < max) {
        picked.push(bucket.shift());
        added = true;
      }
    }
  }
  for (const entry of withoutExcerpt) {
    if (picked.length >= Math.min(max, list.length)) break;
    picked.push(entry);
  }
  return picked.sort((a, b) => (Number(a?.page) || 0) - (Number(b?.page) || 0));
};

const EvidenceItem = ({ entry }) => (
  <li className="rounded-lg border border-zinc-800 bg-zinc-950/70 p-3.5 text-sm">
    <p className="text-zinc-100">
      <span className="font-mono text-gold">p. {compactText(entry?.page, '?')}</span>
      <span className="mx-2 text-zinc-600">—</span>
      <span className="font-medium">{compactText(entry?.topic, 'Tema')}</span>
    </p>
    {entry?.perizia_excerpt ? (
      <p className="mt-1.5 border-l-2 border-gold/30 pl-3 italic leading-6 text-zinc-300">
        “{shortExcerpt(entry.perizia_excerpt, 240)}”
      </p>
    ) : (
      <p className="mt-1.5 text-xs leading-5 text-amber-200/70">
        {compactText(entry?.note, `Estratto non disponibile automaticamente; verificare pagina ${compactText(entry?.page, '?')}.`)}
      </p>
    )}
  </li>
);

const CustomerEvidence = ({ evidence }) => {
  const [showAll, setShowAll] = useState(false);
  const items = useMemo(() => (Array.isArray(evidence) ? evidence : []), [evidence]);
  const preview = useMemo(() => buildEvidencePreview(items), [items]);
  if (!items.length) return null;
  const hasMore = items.length > preview.length;
  return (
    <Section
      icon={Quote}
      title="Prove dalla perizia"
      hint="Passaggi del documento che sostengono i punti principali del report."
      testId="cv2-customer-evidence"
    >
      {!showAll && (
        <ul className="space-y-2">
          {preview.map((entry, idx) => (
            <EvidenceItem key={`ev-preview-${entry?.page || '?'}-${idx}`} entry={entry} />
          ))}
        </ul>
      )}
      {showAll && (
        <ul className="space-y-2">
          {items.slice(0, EVIDENCE_FULL_RENDER_LIMIT).map((entry, idx) => (
            <EvidenceItem key={`ev-all-${entry?.page || '?'}-${idx}`} entry={entry} />
          ))}
          {items.length > EVIDENCE_FULL_RENDER_LIMIT && (
            <p className="text-xs text-zinc-500">Altre {items.length - EVIDENCE_FULL_RENDER_LIMIT} evidenze non mostrate.</p>
          )}
        </ul>
      )}
      {(hasMore || showAll) && (
        <button
          type="button"
          data-testid="cv2-customer-evidence-toggle"
          onClick={() => setShowAll((prev) => !prev)}
          className="inline-flex items-center gap-1.5 rounded-md border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-xs font-medium text-zinc-300 transition-colors hover:border-gold/40 hover:text-zinc-100"
        >
          <ChevronDown className={`h-3.5 w-3.5 transition-transform ${showAll ? 'rotate-180' : ''}`} />
          {showAll ? 'Mostra solo le evidenze principali' : `Mostra tutte le evidenze (${items.length})`}
        </button>
      )}
    </Section>
  );
};

// ---------------------------------------------------------------------------
// Customer lot selector (choose which already-analyzed lot report to view).
// Selecting a lot re-fetches the sanitized report for that lot; when the
// report does not exist yet the product prepares it in background.
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
    <section data-testid="cv2-customer-lot-selector" className="space-y-4 rounded-xl border border-zinc-800 bg-gradient-to-b from-zinc-900 to-zinc-950/80 p-4 sm:p-5">
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
            <article key={`${lotId}-${idx}`} className="rounded-lg border border-zinc-800 bg-zinc-950/80 p-4 transition-colors hover:border-gold/30">
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

// Selected lot has no report yet: graceful state with back navigation. When a
// job is being prepared in background this view keeps itself updated via the
// shared hook's silent polling.
const CustomerLotPendingBox = ({ preparing, onBackToLots }) => (
  <div data-testid="cv2-customer-lot-pending" className="space-y-3 rounded-xl border border-zinc-800 bg-zinc-900/80 p-5">
    <button
      type="button"
      onClick={onBackToLots}
      data-testid="cv2-customer-back-to-lots"
      className="inline-flex items-center gap-1 text-xs text-zinc-400 hover:text-zinc-200"
    >
      <ArrowLeft className="h-3.5 w-3.5" /> Torna alla scelta del lotto
    </button>
    {preparing ? (
      <div className="flex items-start gap-3">
        <Loader2 className="mt-0.5 h-5 w-5 animate-spin text-gold" />
        <div>
          <p className="text-sm font-medium text-zinc-100">Report del lotto in preparazione</p>
          <p className="mt-1 text-sm leading-6 text-zinc-400">
            Stiamo generando il report per il lotto selezionato: comparirà qui automaticamente appena pronto.
            Può richiedere alcuni minuti.
          </p>
        </div>
      </div>
    ) : (
      <div className="flex items-start gap-3">
        <Info className="mt-0.5 h-5 w-5 text-zinc-500" />
        <p className="text-sm leading-6 text-zinc-400">
          Il report per il lotto selezionato non è ancora disponibile.
        </p>
      </div>
    )}
  </div>
);

// ---------------------------------------------------------------------------
// Report body
// ---------------------------------------------------------------------------
const CustomerReportBody = ({ report, onBackToLots, showBack }) => (
  <article data-testid="cv2-customer-report" className="space-y-8 rounded-xl border border-zinc-800 bg-gradient-to-b from-zinc-900 to-zinc-950/70 p-4 sm:p-6">
    <header className="space-y-3">
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
      <div className="border-b border-zinc-800 pb-4">
        <h2 className="text-2xl font-serif font-bold leading-snug text-zinc-100">{compactText(report?.title, 'Report cliente')}</h2>
        {report?.subtitle && <p className="mt-1.5 text-sm text-zinc-400">{compactText(report.subtitle)}</p>}
      </div>
    </header>

    <CustomerDecisionBox decision={report?.decision} />
    <CustomerPropertySection report={report} />
    <CustomerOccupancySection section={report?.occupancy_section} />
    <CustomerMoneySection money={report?.money_sections} />
    <CustomerCostsSection money={report?.money_sections} />
    <CustomerFormalitiesSection money={report?.money_sections} formalities={report?.formalities_section} />
    <CustomerComplianceSection items={report?.compliance_section} evidence={report?.customer_evidence_index} />
    <CustomerOtherFindings riskSections={report?.risk_sections} complianceItems={report?.compliance_section} />
    <CustomerChecklistSection checklist={report?.buyer_checklist} />
    <CustomerEvidence evidence={report?.customer_evidence_index} />

    {report?.disclaimer && (
      <footer className="rounded-lg border border-zinc-800 bg-zinc-950/70 p-4 text-xs leading-5 text-zinc-500">
        {report.disclaimer}
      </footer>
    )}
  </article>
);

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
  const {
    loading, error, payload, report, preparing,
    isLotSelection, lotUnavailable, selectedLotId, selectLot, backToLots,
  } = state;

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
    if (lotUnavailable) {
      return (
        <div data-testid="cv2-customer-view" className="space-y-4">
          <CustomerLotPendingBox preparing={preparing} onBackToLots={backToLots} />
        </div>
      );
    }
    if (preparing) {
      return (
        <div data-testid="cv2-customer-preparing" className="flex items-start gap-3 rounded-lg border border-gold/25 bg-gold/5 p-4 text-sm">
          <Loader2 className="mt-0.5 h-4 w-4 animate-spin text-gold" />
          <div>
            <p className="font-medium text-zinc-100">Report cliente in preparazione</p>
            <p className="mt-1 leading-6 text-zinc-400">
              L'analisi è in corso: il report comparirà qui automaticamente appena pronto.
            </p>
          </div>
        </div>
      );
    }
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
  CustomerPropertySection,
  CustomerMoneySection,
  CustomerCostsSection,
  CustomerFormalitiesSection,
  CustomerComplianceSection,
  CustomerChecklistSection,
  CustomerEvidence,
  CustomerLotSelector,
  buildEvidencePreview,
  shortExcerpt,
};

export default CustomerReportView;
