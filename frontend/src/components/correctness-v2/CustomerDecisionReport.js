import React, { useMemo, useState } from 'react';
import {
  Building2, CheckCircle2, ClipboardList, Coins, FileText, Info,
  KeyRound, Quote, ScrollText, ShieldCheck,
} from 'lucide-react';
import { pagesText, StatusChip } from './shared';
import { ConfirmationDialog } from './ConfirmationDialog';

// ---------------------------------------------------------------------------
// Customer DECISION report — renders ONLY from `report.decision_model`.
//
// The backend builds every legal / technical / money-role / severity
// conclusion (non-negotiable rule 1); this component draws data-driven cards and
// never infers meaning from raw arrays. Sections absent from the payload render
// nothing (no empty "0 beni" cards).
//
// Order (§Part 3): Esito -> Cosa stai acquistando -> Numeri principali ->
// Occupazione -> Cosa verificare -> Conformità -> Formalità -> Altri elementi ->
// Fonti decisive -> Conferme utente -> Stato delle verifiche.
// ---------------------------------------------------------------------------

const ESITO_TONE = { verde: 'verde', ambra: 'ambra', rosso: 'rosso' };
const ESITO_ACCENT = {
  verde: 'border-emerald-500/30 bg-emerald-500/5',
  ambra: 'border-amber-400/30 bg-amber-500/5',
  rosso: 'border-red-500/30 bg-red-500/5',
};

const SectionShell = ({ icon: Icon, title, children, testId }) => (
  <section data-testid={testId} className="space-y-3">
    <h3 className="flex items-center gap-2 text-sm font-semibold uppercase tracking-wide text-zinc-400">
      {Icon ? <Icon className="h-4 w-4 text-zinc-500" /> : null}
      {title}
    </h3>
    {children}
  </section>
);

const Pages = ({ pages, page }) => {
  const text = page ? `p. ${page}` : pagesText(pages);
  if (!text) return null;
  return <p className="mt-1 text-xs text-gold">{text}</p>;
};

// --- §1 Esito operativo -----------------------------------------------------
const EsitoOperativoCard = ({ esito }) => {
  if (!esito) return null;
  const accent = ESITO_ACCENT[esito.level] || ESITO_ACCENT.ambra;
  return (
    <section data-testid="cv2-esito" className={`rounded-xl border p-4 sm:p-5 ${accent}`}>
      <StatusChip tone={ESITO_TONE[esito.level] || 'ambra'} testId="cv2-esito-chip">
        {esito.level === 'verde' ? 'Nessun elemento bloccante' : esito.level === 'rosso' ? 'Verifica tecnica' : 'Verifiche necessarie'}
      </StatusChip>
      <h2 className="mt-2 text-xl font-serif font-bold leading-snug text-zinc-100">{esito.headline}</h2>
      {esito.sentence && <p className="mt-1.5 text-sm leading-6 text-zinc-300">{esito.sentence}</p>}
      {Array.isArray(esito.drivers) && esito.drivers.length > 0 && (
        <ul data-testid="cv2-esito-drivers" className="mt-3 space-y-1.5">
          {esito.drivers.map((d) => (
            <li key={d.finding_id} className="flex items-center gap-2 text-sm text-zinc-300">
              <span className="h-1.5 w-1.5 rounded-full bg-zinc-500" />
              {d.title}
            </li>
          ))}
        </ul>
      )}
    </section>
  );
};

// --- §2 Cosa stai acquistando ----------------------------------------------
const IdentityRow = ({ label, value }) => {
  if (!value) return null;
  return (
    <div className="flex flex-col gap-0.5 sm:flex-row sm:gap-3">
      <span className="w-40 shrink-0 text-xs uppercase tracking-wide text-zinc-500">{label}</span>
      <span className="text-sm text-zinc-200">{value}</span>
    </div>
  );
};

const AcquistoSection = ({ acquisto }) => {
  if (!acquisto) return null;
  const id = acquisto.identity || {};
  return (
    <SectionShell icon={Building2} title="Cosa stai acquistando" testId="cv2-acquisto">
      <div className="space-y-2 rounded-lg border border-zinc-800 bg-zinc-950/70 p-4">
        <IdentityRow label="Tribunale" value={id.tribunale} />
        <IdentityRow label="Procedura/RGE" value={id.procedura_rge} />
        <IdentityRow label="Lotto" value={id.lotto} />
        {id.lotto_selezionato && <IdentityRow label="Lotto selezionato" value={id.lotto_selezionato} />}
        <IdentityRow label="Indirizzo" value={id.indirizzo} />
        <IdentityRow label="Tipologia" value={id.tipologia} />
        <IdentityRow label="Diritto/quota" value={id.diritto_quota} />
        {acquisto.occupazione_sintesi && <IdentityRow label="Occupazione" value={acquisto.occupazione_sintesi} />}
        <Pages pages={id.pagine} />
      </div>
      {Array.isArray(acquisto.beni) && acquisto.beni.length > 0 && (
        <div className="space-y-2" data-testid="cv2-beni">
          {acquisto.beni.map((b, i) => (
            <div key={`${b.titolo}-${i}`} className="rounded-lg border border-zinc-800 bg-zinc-950/50 p-3">
              <p className="text-sm font-medium text-zinc-100">{b.titolo}</p>
              {Array.isArray(b.pertinenze) && b.pertinenze.length > 0 && (
                <ul className="mt-1.5 space-y-1">
                  {b.pertinenze.map((p, j) => (
                    <li key={`${p.label}-${j}`} className="text-xs text-zinc-400">• {p.label}{p.nota ? ` — ${p.nota}` : ''}</li>
                  ))}
                </ul>
              )}
              <Pages pages={b.pagine} />
            </div>
          ))}
        </div>
      )}
    </SectionShell>
  );
};

// --- §3 Numeri principali ---------------------------------------------------
const NumeriPrincipali = ({ numeri, moneyFindings, confirmSlot }) => {
  if (!numeri) return null;
  const catena = numeri.catena || [];
  return (
    <SectionShell icon={Coins} title="Numeri principali" testId="cv2-numeri">
      <div className="rounded-lg border border-zinc-800 bg-zinc-950/70 p-4" data-testid="cv2-catena">
        {catena.map((row, i) => (
          <div
            key={`${row.label}-${i}`}
            className={`flex items-center justify-between gap-3 py-1.5 ${row.terminal ? 'mt-1 border-t border-zinc-800 pt-2.5' : ''}`}
          >
            <span className={`text-sm ${row.kind === 'deduction' ? 'text-zinc-400' : 'text-zinc-200'}`}>
              {row.kind === 'deduction' ? '− ' : ''}{row.label}
            </span>
            <span className={`text-sm tabular-nums ${row.terminal ? 'font-semibold text-gold' : row.kind === 'deduction' ? 'text-zinc-400' : 'text-zinc-100'}`}>
              {row.amount_display}
            </span>
          </div>
        ))}
      </div>

      {Array.isArray(numeri.costi_potenziali) && numeri.costi_potenziali.length > 0 && (
        <div className="space-y-2" data-testid="cv2-costi">
          <p className="text-xs uppercase tracking-wide text-zinc-500">Costi potenzialmente a carico dell'acquirente</p>
          {numeri.costi_potenziali.map((c, i) => (
            <div key={`${c.label}-${i}`} className="rounded-lg border border-zinc-800 bg-zinc-950/50 p-3">
              <div className="flex items-center justify-between gap-3">
                <span className="text-sm text-zinc-200">{c.label}</span>
                <span className="text-sm tabular-nums text-zinc-300">{c.amount_display}</span>
              </div>
              {c.included_in_valuation && (
                <p className="mt-1 text-xs text-zinc-500" data-testid="cv2-included-note">{c.nota}</p>
              )}
            </div>
          ))}
        </div>
      )}

      {Array.isArray(numeri.scenari) && numeri.scenari.length > 0 && (
        <div className="rounded-lg border border-sky-500/20 bg-sky-500/5 p-3" data-testid="cv2-scenari">
          <p className="text-xs uppercase tracking-wide text-sky-300">Scenari alternativi indicati dalla perizia</p>
          {numeri.scenari.map((s, i) => (
            <div key={i} className="mt-1.5 flex items-center justify-between gap-3 text-sm text-zinc-300">
              <span>{s.label}</span><span className="tabular-nums">{s.amount_display}</span>
            </div>
          ))}
        </div>
      )}

      {((Array.isArray(moneyFindings) && moneyFindings.length > 0)
        || (Array.isArray(numeri.da_chiarire) && numeri.da_chiarire.length > 0)) && (
        <div className="space-y-2" data-testid="cv2-da-chiarire">
          <p className="text-xs uppercase tracking-wide text-zinc-500">Importi da chiarire</p>
          {(moneyFindings || []).map((f) => (
            <div key={f.finding_id} className="rounded-lg border border-zinc-800 bg-zinc-950/50 p-3">
              <div className="flex items-center justify-between gap-3">
                <span className="text-sm text-zinc-200">{f.title}</span>
                <span className="text-sm tabular-nums text-zinc-300">{f.amount_display}</span>
              </div>
              {f.customer_summary && <p className="mt-1 text-xs text-zinc-500">{f.customer_summary}</p>}
              <div className="mt-2">{confirmSlot(f)}</div>
            </div>
          ))}
          {(numeri.da_chiarire || []).map((r, i) => (
            <div key={`dc-${i}`} className="rounded-lg border border-zinc-800 bg-zinc-950/50 p-3">
              <div className="flex items-center justify-between gap-3">
                <span className="text-sm text-zinc-200">{r.label}</span>
                <span className="text-sm tabular-nums text-zinc-300">{r.amount_display}</span>
              </div>
              {r.motivo && <p className="mt-1 text-xs text-zinc-500">{r.motivo}</p>}
            </div>
          ))}
        </div>
      )}

      {numeri.auction && (
        <div className="rounded-lg border border-zinc-800 bg-zinc-950/50 p-3" data-testid="cv2-auction">
          <div className="flex items-center justify-between gap-3">
            <span className="text-sm text-zinc-200">{numeri.auction.label || "Prezzo base d'asta"}</span>
            <span className="text-sm tabular-nums text-zinc-100">{numeri.auction.amount_display}</span>
          </div>
          {numeri.auction.nota && <p className="mt-1 text-xs text-zinc-500">{numeri.auction.nota}</p>}
        </div>
      )}

      {numeri.comparatives_summary && (
        <p className="text-xs text-zinc-500" data-testid="cv2-comparatives">
          Metodo di stima basato su comparativi e valori di mercato indicati nella perizia
          {numeri.comparatives_summary.pages?.length ? ` (${pagesText(numeri.comparatives_summary.pages)})` : ''}.
        </p>
      )}
    </SectionShell>
  );
};

// --- §4 Occupazione ---------------------------------------------------------
const OccupazioneSection = ({ occupazione, finding, confirmSlot }) => {
  if (!occupazione) return null;
  return (
    <SectionShell icon={KeyRound} title="Stato di occupazione" testId="cv2-occupazione">
      <div className="space-y-2 rounded-lg border border-zinc-800 bg-zinc-950/70 p-4">
        <div>
          <p className="text-xs uppercase tracking-wide text-zinc-500">Stato</p>
          <p className="text-sm text-zinc-100">{occupazione.stato}</p>
        </div>
        {occupazione.dettaglio && <p className="text-sm leading-6 text-zinc-300">{occupazione.dettaglio}</p>}
        {occupazione.perche_conta && (
          <div>
            <p className="text-xs uppercase tracking-wide text-zinc-500">Perché conta</p>
            <p className="text-sm text-zinc-300">{occupazione.perche_conta}</p>
          </div>
        )}
        {Array.isArray(occupazione.cosa_verificare) && occupazione.cosa_verificare.length > 0 && (
          <div>
            <p className="text-xs uppercase tracking-wide text-zinc-500">Cosa verificare</p>
            <ul className="mt-1 space-y-1">
              {occupazione.cosa_verificare.map((c, i) => (
                <li key={i} className="text-sm text-zinc-300">• {c}</li>
              ))}
            </ul>
          </div>
        )}
        <Pages pages={occupazione.pagine} page={occupazione.pagina} />
        {finding ? <div className="pt-1">{confirmSlot(finding)}</div> : null}
      </div>
    </SectionShell>
  );
};

// --- §5 Cosa verificare -----------------------------------------------------
const CHECK_STATUS_LABEL = {
  da_verificare: 'Da verificare',
  conferma_necessaria: 'Conferma necessaria',
  verifica_tecnica_richiesta: 'Verifica tecnica richiesta',
  completato: 'Completato',
  confermato_utente: "Confermato dall'utente",
};

const CHECK_STATUS_TONE = {
  da_verificare: 'ambra',
  conferma_necessaria: 'ambra',
  verifica_tecnica_richiesta: 'ambra',
  confermato_utente: 'verde',
  completato: 'verde',
};

const VerificheSection = ({ verifiche }) => {
  if (!verifiche || !Array.isArray(verifiche.items) || !verifiche.items.length) return null;
  return (
    <SectionShell icon={ClipboardList} title="Cosa verificare prima di procedere" testId="cv2-verifiche">
      <ul className="space-y-2">
        {verifiche.items.map((it, i) => (
          <li key={it.finding_id || i} data-finding={it.finding_id} className="rounded-lg border border-zinc-800 bg-zinc-950/70 p-3">
            <div className="flex items-start justify-between gap-3">
              <p className="text-sm font-medium text-zinc-100">{it.title}</p>
              <StatusChip tone={CHECK_STATUS_TONE[it.status] || 'ambra'}>
                {it.status_label || CHECK_STATUS_LABEL[it.status] || 'Da verificare'}
              </StatusChip>
            </div>
            {it.why && <p className="mt-1 text-xs text-zinc-400">{it.why}</p>}
            <Pages page={it.page} />
          </li>
        ))}
      </ul>
    </SectionShell>
  );
};

// --- §6 Conformità ----------------------------------------------------------
const CONFORMITY_TONE = { conforme: 'verde', regolarizzabile: 'ambra', non_conforme: 'ambra', non_verificato: 'slate', confermato_utente: 'verde', verifica_tecnica_richiesta: 'ambra' };

const ConformitaSection = ({ conformita, findingsById, confirmSlot }) => {
  if (!conformita || !Array.isArray(conformita.groups) || !conformita.groups.length) return null;
  return (
    <SectionShell icon={ShieldCheck} title="Conformità e documenti tecnici" testId="cv2-conformita">
      <div className="space-y-2">
        {conformita.groups.map((g) => (
          <div key={g.group}>
            {g.items.map((fid) => {
              const f = findingsById[fid];
              if (!f) return null;
              return (
                <div key={fid} className="rounded-lg border border-zinc-800 bg-zinc-950/70 p-3">
                  <div className="flex items-start justify-between gap-3">
                    <p className="text-sm font-medium text-zinc-100">{f.title}</p>
                    <StatusChip tone={CONFORMITY_TONE[f.status] || 'slate'}>{f.status_label}</StatusChip>
                  </div>
                  {f.customer_summary && <p className="mt-1 text-xs leading-5 text-zinc-400">{f.customer_summary}</p>}
                  <div className="mt-1.5 flex flex-wrap gap-x-4 gap-y-1 text-xs text-zinc-500">
                    {f.amount_display && <span>Costo: {f.amount_display}</span>}
                    {f.timing && <span>Tempistica: {f.timing}</span>}
                  </div>
                  {f.evidence?.excerpt && (
                    <div className="mt-2 flex items-start gap-2 rounded border border-zinc-800 bg-zinc-900/50 p-2">
                      <Quote className="mt-0.5 h-3 w-3 shrink-0 text-zinc-600" />
                      <p className="text-xs italic text-zinc-400">{f.evidence.excerpt}</p>
                    </div>
                  )}
                  <Pages page={f.page} pages={f.pages} />
                  <div className="mt-2">{confirmSlot(f)}</div>
                </div>
              );
            })}
          </div>
        ))}
      </div>
    </SectionShell>
  );
};

// --- §7 Formalità -----------------------------------------------------------
const FormalitaCard = ({ card, tone }) => (
  <div className="rounded-lg border border-zinc-800 bg-zinc-950/70 p-3">
    <div className="flex items-center justify-between gap-3">
      <p className="text-sm font-medium text-zinc-100">{card.type_label}</p>
      {tone && <StatusChip tone={tone}>{tone === 'verde' ? 'Cancellata dalla procedura' : 'Da verificare'}</StatusChip>}
    </div>
    {card.statement && <p className="mt-1 text-xs text-zinc-300">{card.statement}</p>}
    {card.note && <p className="mt-1 text-xs text-zinc-500">{card.note}</p>}
    {card.amount_display && (
      <details className="mt-1.5">
        <summary className="cursor-pointer text-xs text-zinc-500">Importo iscritto</summary>
        <p className="mt-1 text-xs text-zinc-400">{card.amount_display}{card.amount_note ? ` — ${card.amount_note}` : ''}</p>
      </details>
    )}
    <Pages pages={card.pages} />
  </div>
);

const FormalitaSection = ({ formalita }) => {
  if (!formalita) return null;
  const { cancellate, costi_cancellazione, da_verificare } = formalita;
  return (
    <SectionShell icon={ScrollText} title="Formalità e cancellazioni" testId="cv2-formalita">
      {Array.isArray(cancellate) && cancellate.map((c, i) => <FormalitaCard key={`can-${i}`} card={c} tone="verde" />)}
      {Array.isArray(costi_cancellazione) && costi_cancellazione.map((c, i) => <FormalitaCard key={`cost-${i}`} card={c} tone="ambra" />)}
      {Array.isArray(da_verificare) && da_verificare.map((c, i) => <FormalitaCard key={`dv-${i}`} card={c} tone="ambra" />)}
    </SectionShell>
  );
};

// --- §8 Altri elementi ------------------------------------------------------
const AltriSection = ({ altri }) => {
  if (!altri || !Array.isArray(altri.items) || !altri.items.length) return null;
  return (
    <SectionShell icon={Info} title="Altri elementi da conoscere" testId="cv2-altri">
      <ul className="space-y-2">
        {altri.items.map((it, i) => (
          <li key={i} className="rounded-lg border border-zinc-800 bg-zinc-950/70 p-3">
            <p className="text-sm font-medium text-zinc-100">{it.title}</p>
            <p className="mt-1 text-xs text-zinc-400">{it.summary}</p>
            <Pages pages={it.pages} />
          </li>
        ))}
      </ul>
    </SectionShell>
  );
};

// --- §9 Fonti decisive ------------------------------------------------------
const FontiDecisiveSection = ({ fonti }) => {
  const [showAll, setShowAll] = useState(false);
  if (!fonti || !Array.isArray(fonti.primary) || !fonti.primary.length) return null;
  const extra = Math.max(0, (fonti.all_count || fonti.primary.length) - fonti.primary.length);
  return (
    <SectionShell icon={FileText} title="Fonti decisive dalla perizia" testId="cv2-fonti">
      <ul className="space-y-2">
        {fonti.primary.map((s) => (
          <li key={s.source_id} className="rounded-lg border border-zinc-800 bg-zinc-950/70 p-3">
            <p className="text-sm font-medium text-zinc-100">
              {s.page ? `p. ${s.page} — ` : ''}{s.title}
            </p>
            {s.excerpt ? (
              <div className="mt-1.5 flex items-start gap-2">
                <Quote className="mt-0.5 h-3 w-3 shrink-0 text-zinc-600" />
                <p className="text-xs italic leading-5 text-zinc-400">{s.excerpt}</p>
              </div>
            ) : (
              <p className="mt-1 text-xs text-zinc-500">Estratto da verificare.</p>
            )}
          </li>
        ))}
      </ul>
      {extra > 0 && !showAll && (
        <button
          type="button"
          data-testid="cv2-fonti-more"
          onClick={() => setShowAll(true)}
          className="text-xs text-gold hover:underline"
        >
          Mostra tutte le fonti ({fonti.all_count})
        </button>
      )}
    </SectionShell>
  );
};

// --- §10 Conferme utente ----------------------------------------------------
const ConfermeSection = ({ conferme }) => {
  if (!conferme || !Array.isArray(conferme.items) || !conferme.items.length) return null;
  return (
    <SectionShell icon={CheckCircle2} title="Conferme fornite dall'utente" testId="cv2-conferme">
      <ul className="space-y-2">
        {conferme.items.map((c) => {
          const chip = c.stale
            ? { tone: 'ambra', label: 'Da rivedere' }
            : c.status === 'non_sicuro'
              ? { tone: 'ambra', label: 'Non sono sicuro' }
              : { tone: 'verde', label: "Confermato dall'utente" };
          return (
          <li key={c.finding_id} className="rounded-lg border border-zinc-800 bg-zinc-950/70 p-3">
            <div className="flex items-center justify-between gap-3">
              <p className="text-sm text-zinc-100">{c.title}</p>
              <StatusChip tone={chip.tone}>{chip.label}</StatusChip>
            </div>
            {c.selected_label && <p className="mt-1 text-xs text-zinc-300">{c.selected_label}</p>}
            <p className="mt-1 text-xs text-zinc-500">{c.wording}</p>
          </li>
          );
        })}
      </ul>
    </SectionShell>
  );
};

// --- §11 Stato delle verifiche ---------------------------------------------
const StatoVerificheSection = ({ stato }) => {
  if (!stato) return null;
  return (
    <SectionShell icon={CheckCircle2} title="Stato delle verifiche" testId="cv2-stato-verifiche">
      <div className="flex flex-wrap items-center gap-3 rounded-lg border border-zinc-800 bg-zinc-950/70 p-4">
        <StatusChip tone="slate">{stato.label}</StatusChip>
        <span className="text-sm text-zinc-400">
          {stato.confirmations_done} verifiche completate
          {stato.confirmations_total > stato.confirmations_done
            ? ` · ${stato.confirmations_total - stato.confirmations_done} ancora aperte`
            : ''}
        </span>
      </div>
    </SectionShell>
  );
};

// --- Root -------------------------------------------------------------------
const CustomerDecisionReport = ({
  report, onSubmitConfirmation, confirmingFinding = false, findingConfirmError = '',
}) => {
  const model = report?.decision_model;
  const [openConfirmId, setOpenConfirmId] = useState(null);
  const findingsById = useMemo(() => {
    const map = {};
    for (const f of model?.findings || []) map[f.finding_id] = f;
    return map;
  }, [model]);

  if (!model) return null;
  const sections = model.sections || {};

  const confirmSlot = (finding) => {
    if (!finding) return null;
    if (finding.user_confirmed || finding.status === 'confermato_utente') {
      return <StatusChip tone="verde">Confermato dall'utente</StatusChip>;
    }
    if (finding.confirmation?.eligible) {
      const open = openConfirmId === finding.finding_id;
      return (
        <div>
          <button
            type="button"
            data-testid={`cv2-confirm-open-${finding.finding_id}`}
            onClick={() => setOpenConfirmId(open ? null : finding.finding_id)}
          >
            <StatusChip tone="conferma">Conferma necessaria</StatusChip>
          </button>
          {open && (
            <div className="mt-2">
              <ConfirmationDialog
                finding={finding}
                submitting={confirmingFinding}
                error={findingConfirmError}
                onSubmit={(fid, optId) => {
                  if (onSubmitConfirmation) {
                    Promise.resolve(onSubmitConfirmation(fid, optId)).then((ok) => {
                      if (ok) setOpenConfirmId(null);
                    });
                  }
                }}
                onClose={() => setOpenConfirmId(null)}
              />
            </div>
          )}
        </div>
      );
    }
    if (finding.professional_check) {
      return <p className="text-xs text-zinc-500">{finding.professional_check}</p>;
    }
    return null;
  };

  const occFinding = (model.findings || []).find((f) => f.section === 'occupazione');
  const moneyFindings = (model.findings || []).filter((f) => f.confirm_class === 'money_role');

  return (
    <div data-testid="cv2-decision-report" className="space-y-8">
      <EsitoOperativoCard esito={model.esito} />
      <AcquistoSection acquisto={sections.acquisto} />
      <NumeriPrincipali numeri={sections.numeri} moneyFindings={moneyFindings} confirmSlot={confirmSlot} />
      <OccupazioneSection occupazione={sections.occupazione} finding={occFinding} confirmSlot={confirmSlot} />
      <VerificheSection verifiche={sections.verifiche} />
      <ConformitaSection conformita={sections.conformita} findingsById={findingsById} confirmSlot={confirmSlot} />
      <FormalitaSection formalita={sections.formalita} />
      <AltriSection altri={sections.altri} />
      <FontiDecisiveSection fonti={sections.fonti} />
      <ConfermeSection conferme={sections.conferme} />
      <StatoVerificheSection stato={sections.stato_verifiche} />
    </div>
  );
};

export default CustomerDecisionReport;
export {
  CustomerDecisionReport, EsitoOperativoCard, AcquistoSection, NumeriPrincipali,
  OccupazioneSection, VerificheSection, ConformitaSection, FormalitaSection,
  AltriSection, FontiDecisiveSection, ConfermeSection, StatoVerificheSection,
};
