import React, { useEffect, useRef, useState } from 'react';
import {
  AlertTriangle,
  Building2,
  Calendar,
  CheckCircle2,
  FileText,
  Info,
  Loader2,
  RefreshCw,
  ShieldCheck,
  X,
} from 'lucide-react';
import { Button } from '../ui/button';
import { compactText } from './shared';
import {
  generateCorrectnessV2Lot,
  getCorrectnessV2LotCreditPreview,
} from '../../lib/api/perizia';

// ---------------------------------------------------------------------------
// Storico lot workspace — the customer-safe lot overview for an analysis.
//
// Every lot card shows the safe display fields from the workspace read and ONE
// dominant action per state (plan §F state machine):
//   REPORT_READY                -> "Apri report" (stored report, zero new job)
//   RUNNING                     -> "Report in preparazione" (poll, no dup start)
//   MONEY_CONFIRMATION_REQUIRED -> "Vedi verifica richiesta" (resume the prompt)
//   VERIFICATION_REQUIRED       -> "Vedi verifica richiesta" (+ explicit retry)
//   FAILED                      -> "Analisi non completata" + explicit retry
//   NOT_ANALYZED                -> "Genera report lotto" (explicit POST)
//
// Generation and rerun ALWAYS go through the explicit confirmation modal that
// renders the backend credit preview verbatim. No credit math happens here —
// every number comes from the server. Opening a lot never creates a job.
// ---------------------------------------------------------------------------

// Restrained status colors consistent with the dark/gold theme:
// green ready / blue preparing / amber confirmation+verification / red failed
// / slate not-analyzed.
const LOT_STATE_META = {
  REPORT_READY: {
    label: 'Report pronto',
    badge: 'border-emerald-500/40 bg-emerald-500/10 text-emerald-300',
    dot: 'bg-emerald-400',
  },
  RUNNING: {
    label: 'Report in preparazione',
    badge: 'border-sky-500/40 bg-sky-500/10 text-sky-300',
    dot: 'bg-sky-400',
  },
  MONEY_CONFIRMATION_REQUIRED: {
    label: 'Conferma richiesta',
    badge: 'border-amber-500/40 bg-amber-500/10 text-amber-200',
    dot: 'bg-amber-400',
  },
  VERIFICATION_REQUIRED: {
    label: 'Verifica richiesta',
    badge: 'border-amber-500/40 bg-amber-500/10 text-amber-200',
    dot: 'bg-amber-400',
  },
  FAILED: {
    label: 'Analisi non completata',
    badge: 'border-red-500/40 bg-red-500/10 text-red-300',
    dot: 'bg-red-400',
  },
  NOT_ANALYZED: {
    label: 'Non analizzato',
    badge: 'border-zinc-700 bg-zinc-800/60 text-zinc-300',
    dot: 'bg-zinc-500',
  },
};

// "6 lotti · 4 pronti · 1 da verificare · 1 non analizzato" — zero categories
// are omitted. Shared by the workspace header (and mirrored in History).
export const buildLotSummaryLine = (summary) => {
  if (!summary || typeof summary !== 'object') return '';
  const count = Number(summary.lot_count) || 0;
  if (count <= 0) return '';
  const parts = [`${count} ${count === 1 ? 'lotto' : 'lotti'}`];
  const push = (value, singular, plural) => {
    const n = Number(value) || 0;
    if (n > 0) parts.push(`${n} ${n === 1 ? singular : plural}`);
  };
  push(summary.ready, 'pronto', 'pronti');
  push(summary.preparing, 'in preparazione', 'in preparazione');
  push(summary.confirmation_required, 'conferma richiesta', 'conferme richieste');
  push(summary.verification_required, 'da verificare', 'da verificare');
  push(summary.failed, 'non completato', 'non completati');
  push(summary.not_analyzed, 'non analizzato', 'non analizzati');
  return parts.join(' · ');
};

// Render the backend credit preview VERBATIM — the numbers/booleans come from
// the server's own billing functions; nothing is computed client-side.
// Today this reads "0 crediti · già incluso nell'analisi".
export const creditPreviewText = (preview) => {
  if (!preview || typeof preview !== 'object') return '';
  const raw = Number(preview.credits_required);
  const amount = Number.isFinite(raw) ? raw : 0;
  const parts = [`${amount} ${amount === 1 ? 'credito' : 'crediti'}`];
  if (preview.already_paid_at_upload) parts.push("già incluso nell'analisi");
  else if (preview.exempt) parts.push('account esente');
  else if (preview.will_consume_credit) parts.push('verrà detratto dai crediti disponibili');
  return parts.join(' · ');
};

const formatReportDate = (value) => {
  if (!value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  return date.toLocaleString('it-IT');
};

const LotStateBadge = ({ state }) => {
  const meta = LOT_STATE_META[state] || LOT_STATE_META.NOT_ANALYZED;
  return (
    <span className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-[11px] font-medium ${meta.badge}`}>
      <span className={`h-1.5 w-1.5 rounded-full ${meta.dot}`} aria-hidden="true" />
      {meta.label}
    </span>
  );
};

// ---------------------------------------------------------------------------
// Explicit generate / rerun confirmation modal.
//
// Shows lot label, current report date/version and the AUTHORITATIVE backend
// credit preview (fetched fresh on open, rendered verbatim). Confirm fires the
// POST exactly once: the button is disabled while in flight so a double-click
// can never create two jobs.
// ---------------------------------------------------------------------------
const LotGenerateModal = ({ analysisId, lot, force, onClose, onStarted }) => {
  const [preview, setPreview] = useState(null);
  const [previewLoading, setPreviewLoading] = useState(true);
  const [previewError, setPreviewError] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState('');
  const submittingRef = useRef(false);

  const lotId = lot?.lot_id;

  useEffect(() => {
    let cancelled = false;
    setPreview(null);
    setPreviewError('');
    setPreviewLoading(true);
    getCorrectnessV2LotCreditPreview(analysisId, lotId)
      .then((response) => {
        if (!cancelled) setPreview(response?.data || null);
      })
      .catch(() => {
        if (!cancelled) setPreviewError('Anteprima crediti non disponibile al momento. Riprova.');
      })
      .finally(() => {
        if (!cancelled) setPreviewLoading(false);
      });
    return () => { cancelled = true; };
  }, [analysisId, lotId]);

  const reportDate = formatReportDate(lot?.latest_report_at);
  const cannotStart = preview ? preview.can_start === false : false;
  const confirmDisabled = submitting || previewLoading || Boolean(previewError) || cannotStart;

  const handleConfirm = () => {
    // Ref guard + disabled button: a double-click can never create two jobs.
    if (submittingRef.current) return;
    submittingRef.current = true;
    setSubmitting(true);
    setSubmitError('');
    generateCorrectnessV2Lot(analysisId, lotId, force)
      .then((response) => {
        onStarted?.(lot, response?.data || {});
      })
      .catch((err) => {
        submittingRef.current = false;
        setSubmitting(false);
        const detail = err?.response?.data?.detail;
        if (err?.response?.status === 409 && detail?.reason_code === 'LOT_FAILED_RERUN_REQUIRED') {
          setSubmitError(
            "L'ultima analisi di questo lotto non è stata completata: è necessaria una rianalisi esplicita tramite \"Riprova analisi\"."
          );
        } else {
          setSubmitError("Impossibile avviare l'analisi del lotto. Riprova.");
        }
      });
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center" data-testid="cv2-lot-generate-modal">
      <div className="absolute inset-0 bg-black/70" onClick={submitting ? undefined : onClose} />
      <div className="relative mx-4 w-full max-w-md rounded-xl border border-zinc-800 bg-zinc-900 p-6 shadow-xl">
        <button
          type="button"
          onClick={onClose}
          disabled={submitting}
          className="absolute right-4 top-4 text-zinc-500 hover:text-zinc-300"
          aria-label="Chiudi"
        >
          <X className="h-5 w-5" />
        </button>

        <div className="mb-4 flex items-center gap-3">
          <div className={`rounded-lg p-2 ${force ? 'bg-amber-500/15' : 'bg-gold/15'}`}>
            {force
              ? <RefreshCw className="h-6 w-6 text-amber-300" />
              : <FileText className="h-6 w-6 text-gold" />}
          </div>
          <h3 className="text-lg font-semibold text-zinc-100">
            {force ? 'Rianalizzare il lotto?' : 'Generare il report del lotto?'}
          </h3>
        </div>

        <p className="text-sm text-zinc-300">{compactText(lot?.label, `Lotto ${compactText(lotId, '')}`)}</p>
        {reportDate && (
          <p className="mt-1 text-xs text-zinc-500">
            Report attuale: {reportDate}
            {lot?.report_version ? ` · versione ${compactText(lot.report_version)}` : ''}
          </p>
        )}
        {force && (
          <p className="mt-3 text-sm leading-6 text-zinc-400">
            Verrà avviata una nuova analisi del lotto. Il report attuale resta disponibile finché la nuova analisi non è completata.
          </p>
        )}

        {/* Credit preview — rendered verbatim from the backend, never computed here. */}
        <div
          data-testid="cv2-lot-credit-preview"
          className="mt-4 rounded-lg border border-zinc-800 bg-zinc-950/70 p-3.5 text-sm"
        >
          {previewLoading ? (
            <span className="inline-flex items-center gap-2 text-zinc-400">
              <Loader2 className="h-4 w-4 animate-spin" /> Verifica crediti in corso...
            </span>
          ) : previewError ? (
            <span className="inline-flex items-start gap-2 text-amber-200">
              <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" /> {previewError}
            </span>
          ) : (
            <>
              <p className="font-medium text-zinc-100">{creditPreviewText(preview)}</p>
              {Number.isFinite(Number(preview?.available_credits)) && (
                <p className="mt-1 text-xs text-zinc-500">
                  Crediti disponibili: {compactText(Number(preview.available_credits))}
                </p>
              )}
              {cannotStart && (
                <p className="mt-2 inline-flex items-start gap-2 text-amber-200">
                  <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
                  Al momento non è possibile avviare l'analisi di questo lotto. Riprova più tardi.
                </p>
              )}
            </>
          )}
        </div>

        {submitError && (
          <div className="mt-3 flex items-start gap-2 rounded-md border border-red-500/30 bg-red-500/5 p-3 text-sm text-red-200">
            <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
            <span>{submitError}</span>
          </div>
        )}

        <div className="mt-6 flex flex-col-reverse gap-3 sm:flex-row sm:justify-end">
          <Button
            type="button"
            variant="outline"
            onClick={onClose}
            disabled={submitting}
            data-testid="cv2-lot-generate-cancel"
            className="border-zinc-700 text-zinc-300 hover:bg-zinc-800"
          >
            Annulla
          </Button>
          <Button
            type="button"
            onClick={handleConfirm}
            disabled={confirmDisabled}
            data-testid="cv2-lot-generate-confirm"
            className="bg-gold text-zinc-950 hover:bg-gold-dim"
          >
            {submitting ? (
              <span className="inline-flex items-center gap-2">
                <Loader2 className="h-4 w-4 animate-spin" /> Avvio in corso...
              </span>
            ) : (
              'Conferma'
            )}
          </Button>
        </div>
      </div>
    </div>
  );
};

// ---------------------------------------------------------------------------
// One lot card: safe display fields + status badge + ONE dominant action.
// ---------------------------------------------------------------------------
const LotCard = ({ lot, onOpenLot, onRequestGenerate, justStarted }) => {
  const lotId = compactText(lot?.lot_id, '');
  const state = justStarted ? 'RUNNING' : (lot?.state || 'NOT_ANALYZED');
  const allowed = Array.isArray(lot?.actions) ? new Set(lot.actions) : null;
  const can = (action) => (allowed ? allowed.has(action) : true);
  const reportDate = formatReportDate(lot?.latest_report_at);
  const priorSafeReport = Boolean(lot?.last_attempt_failed && lot?.has_safe_report);

  const openLot = () => onOpenLot?.(lot?.lot_id);

  return (
    <article
      data-testid={`cv2-lot-card-${lotId}`}
      className="flex flex-col rounded-lg border border-zinc-800 bg-zinc-950/80 p-4 transition-colors hover:border-gold/30"
    >
      <div className="flex flex-wrap items-start justify-between gap-2">
        <h4 className="min-w-0 break-words text-base font-semibold text-zinc-100">
          {compactText(lot?.label, `Lotto ${lotId}`)}
        </h4>
        <LotStateBadge state={state} />
      </div>

      <div className="mt-2 space-y-1 text-sm text-zinc-400">
        {lot?.address && <p className="break-words">Indirizzo: {compactText(lot.address)}</p>}
        {lot?.property_type && <p>Tipo: {compactText(lot.property_type)}</p>}
        {lot?.occupancy_summary && <p>Occupazione: {compactText(lot.occupancy_summary)}</p>}
        {lot?.final_value && (
          <p className="text-zinc-300">
            Valore finale: <span className="font-mono text-gold">{compactText(lot.final_value)}</span>
          </p>
        )}
        {reportDate && (
          <p className="flex items-center gap-1.5 text-xs text-zinc-500">
            <Calendar className="h-3 w-3" />
            Ultimo report: {reportDate}
            {lot?.report_version ? ` · versione ${compactText(lot.report_version)}` : ''}
          </p>
        )}
      </div>

      {/* Prior safe report survives a failed rerun: never replaced by an error. */}
      {priorSafeReport && (
        <div
          data-testid={`cv2-lot-prior-safe-${lotId}`}
          className="mt-3 rounded-md border border-amber-500/25 bg-amber-500/5 p-3 text-sm"
        >
          <p className="flex items-start gap-2 text-amber-200">
            <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
            L'ultimo tentativo non è stato completato.
          </p>
          <button
            type="button"
            data-testid={`cv2-lot-open-safe-${lotId}`}
            onClick={openLot}
            className="mt-2 inline-flex items-center gap-1.5 text-sm font-medium text-emerald-300 hover:text-emerald-200"
          >
            <ShieldCheck className="h-4 w-4" />
            Ultimo report verificato disponibile
          </button>
        </div>
      )}

      <div className="mt-4 flex flex-wrap items-center gap-3 pt-1">
        {state === 'REPORT_READY' && (
          <>
            <Button
              type="button"
              data-testid={`cv2-lot-open-${lotId}`}
              onClick={openLot}
              className="bg-gold text-zinc-950 hover:bg-gold-dim"
            >
              Apri report
            </Button>
            {can('rerun') && (
              <button
                type="button"
                data-testid={`cv2-lot-rerun-${lotId}`}
                onClick={() => onRequestGenerate?.(lot, true)}
                className="inline-flex items-center gap-1.5 text-xs text-zinc-500 transition-colors hover:text-zinc-300"
              >
                <RefreshCw className="h-3.5 w-3.5" />
                Rianalizza lotto
              </button>
            )}
          </>
        )}

        {state === 'RUNNING' && (
          <span
            data-testid={`cv2-lot-running-${lotId}`}
            className="inline-flex items-center gap-2 text-sm text-sky-300"
          >
            <Loader2 className="h-4 w-4 animate-spin" />
            Report in preparazione
          </span>
        )}

        {(state === 'MONEY_CONFIRMATION_REQUIRED' || state === 'VERIFICATION_REQUIRED') && (
          <>
            <Button
              type="button"
              data-testid={`cv2-lot-open-${lotId}`}
              onClick={openLot}
              variant="outline"
              className="border-amber-500/40 text-amber-200 hover:bg-amber-500/10"
            >
              Vedi verifica richiesta
            </Button>
            {state === 'VERIFICATION_REQUIRED' && can('rerun') && (
              <button
                type="button"
                data-testid={`cv2-lot-rerun-${lotId}`}
                onClick={() => onRequestGenerate?.(lot, true)}
                className="inline-flex items-center gap-1.5 text-xs text-zinc-500 transition-colors hover:text-zinc-300"
              >
                <RefreshCw className="h-3.5 w-3.5" />
                Riprova analisi
              </button>
            )}
          </>
        )}

        {state === 'FAILED' && (
          <div className="w-full space-y-2">
            {!priorSafeReport && (
              <p className="flex items-start gap-2 text-sm text-red-300">
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
                Analisi non completata.
              </p>
            )}
            {can('rerun') && (
              <Button
                type="button"
                data-testid={`cv2-lot-rerun-${lotId}`}
                onClick={() => onRequestGenerate?.(lot, true)}
                variant="outline"
                className="border-red-500/30 text-red-300 hover:border-red-500/50 hover:bg-red-500/10"
              >
                <RefreshCw className="mr-2 h-4 w-4" />
                Riprova analisi
              </Button>
            )}
          </div>
        )}

        {state === 'NOT_ANALYZED' && can('generate') && (
          <Button
            type="button"
            data-testid={`cv2-lot-generate-${lotId}`}
            onClick={() => onRequestGenerate?.(lot, false)}
            className="bg-gold text-zinc-950 hover:bg-gold-dim"
          >
            Genera report lotto
          </Button>
        )}
      </div>
    </article>
  );
};

// ---------------------------------------------------------------------------
// The workspace overview: header + summary line + lot cards grid.
// ---------------------------------------------------------------------------
const LotWorkspace = ({ analysisId, state, onOpenLot }) => {
  const { workspace, loading, refresh, reload } = state || {};
  // { lot, force } while the confirmation modal is open.
  const [modal, setModal] = useState(null);
  // Instant "in preparazione" feedback for a lot whose generate just started,
  // until the silent workspace refresh reflects the RUNNING state.
  const [startedLots, setStartedLots] = useState(() => new Set());

  const lots = Array.isArray(workspace?.lots) ? workspace.lots : [];
  const summaryLine = buildLotSummaryLine(workspace?.summary);

  const handleStarted = (lot) => {
    setModal(null);
    const lotId = lot?.lot_id;
    if (lotId) {
      setStartedLots((prev) => {
        const next = new Set(prev);
        next.add(lotId);
        return next;
      });
    }
    Promise.resolve(refresh?.()).finally(() => {
      if (lotId) {
        setStartedLots((prev) => {
          if (!prev.has(lotId)) return prev;
          const next = new Set(prev);
          next.delete(lotId);
          return next;
        });
      }
    });
  };

  if (!workspace) {
    if (loading) {
      return (
        <div className="flex items-center gap-2 text-sm text-zinc-400">
          <Loader2 className="h-4 w-4 animate-spin" /> Caricamento lotti...
        </div>
      );
    }
    return (
      <div className="flex items-start gap-3 rounded-lg border border-zinc-800 bg-zinc-950 p-4 text-sm text-zinc-400">
        <Info className="mt-0.5 h-4 w-4 text-zinc-500" />
        <div>
          <p>L'elenco dei lotti non è al momento disponibile.</p>
          {typeof reload === 'function' && (
            <button
              type="button"
              onClick={reload}
              className="mt-2 text-xs text-gold hover:underline"
            >
              Riprova
            </button>
          )}
        </div>
      </div>
    );
  }

  return (
    <section
      data-testid="cv2-lot-workspace"
      className="space-y-4 rounded-xl border border-zinc-800 bg-gradient-to-b from-zinc-900 to-zinc-950/80 p-4 sm:p-5"
    >
      <header className="flex flex-col gap-2">
        <div className="flex items-start gap-3">
          <span className="mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-lg border border-zinc-700/60 bg-zinc-800/70">
            <Building2 className="h-4 w-4 text-gold" />
          </span>
          <div className="min-w-0">
            <h3 className="text-lg font-semibold text-zinc-100">Lotti della perizia</h3>
            <p className="mt-0.5 text-sm text-zinc-500">
              I report già generati si aprono subito, senza nuove elaborazioni.
              L'analisi di un lotto parte solo su tua richiesta esplicita.
            </p>
          </div>
        </div>
        {summaryLine && (
          <p
            data-testid="cv2-lot-summary"
            className="flex items-center gap-2 pl-11 text-sm text-zinc-300"
          >
            <CheckCircle2 className="h-3.5 w-3.5 text-gold" />
            {summaryLine}
          </p>
        )}
      </header>

      <div className="grid grid-cols-1 gap-3 xl:grid-cols-2">
        {lots.map((lot, idx) => (
          <LotCard
            key={`${lot?.lot_id || idx}`}
            lot={lot}
            onOpenLot={onOpenLot}
            onRequestGenerate={(targetLot, force) => setModal({ lot: targetLot, force })}
            justStarted={startedLots.has(lot?.lot_id)}
          />
        ))}
      </div>

      {modal && (
        <LotGenerateModal
          analysisId={analysisId}
          lot={modal.lot}
          force={modal.force}
          onClose={() => setModal(null)}
          onStarted={handleStarted}
        />
      )}
    </section>
  );
};

export { LotStateBadge, LotCard, LotGenerateModal };
export default LotWorkspace;
