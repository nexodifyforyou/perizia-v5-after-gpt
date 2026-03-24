import React, { useState, useEffect, useRef } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { Sidebar } from './Dashboard';
import { Button } from '../components/ui/button';
import {
  CreditCard,
  CheckCircle2,
  Loader2,
  AlertCircle,
  Crown
} from 'lucide-react';
import axios from 'axios';
import { toast } from 'sonner';

const API_URL = process.env.REACT_APP_BACKEND_URL;
const LEDGER_PAGE_SIZE = 10;
const ACTIVE_CHECKOUT_STORAGE_KEY = 'periziascan.active_checkout_session';
const INTERNAL_PLAN_DETAILS = {
  name_it: 'Enterprise',
  plan_type_label_it: 'Interno',
  support_level_it: 'Supporto dedicato',
};
const QUOTA_LABELS = {
  perizia_scans_remaining: 'Crediti perizia',
  image_scans_remaining: 'Crediti immagini',
  assistant_messages_remaining: 'Messaggi assistente',
};
const ENTRY_TYPE_LABELS = {
  opening_balance: 'Saldo iniziale',
  admin_adjustment: 'Variazione admin',
  plan_purchase: 'Accredito piano',
  perizia_upload: 'Analisi perizia',
  image_forensics: 'Analisi immagini',
  assistant_message: 'Messaggio assistente',
  system_correction: 'Correzione sistema',
};
const REFERENCE_TYPE_LABELS = {
  analysis: 'Analisi',
  forensics: 'Analisi immagini',
  assistant_qa: 'Sessione assistente',
  checkout_session: 'Checkout',
  admin_user_update: 'Aggiornamento admin',
  system: 'Sistema',
  legacy_helper: 'Sistema',
};

const formatLedgerDate = (value) => {
  if (!value) return 'Data non disponibile';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return 'Data non disponibile';
  return new Intl.DateTimeFormat('it-IT', {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(parsed);
};

const formatQuotaLabel = (field) => QUOTA_LABELS[field] || 'Credito';
const formatEntryTypeLabel = (type) => ENTRY_TYPE_LABELS[type] || 'Movimento';
const formatReferenceLabel = (type) => REFERENCE_TYPE_LABELS[type] || null;

const readTrackedCheckoutSession = () => {
  try {
    const raw = window.sessionStorage.getItem(ACTIVE_CHECKOUT_STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch (_error) {
    return null;
  }
};

const writeTrackedCheckoutSession = (payload) => {
  try {
    window.sessionStorage.setItem(ACTIVE_CHECKOUT_STORAGE_KEY, JSON.stringify(payload));
  } catch (_error) {
    // Ignore sessionStorage errors.
  }
};

const clearTrackedCheckoutSession = () => {
  try {
    window.sessionStorage.removeItem(ACTIVE_CHECKOUT_STORAGE_KEY);
  } catch (_error) {
    // Ignore sessionStorage errors.
  }
};

const FEEDBACK_STYLES = {
  success: {
    wrapper: 'border-emerald-500/30 bg-emerald-500/10',
    icon: 'text-emerald-300',
    title: 'text-zinc-100',
    body: 'text-emerald-100/80',
  },
  error: {
    wrapper: 'border-red-500/30 bg-red-500/10',
    icon: 'text-red-300',
    title: 'text-zinc-100',
    body: 'text-red-100/80',
  },
  info: {
    wrapper: 'border-zinc-700 bg-zinc-900/70',
    icon: 'text-zinc-300',
    title: 'text-zinc-100',
    body: 'text-zinc-400',
  },
  warning: {
    wrapper: 'border-amber-500/30 bg-amber-500/10',
    icon: 'text-amber-200',
    title: 'text-zinc-100',
    body: 'text-amber-100/80',
  },
};

const LedgerRow = ({ entry }) => {
  const isCredit = entry.direction === 'credit';
  const directionLabel = isCredit ? '+' : '-';
  const directionClasses = isCredit
    ? 'text-emerald-300 bg-emerald-500/10 border-emerald-500/20'
    : 'text-amber-300 bg-amber-500/10 border-amber-500/20';
  const pagesCount = entry.metadata?.pages_count;
  const referenceLabel = formatReferenceLabel(entry.reference_type);
  const showReferenceId = entry.reference_id && entry.reference_id !== 'n/a';

  return (
    <div className="rounded-2xl border border-zinc-800 bg-zinc-950/70 p-4 md:p-5">
      <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <p className="text-sm font-semibold text-zinc-100">{entry.description_it || 'Movimento crediti'}</p>
            <span className="rounded-full border border-zinc-700 px-2.5 py-1 text-[11px] font-medium uppercase tracking-wide text-zinc-300">
              {formatEntryTypeLabel(entry.entry_type)}
            </span>
          </div>
          <p className="mt-1 text-sm text-zinc-400">{formatLedgerDate(entry.created_at)}</p>
          <div className="mt-3 flex flex-wrap gap-2 text-xs text-zinc-300">
            <span className="rounded-full border border-zinc-800 bg-zinc-900 px-2.5 py-1">
              {formatQuotaLabel(entry.quota_field)}
            </span>
            {referenceLabel && (
              <span className="rounded-full border border-zinc-800 bg-zinc-900 px-2.5 py-1">
                {referenceLabel}
              </span>
            )}
            {typeof pagesCount === 'number' && pagesCount > 0 && (
              <span className="rounded-full border border-zinc-800 bg-zinc-900 px-2.5 py-1">
                {pagesCount} pagine
              </span>
            )}
            {showReferenceId && (
              <span className="rounded-full border border-zinc-800 bg-zinc-900 px-2.5 py-1 font-mono text-[11px] text-zinc-400">
                Rif. {entry.reference_id}
              </span>
            )}
          </div>
        </div>

        <div className="grid grid-cols-2 gap-3 md:min-w-[320px]">
          <div className="rounded-xl border border-zinc-800 bg-zinc-900/60 p-3">
            <p className="text-[11px] uppercase tracking-wide text-zinc-500">Direzione</p>
            <p className={`mt-1 inline-flex items-center rounded-full border px-2.5 py-1 text-sm font-semibold ${directionClasses}`}>
              {directionLabel} {entry.amount}
            </p>
          </div>
          <div className="rounded-xl border border-zinc-800 bg-zinc-900/60 p-3">
            <p className="text-[11px] uppercase tracking-wide text-zinc-500">Saldo prima</p>
            <p className="mt-1 text-lg font-semibold text-zinc-100">{entry.balance_before}</p>
          </div>
          <div className="rounded-xl border border-zinc-800 bg-zinc-900/60 p-3">
            <p className="text-[11px] uppercase tracking-wide text-zinc-500">Saldo dopo</p>
            <p className="mt-1 text-lg font-semibold text-zinc-100">{entry.balance_after}</p>
          </div>
          <div className="rounded-xl border border-zinc-800 bg-zinc-900/60 p-3">
            <p className="text-[11px] uppercase tracking-wide text-zinc-500">Tipo credito</p>
            <p className="mt-1 text-sm font-semibold text-zinc-200">{formatQuotaLabel(entry.quota_field)}</p>
          </div>
        </div>
      </div>
    </div>
  );
};

const Billing = () => {
  const { user, logout, refreshUser, accountState } = useAuth();
  const [searchParams] = useSearchParams();
  const [plans, setPlans] = useState([]);
  const [loading, setLoading] = useState(true);
  const [checkoutLoadingPlanId, setCheckoutLoadingPlanId] = useState('');
  const [checkingPayment, setCheckingPayment] = useState(false);
  const [checkoutFeedback, setCheckoutFeedback] = useState(null);
  const [ledgerEntries, setLedgerEntries] = useState([]);
  const [ledgerTotal, setLedgerTotal] = useState(0);
  const [ledgerLoading, setLedgerLoading] = useState(true);
  const [ledgerLoadingMore, setLedgerLoadingMore] = useState(false);
  const [ledgerError, setLedgerError] = useState('');
  const pollTimeoutRef = useRef(null);
  const activeCheckoutSessionRef = useRef('');
  const checkoutRequestRef = useRef(0);
  const creditBands = [
    '1-20 pagine = 4 crediti',
    '21-40 pagine = 7 crediti',
    '41-60 pagine = 10 crediti',
    '61-80 pagine = 13 crediti',
    '81-100 pagine = 16 crediti'
  ];

  const clearCheckoutPoll = () => {
    if (pollTimeoutRef.current) {
      window.clearTimeout(pollTimeoutRef.current);
      pollTimeoutRef.current = null;
    }
  };

  const clearCheckoutUrlState = () => {
    const url = new URL(window.location.href);
    url.searchParams.delete('checkout');
    url.searchParams.delete('session_id');
    window.history.replaceState({}, '', `${url.pathname}${url.search}${url.hash}`);
  };

  useEffect(() => {
    fetchPlans();
    fetchLedger({ reset: true });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    const checkoutState = searchParams.get('checkout');
    const sessionId = searchParams.get('session_id');
    const trackedSession = readTrackedCheckoutSession();
    const trackedSessionId = trackedSession?.sessionId || '';

    clearCheckoutPoll();
    setCheckingPayment(false);
    setCheckoutFeedback(null);
    activeCheckoutSessionRef.current = sessionId || '';
    checkoutRequestRef.current += 1;

    if (sessionId) {
      if (trackedSessionId && trackedSessionId !== sessionId) {
        activeCheckoutSessionRef.current = '';
        clearCheckoutUrlState();
        return undefined;
      }
      checkPaymentStatus(sessionId, 0, checkoutRequestRef.current);
      return undefined;
    }

    if (checkoutState === 'cancel') {
      const feedback = {
        type: 'info',
        title: 'Checkout annullato',
        body: 'Questo tentativo e stato annullato. Nessun addebito effettuato e nessun credito aggiunto.',
      };
      setCheckoutFeedback(feedback);
      toast.info(feedback.title);
      clearTrackedCheckoutSession();
      clearCheckoutUrlState();
    }

    return () => {
      clearCheckoutPoll();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams]);

  useEffect(() => () => clearCheckoutPoll(), []);

  const fetchPlans = async () => {
    try {
      const response = await axios.get(`${API_URL}/api/plans`);
      setPlans(response.data.plans || []);
    } catch (error) {
      toast.error('Errore nel caricamento dei piani');
      setPlans([]);
    } finally {
      setLoading(false);
    }
  };

  const fetchLedger = async ({ reset = false } = {}) => {
    const nextSkip = reset ? 0 : ledgerEntries.length;
    if (reset) {
      setLedgerLoading(true);
      setLedgerError('');
    } else {
      setLedgerLoadingMore(true);
    }

    try {
      const response = await axios.get(`${API_URL}/api/billing/ledger`, {
        params: {
          limit: LEDGER_PAGE_SIZE,
          skip: nextSkip,
        },
        withCredentials: true,
      });

      const nextEntries = Array.isArray(response.data?.entries) ? response.data.entries : [];
      setLedgerTotal(Number(response.data?.total || 0));
      setLedgerEntries((current) => (reset ? nextEntries : [...current, ...nextEntries]));
      setLedgerError('');
    } catch (error) {
      console.error('Ledger fetch error:', error);
      setLedgerError('Non è stato possibile caricare i movimenti crediti.');
      if (reset) {
        setLedgerEntries([]);
        setLedgerTotal(0);
      }
    } finally {
      if (reset) {
        setLedgerLoading(false);
      } else {
        setLedgerLoadingMore(false);
      }
    }
  };

  const handleCheckoutResolution = async ({ result, planId, purchaseType }) => {
    const isPack = purchaseType === 'pack' || planId === 'starter';

    if (result === 'success') {
      const feedback = {
        type: 'success',
        title: 'Pagamento confermato',
        body: isPack
          ? 'I crediti extra di questa sessione sono stati aggiunti correttamente.'
          : 'Il piano e i crediti di questa sessione sono stati aggiornati correttamente.',
      };
      setCheckoutFeedback(feedback);
      toast.success(feedback.title);
      await refreshUser();
      await fetchLedger({ reset: true });
      return;
    }

    if (result === 'failed' || result === 'expired') {
      const feedback = {
        type: 'error',
        title: result === 'expired' ? 'Sessione di pagamento scaduta' : 'Pagamento non completato',
        body: 'Questa sessione non ha prodotto variazioni ai crediti o al piano.',
      };
      setCheckoutFeedback(feedback);
      toast.error(feedback.title);
      return;
    }

    if (result === 'manual_review') {
      const feedback = {
        type: 'warning',
        title: 'Pagamento in verifica manuale',
        body: 'Il pagamento e stato ricevuto ma richiede ancora una verifica manuale prima dell aggiornamento dei crediti.',
      };
      setCheckoutFeedback(feedback);
      toast.warning(feedback.title);
    }
  };

  const checkPaymentStatus = async (sessionId, attempts = 0, requestId = checkoutRequestRef.current) => {
    const maxAttempts = 6;
    const pollInterval = 2000;

    if (!sessionId || activeCheckoutSessionRef.current !== sessionId || checkoutRequestRef.current !== requestId) {
      return;
    }

    setCheckingPayment(true);

    try {
      const response = await axios.get(`${API_URL}/api/checkout/status/${sessionId}`, {
        withCredentials: true
      });

      if (activeCheckoutSessionRef.current !== sessionId || checkoutRequestRef.current !== requestId) {
        return;
      }

      const payload = response.data || {};
      const sessionResult = payload.session_result;

      if (['success', 'failed', 'expired', 'manual_review'].includes(sessionResult)) {
        await handleCheckoutResolution({
          result: sessionResult,
          planId: payload.plan_id,
          purchaseType: payload.purchase_type,
        });
        setCheckingPayment(false);
        activeCheckoutSessionRef.current = '';
        clearTrackedCheckoutSession();
        clearCheckoutUrlState();
        return;
      }

      if (attempts >= maxAttempts) {
        const feedback = {
          type: 'info',
          title: 'Checkout ancora in elaborazione',
          body: 'Questa sessione e ancora in sincronizzazione. Se il saldo non si aggiorna a breve, ricarica la pagina.',
        };
        setCheckoutFeedback(feedback);
        toast.info(feedback.title);
        setCheckingPayment(false);
        clearCheckoutUrlState();
        activeCheckoutSessionRef.current = '';
        clearTrackedCheckoutSession();
        return;
      }

      pollTimeoutRef.current = window.setTimeout(() => {
        checkPaymentStatus(sessionId, attempts + 1, requestId);
      }, pollInterval);
    } catch (error) {
      if (activeCheckoutSessionRef.current !== sessionId || checkoutRequestRef.current !== requestId) {
        return;
      }

      const feedback = {
        type: 'info',
        title: 'Stato checkout in sincronizzazione',
        body: 'Questa sessione non puo essere verificata adesso. Se il saldo non si aggiorna a breve, ricarica la pagina una volta.',
      };
      setCheckoutFeedback(feedback);
      toast.info(feedback.title);
      setCheckingPayment(false);
      clearCheckoutUrlState();
      activeCheckoutSessionRef.current = '';
      clearTrackedCheckoutSession();
    }
  };

  const currentPlan = plans.find((plan) => plan.plan_id === accountState.planId);
  const currentPlanDetails = currentPlan || (accountState.isMasterAdmin ? INTERNAL_PLAN_DETAILS : null);
  const hasMoreLedgerEntries = ledgerEntries.length < ledgerTotal;

  const handlePlanAction = async (plan) => {
    if (plan.plan_id === 'free' || plan.plan_id === accountState.planId) return;
    if (plan.plan_id === 'studio') {
      toast.info("Il piano Studio resta gestito manualmente in questa fase.");
      return;
    }

    clearCheckoutPoll();
    setCheckoutFeedback(null);
    setCheckoutLoadingPlanId(plan.plan_id);
    try {
      const response = await axios.post(
        `${API_URL}/api/checkout/create`,
        { plan_id: plan.plan_id, origin_url: window.location.origin },
        { withCredentials: true }
      );
      const checkoutUrl = response.data?.url;
      const checkoutSessionId = response.data?.session_id;
      if (!checkoutUrl) {
        throw new Error('Missing checkout url');
      }
      if (checkoutSessionId) {
        writeTrackedCheckoutSession({
          sessionId: checkoutSessionId,
          planId: plan.plan_id,
          createdAt: Date.now(),
        });
      } else {
        clearTrackedCheckoutSession();
      }
      window.location.assign(checkoutUrl);
    } catch (error) {
      console.error('Checkout create error:', error);
      const detail = error?.response?.data?.detail;
      clearTrackedCheckoutSession();
      toast.error(typeof detail === 'string' ? detail : 'Impossibile avviare il checkout.');
      setCheckoutLoadingPlanId('');
    }
  };

  const feedbackStyle = FEEDBACK_STYLES[checkoutFeedback?.type || 'info'];
  const totalAvailableCredits =
    accountState?.periziaCredits?.totalAvailable ??
    user?.account?.perizia_credits?.total_available ??
    user?.perizia_credits?.total_available ??
    accountState?.quota?.perizia_scans_remaining ??
    0;

  return (
    <div className="min-h-screen bg-[#09090b]">
      <Sidebar user={user} logout={logout} />

      <main className="ml-64 p-8">
        <div className="mb-8">
          <h1 className="text-3xl font-serif font-bold text-zinc-100 mb-2">
            Abbonamento
          </h1>
          <p className="text-zinc-400">
            Panoramica piani e crediti del prodotto attuale
          </p>
        </div>

        {checkingPayment && (
          <div className="mb-8 p-4 bg-gold/10 border border-gold/30 rounded-xl flex items-center gap-4">
            <Loader2 className="w-6 h-6 text-gold animate-spin" />
            <div>
              <p className="font-semibold text-zinc-100">Verifica pagamento in corso...</p>
              <p className="text-sm text-zinc-400">Controllo limitato alla sessione corrente</p>
            </div>
          </div>
        )}

        {checkoutFeedback && (
          <div className={`mb-8 flex items-start gap-4 rounded-xl border p-4 ${feedbackStyle.wrapper}`}>
            <AlertCircle className={`w-5 h-5 flex-shrink-0 mt-0.5 ${feedbackStyle.icon}`} />
            <div>
              <p className={`font-semibold ${feedbackStyle.title}`}>{checkoutFeedback.title}</p>
              <p className={`mt-1 text-sm ${feedbackStyle.body}`}>{checkoutFeedback.body}</p>
            </div>
          </div>
        )}

        <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6 mb-8">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-zinc-500 mb-1">Piano attuale</p>
              <div className="flex items-center gap-3">
                <h2 className="text-2xl font-serif font-bold text-zinc-100 capitalize">
                  {currentPlanDetails?.name_it || accountState.planLabel}
                </h2>
                {accountState.isMasterAdmin && (
                  <span className="px-2 py-1 bg-gold/20 text-gold text-xs font-mono rounded">
                    MASTER ADMIN
                  </span>
                )}
              </div>
            </div>
            <Crown className={`w-8 h-8 ${
              accountState.planId === 'enterprise' ? 'text-gold' :
              accountState.planId === 'pro' ? 'text-indigo-400' :
                'text-zinc-600'
            }`} />
          </div>

          <div className="mt-6 grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="p-4 bg-zinc-950 rounded-lg">
              <p className="text-xs text-zinc-500 mb-1">Crediti disponibili</p>
              <p className="text-2xl font-mono font-bold text-gold">
                {totalAvailableCredits}
              </p>
            </div>
            <div className="p-4 bg-zinc-950 rounded-lg">
              <p className="text-xs text-zinc-500 mb-1">Tipo piano</p>
              <p className="text-sm font-semibold text-zinc-200">
                {currentPlanDetails?.plan_type_label_it || 'Non disponibile'}
              </p>
            </div>
            <div className="p-4 bg-zinc-950 rounded-lg">
              <p className="text-xs text-zinc-500 mb-1">Supporto</p>
              <p className="text-sm font-semibold text-zinc-200">
                {currentPlanDetails?.support_level_it || 'Supporto base'}
              </p>
            </div>
          </div>
          <div className="mt-6 flex justify-start">
            <Button asChild className="bg-gold text-zinc-950 hover:bg-gold-dim">
              <Link to="#billing-plans">Ricarica crediti</Link>
            </Button>
          </div>
        </div>

        <h3 className="text-xl font-serif font-bold text-zinc-100 mb-6">Piani Disponibili</h3>

        <div id="billing-plans" className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-5 gap-6">
          {loading ? (
            <div className="col-span-full text-center py-12">
              <Loader2 className="w-8 h-8 text-gold animate-spin mx-auto" />
            </div>
          ) : (
            plans.map((plan) => (
              <div
                key={plan.plan_id}
                data-testid={`billing-plan-${plan.plan_id}`}
                className={`relative bg-zinc-900 border rounded-2xl p-6 transition-all duration-300 ${
                  plan.plan_id === accountState.planId
                    ? 'border-gold ring-2 ring-gold/20'
                    : plan.plan_id === 'solo'
                      ? 'border-indigo-500/50'
                      : 'border-zinc-800 hover:border-zinc-600'
                }`}
              >
                {plan.plan_id === accountState.planId && (
                  <div className="absolute -top-3 left-1/2 -translate-x-1/2">
                    <span className="px-3 py-1 bg-gold text-zinc-950 text-xs font-bold rounded-full">
                      ATTIVO
                    </span>
                  </div>
                )}

                <h3 className="text-xl font-serif font-bold text-zinc-100 mb-2">
                  {plan.name_it}
                </h3>

                <p className="text-xs font-mono uppercase tracking-wider text-zinc-500 mb-4">
                  {plan.plan_type_label_it}
                </p>

                <div className="flex items-baseline gap-1 mb-6">
                  {plan.plan_id === 'studio' ? (
                    <span className="text-2xl font-bold text-gold">Richiedi un'offerta</span>
                  ) : (
                    <span className="text-3xl font-bold text-gold">
                      €{plan.price.toFixed(0)}
                    </span>
                  )}
                  {plan.plan_id !== 'studio' && plan.price_suffix_it && (
                    <span className="text-zinc-500">{plan.price_suffix_it}</span>
                  )}
                </div>

                <div className="space-y-2 mb-6 text-sm">
                  <p className="text-zinc-200 font-medium">{plan.credits_label_it}</p>
                  {plan.validity_label_it && (
                    <p className="text-zinc-500">{plan.validity_label_it}</p>
                  )}
                  {plan.support_level_it && (
                    <p className="text-zinc-500">{plan.support_level_it}</p>
                  )}
                </div>

                <ul className="space-y-3 mb-6">
                  {plan.features_it.map((feature, i) => (
                    <li key={i} className="flex items-start gap-3 text-sm text-zinc-300">
                      <CheckCircle2 className="w-5 h-5 text-emerald-400 flex-shrink-0 mt-0.5" />
                      {feature}
                    </li>
                  ))}
                </ul>

                {plan.plan_id === accountState.planId ? (
                  <Button disabled className="w-full bg-zinc-800 text-zinc-500 cursor-not-allowed">
                    Piano Attuale
                  </Button>
                ) : plan.plan_id === 'free' ? (
                  <Button disabled className="w-full bg-zinc-800 text-zinc-500 cursor-not-allowed">
                    {plan.cta_label_it}
                  </Button>
                ) : (
                  <Button
                    onClick={() => handlePlanAction(plan)}
                    data-testid={`subscribe-${plan.plan_id}-btn`}
                    disabled={Boolean(checkoutLoadingPlanId)}
                    className={`w-full ${
                      plan.plan_id === 'solo'
                        ? 'bg-indigo-600 hover:bg-indigo-700 text-white'
                        : 'bg-gold text-zinc-950 hover:bg-gold-dim'
                    }`}
                  >
                    {checkoutLoadingPlanId === plan.plan_id ? (
                      <>
                        <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                        Reindirizzamento...
                      </>
                    ) : (
                      <>
                        <CreditCard className="w-4 h-4 mr-2" />
                        {plan.plan_id === 'studio' ? "Richiedi un'offerta" : plan.cta_label_it}
                      </>
                    )}
                  </Button>
                )}
              </div>
            ))
          )}
        </div>

        <div className="mt-8 p-4 bg-zinc-900/50 border border-zinc-800 rounded-lg">
          <h3 className="text-lg font-semibold text-zinc-100 mb-2">Come funzionano i crediti</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-5 gap-3 text-sm">
            {creditBands.map((band) => (
              <div key={band} className="rounded-xl border border-zinc-800 bg-zinc-950 px-4 py-3 text-zinc-300">
                {band}
              </div>
            ))}
          </div>
          <p className="text-xs text-zinc-600 mt-4">Crediti extra disponibili.</p>
        </div>

        <section className="mt-8 rounded-2xl border border-zinc-800 bg-zinc-900/60 p-6">
          <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
            <div>
              <h3 className="text-xl font-serif font-bold text-zinc-100">Movimenti crediti</h3>
              <p className="mt-1 text-sm text-zinc-400">
                Storico recente dei movimenti registrati sul tuo account.
              </p>
            </div>
            <p className="text-sm text-zinc-500">
              {ledgerTotal > 0 ? `${ledgerEntries.length} di ${ledgerTotal} movimenti` : 'Nessun movimento registrato'}
            </p>
          </div>

          <div className="mt-6">
            {ledgerLoading ? (
              <div className="space-y-3">
                {[0, 1, 2].map((item) => (
                  <div key={item} className="animate-pulse rounded-2xl border border-zinc-800 bg-zinc-950/70 p-4">
                    <div className="h-4 w-48 rounded bg-zinc-800" />
                    <div className="mt-3 h-3 w-32 rounded bg-zinc-800" />
                    <div className="mt-4 grid grid-cols-2 gap-3 md:grid-cols-4">
                      {[0, 1, 2, 3].map((block) => (
                        <div key={block} className="h-16 rounded-xl bg-zinc-900" />
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            ) : ledgerError ? (
              <div className="rounded-2xl border border-amber-500/20 bg-amber-500/5 p-5">
                <p className="text-sm font-medium text-amber-200">{ledgerError}</p>
                <Button
                  onClick={() => fetchLedger({ reset: true })}
                  variant="outline"
                  className="mt-4 border-zinc-700 bg-transparent text-zinc-200 hover:bg-zinc-800"
                >
                  Riprova
                </Button>
              </div>
            ) : ledgerEntries.length === 0 ? (
              <div className="rounded-2xl border border-dashed border-zinc-700 bg-zinc-950/50 p-8 text-center">
                <p className="text-sm font-medium text-zinc-200">Non ci sono ancora movimenti registrati.</p>
                <p className="mt-2 text-sm text-zinc-500">I prossimi addebiti o accrediti compariranno qui.</p>
              </div>
            ) : (
              <div className="space-y-4">
                {ledgerEntries.map((entry) => (
                  <LedgerRow key={entry.ledger_id} entry={entry} />
                ))}
              </div>
            )}
          </div>

          {!ledgerLoading && !ledgerError && hasMoreLedgerEntries && (
            <div className="mt-6 flex justify-center">
              <Button
                onClick={() => fetchLedger()}
                disabled={ledgerLoadingMore}
                variant="outline"
                className="border-zinc-700 bg-transparent text-zinc-100 hover:bg-zinc-800"
              >
                {ledgerLoadingMore ? (
                  <>
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    Caricamento...
                  </>
                ) : (
                  'Carica altri movimenti'
                )}
              </Button>
            </div>
          )}
        </section>

        <div className="mt-8 p-4 bg-zinc-900/50 border border-zinc-800 rounded-lg flex items-start gap-3">
          <AlertCircle className="w-5 h-5 text-zinc-500 flex-shrink-0 mt-0.5" />
          <div className="text-sm text-zinc-500">
            <p>Questa schermata mostra il modello commerciale attuale del prodotto.</p>
            <p className="mt-1">Assistente e Image Forensics non sono inclusi nei pacchetti attivi.</p>
          </div>
        </div>
      </main>
    </div>
  );
};

export default Billing;
