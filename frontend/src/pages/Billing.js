import React, { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
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

const Billing = () => {
  const { user, logout, refreshUser } = useAuth();
  const [searchParams] = useSearchParams();
  const [plans, setPlans] = useState([]);
  const [loading, setLoading] = useState(true);
  const [checkingPayment, setCheckingPayment] = useState(false);
  const creditBands = [
    '1-20 pagine = 4 crediti',
    '21-40 pagine = 7 crediti',
    '41-60 pagine = 10 crediti',
    '61-80 pagine = 13 crediti',
    '81-100 pagine = 16 crediti'
  ];

  useEffect(() => {
    fetchPlans();
    
    // Check if returning from Stripe
    const sessionId = searchParams.get('session_id');
    if (sessionId) {
      checkPaymentStatus(sessionId);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams]);

  const fetchPlans = async () => {
    try {
      const response = await axios.get(`${API_URL}/api/plans`);
      setPlans(response.data.plans);
    } catch (error) {
      toast.error('Errore nel caricamento dei piani');
    } finally {
      setLoading(false);
    }
  };

  const checkPaymentStatus = async (sessionId, attempts = 0) => {
    const maxAttempts = 5;
    const pollInterval = 2000;

    if (attempts >= maxAttempts) {
      toast.info('Verifica lo stato del pagamento nel tuo account');
      setCheckingPayment(false);
      return;
    }

    setCheckingPayment(true);

    try {
      const response = await axios.get(`${API_URL}/api/checkout/status/${sessionId}`, {
        withCredentials: true
      });

      if (response.data.payment_status === 'paid') {
        toast.success('Pagamento completato! Il tuo piano è stato aggiornato.');
        await refreshUser();
        setCheckingPayment(false);
        // Clear the URL params
        window.history.replaceState({}, '', window.location.pathname);
        return;
      } else if (response.data.status === 'expired') {
        toast.error('Sessione di pagamento scaduta. Riprova.');
        setCheckingPayment(false);
        return;
      }

      // Continue polling
      setTimeout(() => checkPaymentStatus(sessionId, attempts + 1), pollInterval);
    } catch (error) {
      console.error('Payment check error:', error);
      setCheckingPayment(false);
    }
  };

  const currentPlan = plans.find((plan) => plan.plan_id === user?.plan);

  const handlePlanAction = (plan) => {
    if (plan.plan_id === 'free' || plan.plan_id === user?.plan) return;
    toast.info(`${plan.cta_label_it} non ancora abilitato in questa versione.`);
  };

  return (
    <div className="min-h-screen bg-[#09090b]">
      <Sidebar user={user} logout={logout} />
      
      <main className="ml-64 p-8">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-serif font-bold text-zinc-100 mb-2">
            Abbonamento
          </h1>
          <p className="text-zinc-400">
            Panoramica piani e crediti del prodotto attuale
          </p>
        </div>

        {/* Payment Processing Banner */}
        {checkingPayment && (
          <div className="mb-8 p-4 bg-gold/10 border border-gold/30 rounded-xl flex items-center gap-4">
            <Loader2 className="w-6 h-6 text-gold animate-spin" />
            <div>
              <p className="font-semibold text-zinc-100">Verifica pagamento in corso...</p>
              <p className="text-sm text-zinc-400">Attendere prego</p>
            </div>
          </div>
        )}

        {/* Current Plan */}
        <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6 mb-8">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-zinc-500 mb-1">Piano attuale</p>
              <div className="flex items-center gap-3">
                <h2 className="text-2xl font-serif font-bold text-zinc-100 capitalize">
                  {currentPlan?.name_it || user?.plan || 'Free'}
                </h2>
                {user?.is_master_admin && (
                  <span className="px-2 py-1 bg-gold/20 text-gold text-xs font-mono rounded">
                    MASTER ADMIN
                  </span>
                )}
              </div>
            </div>
            <Crown className={`w-8 h-8 ${
              user?.plan === 'enterprise' ? 'text-gold' :
              user?.plan === 'pro' ? 'text-indigo-400' :
                'text-zinc-600'
            }`} />
          </div>
          
          <div className="mt-6 grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="p-4 bg-zinc-950 rounded-lg">
              <p className="text-xs text-zinc-500 mb-1">Crediti disponibili</p>
              <p className="text-2xl font-mono font-bold text-gold">
                {user?.quota?.perizia_scans_remaining || 0}
              </p>
            </div>
            <div className="p-4 bg-zinc-950 rounded-lg">
              <p className="text-xs text-zinc-500 mb-1">Tipo piano</p>
              <p className="text-sm font-semibold text-zinc-200">
                {currentPlan?.plan_type_label_it || (user?.plan === 'enterprise' ? 'Interno' : 'Non disponibile')}
              </p>
            </div>
            <div className="p-4 bg-zinc-950 rounded-lg">
              <p className="text-xs text-zinc-500 mb-1">Supporto</p>
              <p className="text-sm font-semibold text-zinc-200">
                {currentPlan?.support_level_it || (user?.plan === 'enterprise' ? 'Supporto dedicato' : 'Supporto base')}
              </p>
            </div>
          </div>
        </div>

        {/* Plans Grid */}
        <h3 className="text-xl font-serif font-bold text-zinc-100 mb-6">Piani Disponibili</h3>
        
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-5 gap-6">
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
                  plan.plan_id === user?.plan 
                    ? 'border-gold ring-2 ring-gold/20' 
                    : plan.plan_id === 'solo'
                      ? 'border-indigo-500/50'
                      : 'border-zinc-800 hover:border-zinc-600'
                }`}
              >
                {plan.plan_id === user?.plan && (
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
                  <span className="text-3xl font-bold text-gold">
                    €{plan.price.toFixed(0)}
                  </span>
                  {plan.price_suffix_it && (
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
                
                {plan.plan_id === user?.plan ? (
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
                    disabled={false}
                    className={`w-full ${
                      plan.plan_id === 'solo'
                        ? 'bg-indigo-600 hover:bg-indigo-700 text-white'
                        : 'bg-gold text-zinc-950 hover:bg-gold-dim'
                    }`}
                  >
                    <>
                      <CreditCard className="w-4 h-4 mr-2" />
                      {plan.cta_label_it}
                    </>
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

        {/* Info */}
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
