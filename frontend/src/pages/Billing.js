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
  const [processingPlan, setProcessingPlan] = useState(null);
  const [checkingPayment, setCheckingPayment] = useState(false);

  useEffect(() => {
    fetchPlans();
    
    // Check if returning from Stripe
    const sessionId = searchParams.get('session_id');
    if (sessionId) {
      checkPaymentStatus(sessionId);
    }
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

  const handleSubscribe = async (planId) => {
    if (planId === 'free' || planId === user?.plan) return;

    setProcessingPlan(planId);

    try {
      const response = await axios.post(`${API_URL}/api/checkout/create`, {
        plan_id: planId,
        origin_url: window.location.origin
      }, {
        withCredentials: true
      });

      // Redirect to Stripe Checkout
      window.location.href = response.data.url;
    } catch (error) {
      console.error('Checkout error:', error);
      toast.error('Errore nella creazione del checkout');
      setProcessingPlan(null);
    }
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
            Gestisci il tuo piano e le opzioni di fatturazione
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
                  {user?.plan || 'Free'}
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
          
          {/* Quota Display */}
          <div className="mt-6 grid grid-cols-3 gap-4">
            <div className="p-4 bg-zinc-950 rounded-lg">
              <p className="text-xs text-zinc-500 mb-1">Perizie Rimanenti</p>
              <p className="text-2xl font-mono font-bold text-gold">
                {user?.quota?.perizia_scans_remaining || 0}
              </p>
            </div>
            <div className="p-4 bg-zinc-950 rounded-lg">
              <p className="text-xs text-zinc-500 mb-1">Immagini Rimanenti</p>
              <p className="text-2xl font-mono font-bold text-indigo-400">
                {user?.quota?.image_scans_remaining || 0}
              </p>
            </div>
            <div className="p-4 bg-zinc-950 rounded-lg">
              <p className="text-xs text-zinc-500 mb-1">Messaggi Rimanenti</p>
              <p className="text-2xl font-mono font-bold text-emerald-400">
                {user?.quota?.assistant_messages_remaining || 0}
              </p>
            </div>
          </div>
        </div>

        {/* Plans Grid */}
        <h3 className="text-xl font-serif font-bold text-zinc-100 mb-6">Piani Disponibili</h3>
        
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {loading ? (
            <div className="col-span-3 text-center py-12">
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
                    : plan.plan_id === 'pro'
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
                
                <div className="flex items-baseline gap-1 mb-6">
                  <span className="text-3xl font-bold text-gold">
                    €{plan.price.toFixed(0)}
                  </span>
                  {plan.price > 0 && (
                    <span className="text-zinc-500">/mese</span>
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
                    Piano Base
                  </Button>
                ) : (
                  <Button 
                    onClick={() => handleSubscribe(plan.plan_id)}
                    data-testid={`subscribe-${plan.plan_id}-btn`}
                    disabled={processingPlan === plan.plan_id}
                    className={`w-full ${
                      plan.plan_id === 'pro'
                        ? 'bg-indigo-600 hover:bg-indigo-700 text-white'
                        : 'bg-gold text-zinc-950 hover:bg-gold-dim'
                    }`}
                  >
                    {processingPlan === plan.plan_id ? (
                      <Loader2 className="w-5 h-5 animate-spin" />
                    ) : (
                      <>
                        <CreditCard className="w-4 h-4 mr-2" />
                        Abbonati
                      </>
                    )}
                  </Button>
                )}
              </div>
            ))
          )}
        </div>

        {/* Info */}
        <div className="mt-8 p-4 bg-zinc-900/50 border border-zinc-800 rounded-lg flex items-start gap-3">
          <AlertCircle className="w-5 h-5 text-zinc-500 flex-shrink-0 mt-0.5" />
          <div className="text-sm text-zinc-500">
            <p>I pagamenti sono processati in modo sicuro tramite Stripe.</p>
            <p className="mt-1">Puoi cancellare il tuo abbonamento in qualsiasi momento.</p>
          </div>
        </div>
      </main>
    </div>
  );
};

export default Billing;
