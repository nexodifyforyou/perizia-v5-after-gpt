import React, { useEffect, useState } from 'react';
import axios from 'axios';
import { CheckCircle2 } from 'lucide-react';
import { Link } from 'react-router-dom';
import { Button } from './ui/button';

const API_URL = process.env.REACT_APP_BACKEND_URL;

export const usePublicPlans = () => {
  const [plans, setPlans] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let active = true;

    const fetchPlans = async () => {
      try {
        const response = await axios.get(`${API_URL}/api/plans`);
        if (active) {
          setPlans(response.data.plans || []);
        }
      } catch (error) {
        if (active) {
          setPlans([]);
        }
        console.error('Failed to fetch plans:', error);
      } finally {
        if (active) {
          setLoading(false);
        }
      }
    };

    fetchPlans();
    return () => {
      active = false;
    };
  }, []);

  return { plans, loading };
};

const defaultSummary = {
  free: 'Accesso introduttivo per valutare il flusso documentale.',
  starter: 'Taglio operativo leggero per volumi contenuti.',
  solo: 'Il pacchetto centrale oggi piu coerente con il prodotto attuale.',
  pro: 'Maggiore continuita per professionisti con frequenza piu alta.',
  studio: 'Capacita estesa per team e studi con piu pratiche.'
};

const ctaByPlan = {
  free: 'Accesso disponibile in piattaforma',
  starter: 'Disponibilita commerciale in aggiornamento',
  solo: 'Disponibilita commerciale in aggiornamento',
  pro: 'Disponibilita commerciale in aggiornamento',
  studio: 'Disponibilita commerciale in aggiornamento'
};

const PublicPlansGrid = ({ detailed = false }) => {
  const { plans, loading } = usePublicPlans();

  if (loading) {
    return (
      <div className="rounded-3xl border border-zinc-800 bg-zinc-900/50 p-10 text-center text-zinc-500">
        Caricamento pacchetti...
      </div>
    );
  }

  return (
    <div className={`grid grid-cols-1 md:grid-cols-2 ${detailed ? 'xl:grid-cols-5' : 'xl:grid-cols-5'} gap-6`}>
      {plans.map((plan) => {
        const featured = plan.plan_id === 'solo';

        return (
          <article
            key={plan.plan_id}
            className={`relative rounded-3xl border p-7 transition-colors ${
              featured
                ? 'border-gold/60 bg-gradient-to-b from-[#1d1910] to-zinc-900 gold-glow'
                : 'border-zinc-800 bg-zinc-900/80 hover:border-zinc-700'
            }`}
          >
            {featured && (
              <div className="absolute -top-4 left-6">
                <span className="premium-badge">Core attuale</span>
              </div>
            )}

            <div className="mb-6">
              <p className="text-xs font-mono uppercase tracking-[0.28em] text-zinc-500 mb-3">{plan.plan_type_label_it}</p>
              <h3 className="text-2xl font-serif font-bold text-zinc-100 mb-2">{plan.name_it}</h3>
              <p className="text-sm leading-relaxed text-zinc-400">{defaultSummary[plan.plan_id] || 'Pacchetto pubblico PeriziaScan.'}</p>
            </div>

            <div className="flex items-end gap-2 mb-5">
              {plan.plan_id === 'studio' ? (
                <span className="text-3xl font-bold text-gold">Richiedi un'offerta</span>
              ) : (
                <span className="text-4xl font-bold text-gold">€{Number(plan.price).toFixed(0)}</span>
              )}
              {plan.plan_id !== 'studio' && plan.price_suffix_it && <span className="text-zinc-500 mb-1">{plan.price_suffix_it}</span>}
            </div>

            <div className="space-y-2 text-sm mb-6">
              <p className="text-zinc-100 font-medium">{plan.credits_label_it}</p>
              {plan.validity_label_it && <p className="text-zinc-500">{plan.validity_label_it}</p>}
              {plan.support_level_it && <p className="text-zinc-500">{plan.support_level_it}</p>}
            </div>

            <ul className="space-y-3 mb-7">
              {((detailed ? plan.features_it : (plan.features_it || []).slice(0, 3)) || []).map((feature) => (
                <li key={feature} className="flex items-start gap-3 text-sm text-zinc-300">
                  <CheckCircle2 className="w-5 h-5 text-emerald-400 flex-shrink-0 mt-0.5" />
                  <span>{feature}</span>
                </li>
              ))}
            </ul>

            {!detailed && (
              <p className="text-xs text-zinc-500 mb-6">
                Valore attuale centrato sull’analisi documentale. Assistente e Image Forensics restano funzioni in arrivo.
              </p>
            )}

            {detailed ? (
              <div className="rounded-2xl border border-zinc-800 bg-black/20 p-4">
                <p className="text-xs font-mono uppercase tracking-[0.22em] text-zinc-500 mb-2">Stato commerciale</p>
                <p className="text-sm text-zinc-300">{ctaByPlan[plan.plan_id] || 'Dettagli commerciali disponibili in piattaforma.'}</p>
              </div>
            ) : (
              <Button
                asChild
                className={`w-full ${featured ? 'bg-gold text-zinc-950 hover:bg-gold-dim' : 'bg-zinc-800 text-zinc-100 hover:bg-zinc-700'}`}
              >
                <Link to="/pacchetti">Dettagli pacchetto</Link>
              </Button>
            )}
          </article>
        );
      })}
    </div>
  );
};

export default PublicPlansGrid;
