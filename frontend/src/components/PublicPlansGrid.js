import React, { useEffect, useState } from 'react';
import axios from 'axios';
import { ArrowUpRight, CheckCircle2 } from 'lucide-react';
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
  free: 'Ingresso senza attrito per provare il metodo su perizie standard.',
  starter: 'Top-up occasionale per aggiungere capacita senza cambiare piano.',
  solo: 'Il piano centrale per chi fa screening serio e ricorrente.',
  pro: 'Per uso professionale ad alta frequenza e maggior continuita operativa.',
  studio: 'Percorso dedicato con attivazione manuale per studi e team.'
};

const ctaByPlan = {
  free: 'Fino a 3 perizie standard da 1-20 pagine',
  starter: 'Top-up una tantum per uso occasionale',
  solo: 'Consigliato per investitori che analizzano con continuita',
  pro: 'Pensato per chi lavora su volumi piu alti',
  studio: 'Offerta manuale per esigenze dedicate'
};

const planNameOverrides = {
  starter: 'Pacchetto 8 crediti',
};

const planEyebrow = {
  free: 'Ingresso',
  starter: 'Extra',
  solo: 'Hero plan',
  pro: 'Professionale',
  studio: 'Su richiesta',
};

const planEvidenceNotes = {
  free: 'Provi il flusso con alert collegati alle pagine e lettura documentale verificabile.',
  starter: 'Aggiunge 8 crediti extra mantenendo invariata la logica di consumo per fascia pagine.',
  solo: 'Pensato per chi vuole uno strumento piu rapido della revisione manuale iniziale e piu difendibile di un riassunto generico.',
  pro: 'Per screening ricorrente, operativo e professionale su piu pratiche.',
  studio: 'Da configurare manualmente quando servono volumi o esigenze organizzative dedicate.',
};

const supportOverrides = {
  studio: 'Offerta dedicata con attivazione assistita',
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
    <div className={`grid grid-cols-1 gap-6 md:grid-cols-2 xl:grid-cols-5`}>
      {plans.map((plan) => {
        const featured = plan.plan_id === 'solo';

        return (
          <article
            key={plan.plan_id}
            className={`plan-surface section-fade relative flex h-full flex-col rounded-3xl border p-5 transition-all duration-300 sm:p-7 ${
              featured
                ? 'plan-surface-hero border-gold/60 bg-gradient-to-b from-[#20190d] via-[#141414] to-zinc-950 gold-glow'
                : 'border-zinc-800 bg-zinc-900/80 hover:-translate-y-1 hover:border-zinc-700'
            }`}
          >
            {featured && (
              <div className="absolute -top-4 left-6">
                <span className="premium-badge">Piano consigliato</span>
              </div>
            )}

            <div className="mb-6">
              <p className="mb-3 text-xs font-mono uppercase tracking-[0.28em] text-zinc-500">{planEyebrow[plan.plan_id] || plan.plan_type_label_it}</p>
              <h3 className="mb-2 text-2xl font-serif font-bold text-zinc-100 text-wrap-safe">{planNameOverrides[plan.plan_id] || plan.name_it}</h3>
              <p className="text-sm leading-relaxed text-zinc-400 text-wrap-safe">{defaultSummary[plan.plan_id] || 'Pacchetto pubblico PeriziaScan.'}</p>
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
              {(supportOverrides[plan.plan_id] || plan.support_level_it) && <p className="text-zinc-500">{supportOverrides[plan.plan_id] || plan.support_level_it}</p>}
            </div>

            <div className={`mb-6 rounded-2xl border p-4 text-sm leading-relaxed ${
              featured
                ? 'border-gold/20 bg-gold/10 text-zinc-200'
                : 'border-zinc-800 bg-black/20 text-zinc-300'
            }`}>
              {planEvidenceNotes[plan.plan_id]}
            </div>

            <ul className="mb-7 space-y-3">
              {((detailed ? plan.features_it : (plan.features_it || []).slice(0, 3)) || []).map((feature) => (
                <li key={feature} className="flex items-start gap-3 text-sm text-zinc-300">
                  <CheckCircle2 className="w-5 h-5 text-emerald-400 flex-shrink-0 mt-0.5" />
                  <span className="text-wrap-safe">{feature}</span>
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
                className={`mt-auto w-full ${featured ? 'bg-gold text-zinc-950 hover:bg-gold-dim' : 'bg-zinc-800 text-zinc-100 hover:bg-zinc-700'}`}
              >
                <Link to="/pacchetti">Dettagli pacchetto <ArrowUpRight className="w-4 h-4 ml-2" /></Link>
              </Button>
            )}
          </article>
        );
      })}
    </div>
  );
};

export default PublicPlansGrid;
