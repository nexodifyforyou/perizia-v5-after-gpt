import React from 'react';
import { Link } from 'react-router-dom';
import { ArrowRight, CheckCircle2, FileStack, ShieldAlert } from 'lucide-react';
import PublicSiteChrome, { PublicSection } from '../components/PublicSiteChrome';
import PublicPlansGrid from '../components/PublicPlansGrid';
import { Button } from '../components/ui/button';
import { creditBands, creditRules, excludedToday, includedToday, packageFaq } from '../lib/publicContent';

const Pacchetti = () => {
  return (
    <PublicSiteChrome
      eyebrow="Pacchetti"
      title="Accesso, crediti e perimetro commerciale del prodotto attuale"
      description="La pagina Pacchetti spiega come viene misurato l’utilizzo di PeriziaScan e quali risultati rendono il prodotto piu utile di una semplice sintesi AI: alert collegati alle pagine, screening strutturato e valore documentale piu difendibile."
      actions={
        <div className="flex flex-col sm:flex-row gap-4">
          <Button asChild className="bg-gold text-zinc-950 hover:bg-gold-dim font-semibold px-6">
            <Link to="/">Torna alla landing</Link>
          </Button>
          <Button asChild variant="outline" className="border-zinc-700 text-zinc-300 hover:bg-zinc-800 px-6">
            <Link to="/supporto">Metodo e limiti del servizio <ArrowRight className="w-4 h-4 ml-2" /></Link>
          </Button>
        </div>
      }
    >
      <PublicSection
        eyebrow="Panoramica"
        title="Panoramica dei piani pubblici"
        description="I pacchetti definiscono crediti, validita e livello di utilizzo previsto. Free rimuove l’attrito iniziale, Starter serve come top-up, Solo e il piano centrale, Pro copre uso frequente e Studio resta su offerta manuale."
      >
        <PublicPlansGrid detailed />
      </PublicSection>

      <PublicSection
        eyebrow="Crediti"
        title="Come funzionano i crediti"
        description="Il consumo e legato alla fascia pagine del documento analizzato. Il piano Free include 12 crediti, cioe fino a 3 perizie standard da 1-20 pagine."
      >
        <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
          {creditBands.map((band) => (
            <div key={band} className="rounded-2xl border border-zinc-800 bg-zinc-900/70 p-5 text-zinc-200">
              {band}
            </div>
          ))}
        </div>
      </PublicSection>

      <PublicSection
        eyebrow="Regole"
        title="Regole di consumo crediti"
        description="Queste regole spiegano quando il sistema consuma crediti e quando non lo fa."
      >
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {creditRules.map((rule) => (
            <div key={rule} className="rounded-2xl border border-zinc-800 bg-zinc-950/70 p-5 flex items-start gap-3">
              <CheckCircle2 className="w-5 h-5 text-gold flex-shrink-0 mt-0.5" />
              <p className="text-zinc-300">{rule}</p>
            </div>
          ))}
        </div>
      </PublicSection>

      <PublicSection
        eyebrow="Perimetro attuale"
        title="Cosa e incluso oggi nel valore dei pacchetti"
        description="Il valore commerciale attuale di PeriziaScan riguarda il core product documentale: lettura strutturata della perizia, controllabilita e report orientato alla verifica."
      >
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="rounded-3xl border border-emerald-500/20 bg-emerald-500/5 p-8">
            <div className="flex items-center gap-3 mb-6">
              <FileStack className="w-6 h-6 text-emerald-300" />
              <h3 className="text-2xl font-serif font-bold text-zinc-100">Incluso oggi</h3>
            </div>
            <ul className="space-y-4">
              {includedToday.map((item) => (
                <li key={item} className="flex items-start gap-3 text-zinc-300">
                  <CheckCircle2 className="w-5 h-5 text-emerald-400 flex-shrink-0 mt-0.5" />
                  <span>{item}</span>
                </li>
              ))}
            </ul>
          </div>
          <div className="rounded-3xl border border-amber-500/20 bg-amber-500/5 p-8">
            <div className="flex items-center gap-3 mb-6">
              <ShieldAlert className="w-6 h-6 text-amber-300" />
              <h3 className="text-2xl font-serif font-bold text-zinc-100">Non incluso oggi</h3>
            </div>
            <ul className="space-y-4">
              {excludedToday.map((item) => (
                <li key={item} className="flex items-start gap-3 text-zinc-300">
                  <CheckCircle2 className="w-5 h-5 text-amber-300 flex-shrink-0 mt-0.5" />
                  <span>{item}</span>
                </li>
              ))}
            </ul>
          </div>
        </div>
      </PublicSection>

      <PublicSection
        eyebrow="FAQ"
        title="FAQ pratica"
        description="Risposte sintetiche alle domande commerciali e operative piu frequenti."
      >
        <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
          {packageFaq.map((item) => (
            <article key={item.question} className="rounded-3xl border border-zinc-800 bg-zinc-900/80 p-7">
              <h3 className="text-xl font-serif font-bold text-zinc-100 mb-3">{item.question}</h3>
              <p className="text-zinc-400 leading-relaxed">{item.answer}</p>
            </article>
          ))}
        </div>
      </PublicSection>
    </PublicSiteChrome>
  );
};

export default Pacchetti;
