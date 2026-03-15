import React from 'react';
import PublicSiteChrome, { PublicSection } from '../components/PublicSiteChrome';

const sections = [
  {
    title: '1. Ambito del servizio',
    text: 'PeriziaScan e una piattaforma di supporto all’analisi documentale di perizie d’asta immobiliari. Il servizio aiuta a organizzare informazioni, evidenziare punti di attenzione e presentare un report strutturato basato sul documento caricato.'
  },
  {
    title: '2. Uso consentito della piattaforma',
    text: 'La piattaforma puo essere utilizzata per la lettura e il pre-screening di documenti con finalita lecite e coerenti con il contesto immobiliare, professionale o informativo dell’utente. Non e consentito usare il servizio per scopi illeciti, fraudolenti o contrari alla normativa applicabile.'
  },
  {
    title: '3. Account e accesso',
    text: 'L’accesso ad alcune funzioni richiede un account valido. L’utente e responsabile dell’uso del proprio accesso e della correttezza delle informazioni fornite in piattaforma.'
  },
  {
    title: '4. Crediti, pacchetti e validita',
    text: 'L’utilizzo del servizio puo essere regolato da crediti e pacchetti pubblici. Il consumo dipende dalle regole commerciali dichiarate nella pagina Pacchetti. I crediti in abbonamento hanno validita mensile; i crediti acquistati come pack una tantum restano validi 12 mesi, salvo diverse comunicazioni ufficiali in piattaforma.'
  },
  {
    title: '5. Limitazioni del servizio',
    text: 'La qualita dell’output dipende dal contenuto della perizia, dalla qualita del file e dall’eventuale OCR disponibile. Informazioni mancanti o non supportate dal documento non possono essere ricostruite come fatti certi.'
  },
  {
    title: '6. Nessuna consulenza professionale',
    text: 'Il servizio non costituisce consulenza legale, fiscale, tecnica o professionale. Le informazioni mostrate hanno finalita di supporto alla lettura documentale e non sostituiscono le verifiche richieste prima di una decisione di acquisto o partecipazione in asta.'
  },
  {
    title: '7. Responsabilita dell’utente',
    text: 'L’utente resta responsabile della valutazione finale del bene, della verifica delle fonti e dell’eventuale coinvolgimento di professionisti qualificati. Le decisioni operative o economiche restano in capo all’utente.'
  },
  {
    title: '8. Disponibilita del servizio e modifiche',
    text: 'La piattaforma puo essere aggiornata, modificata o limitata nel tempo per ragioni tecniche, operative o commerciali. Pagine pubbliche, pacchetti e funzionalita possono evolvere senza che cio implichi continuita automatica di funzioni in arrivo o sperimentali.'
  },
  {
    title: '9. Uso improprio / abusi',
    text: 'Comportamenti abusivi, tentativi di accesso non autorizzato, uso anomalo del servizio o caricamento di contenuti non appropriati possono comportare limitazioni o sospensione dell’accesso.'
  },
  {
    title: '10. Contatti e aggiornamenti',
    text: 'Per richieste relative all’uso della piattaforma o agli aggiornamenti di questi termini puoi scrivere a nexodifyforyou@gmail.com. Questa pagina puo essere aggiornata per riflettere l’evoluzione operativa e commerciale di PeriziaScan.'
  }
];

const Termini = () => {
  return (
    <PublicSiteChrome
      eyebrow="Termini"
      title="Termini pubblici del servizio PeriziaScan"
      description="Questa pagina riassume in forma pubblica e leggibile il perimetro d’uso del prodotto, i limiti del servizio e le responsabilita essenziali connesse all’utilizzo della piattaforma."
    >
      <PublicSection>
        <div className="space-y-5">
          {sections.map((section) => (
            <article key={section.title} className="rounded-3xl border border-zinc-800 bg-zinc-900/80 p-8">
              <h2 className="text-2xl font-serif font-bold text-zinc-100 mb-4">{section.title}</h2>
              <p className="text-zinc-400 leading-relaxed">{section.text}</p>
            </article>
          ))}
        </div>
      </PublicSection>
    </PublicSiteChrome>
  );
};

export default Termini;
