import React from 'react';
import PublicSiteChrome, { PublicSection } from '../components/PublicSiteChrome';

const sections = [
  {
    title: '1. Tipi di dati trattati',
    text: 'Per l’uso del servizio possono essere trattati dati account, file o documenti caricati dall’utente e metadati essenziali di utilizzo necessari al funzionamento della piattaforma.'
  },
  {
    title: '2. Finalita del trattamento',
    text: 'I dati vengono trattati per eseguire l’analisi richiesta, presentare il report strutturato, fornire supporto operativo e mantenere sicurezza e miglioramento del servizio nei limiti del contesto operativo effettivo.'
  },
  {
    title: '3. Principio di minimizzazione e controllabilita',
    text: 'PeriziaScan e progettato per analizzare il materiale caricato in relazione al servizio richiesto. L’obiettivo e limitare il trattamento a quanto necessario per produrre l’analisi documentale e renderne leggibile il risultato.'
  },
  {
    title: '4. Conservazione e accesso',
    text: 'Il PDF originale non viene conservato salvo consenso specifico, facoltativo e separato per la singola analisi. Se l’opzione è disponibile e viene selezionata, la copia originale è cifrata, usata soltanto per finalità diagnostiche, collegata all’account e all’analisi, ed eliminata automaticamente entro il periodo indicato al momento del consenso. Il servizio resta utilizzabile senza prestare questo consenso.'
  },
  {
    title: '5. Sicurezza',
    text: 'La piattaforma adotta misure tecniche e organizzative coerenti con un servizio applicativo online. Questa pagina non elenca certificazioni, standard o garanzie ulteriori non pubblicamente dichiarate.'
  },
  {
    title: '6. Diritti e richieste',
    text: 'Per richieste relative ai dati o all’uso del servizio puoi scrivere a nexodifyforyou@gmail.com. Le richieste vengono gestite secondo il contesto operativo e normativo applicabile.'
  },
  {
    title: 'Richieste di cancellazione',
    text: 'Puoi revocare in qualsiasi momento il consenso alla conservazione diagnostica dalla pagina Profilo: la copia cifrata viene eliminata immediatamente e non attende la scadenza automatica. Anche l’eliminazione della singola analisi o di tutto lo storico elimina la relativa copia conservata. In caso di errore tecnico, il sistema non dichiara conclusa la cancellazione e mantiene uno stato sicuro da ritentare. Per richieste ulteriori puoi contattarci a nexodifyforyou@gmail.com.'
  },
  {
    title: '7. Aggiornamenti dell’informativa',
    text: 'Questa informativa pubblica puo essere aggiornata per riflettere cambiamenti di prodotto, processo o assetto operativo. La versione pubblicata sul sito rappresenta il testo corrente.'
  }
];

const Privacy = () => {
  return (
    <PublicSiteChrome
      eyebrow="Privacy"
      title="Informativa pubblica sul trattamento dati del servizio"
      description="Questa pagina fornisce una panoramica onesta e product-specific dei dati trattati da PeriziaScan, delle finalita del trattamento e dei limiti delle informazioni oggi dichiarabili pubblicamente."
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

export default Privacy;
