import React from 'react';
import PublicSiteChrome, { PublicSection } from '../components/PublicSiteChrome';

const supportItems = [
  'Problemi di accesso o di account',
  'Caricamento file e gestione del documento',
  'Errori di elaborazione',
  'Chiarimenti sul funzionamento dei crediti',
  'Comprensione della struttura del report'
];

const methodologyItems = [
  'Analisi document-first: il punto di partenza e sempre la perizia caricata dall’utente.',
  'Estrazione strutturata dal testo della perizia con lettura orientata a sezioni, dati e passaggi rilevanti.',
  'Normalizzazione deterministica dei contenuti per rendere il report leggibile e controllabile.',
  'Identificazione di rischi, punti legali e costi/oneri da verificare sulla base di quanto il documento consente di sostenere.',
  'Riferimenti di pagina ed evidence anchors quando disponibili, per mantenere tracciabilita verso la fonte.',
  'Approccio conservativo quando la perizia non supporta una quantificazione precisa o lascia aree qualitative.',
  'Distinzione tra fatti trovati nel documento e calcoli derivati dai valori presenti in perizia, quando applicabile.',
  'Obiettivo operativo: aumentare la controllabilita del pre-screening, non produrre magia opaca da black box.'
];

const notReplacement = [
  'Non e consulenza legale',
  'Non e consulenza fiscale',
  'Non sostituisce il sopralluogo o la verifica tecnica sul posto',
  'Non sostituisce la due diligence professionale',
  'La decisione finale sull’asta resta in capo all’utente'
];

const limits = [
  'La qualita dell’output dipende dalla qualita e completezza del documento caricato.',
  'Scansioni deboli o OCR incompleto possono ridurre la precisione di estrazione.',
  'Informazioni assenti nella perizia non possono essere inventate dal sistema.',
  'Alcuni costi o oneri possono restare qualitativi o non quantificati se il documento non li supporta in modo sufficiente.'
];

const Supporto = () => {
  return (
    <PublicSiteChrome
      eyebrow="Supporto"
      title="Supporto operativo, metodologia pubblica e confini del servizio"
      description="Questa pagina spiega cosa copre il supporto, come PeriziaScan affronta l’analisi della perizia e quali aspetti restano necessariamente fuori da un sistema di lettura documentale."
    >
      <PublicSection
        eyebrow="Copertura"
        title="Cosa copre il supporto"
        description="Il supporto e focalizzato sull’uso corretto della piattaforma e sulla lettura del prodotto attuale. Non vengono promessi SLA pubblici non dichiarati."
      >
        <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
          {supportItems.map((item) => (
            <div key={item} className="rounded-3xl border border-zinc-800 bg-zinc-900/80 p-7 text-zinc-300">
              {item}
            </div>
          ))}
        </div>
      </PublicSection>

      <PublicSection
        eyebrow="Metodo"
        title="Come lavora PeriziaScan"
        description="La metodologia pubblica privilegia struttura, controllabilita e riferimenti al documento. Il sistema non pretende precisione dove la perizia non la consente."
      >
        <div className="space-y-4">
          {methodologyItems.map((item) => (
            <div key={item} className="rounded-2xl border border-zinc-800 bg-zinc-950/60 p-5 text-zinc-300">
              {item}
            </div>
          ))}
        </div>
      </PublicSection>

      <PublicSection
        eyebrow="Non sostituisce"
        title="Cosa il sistema non sostituisce"
        description="PeriziaScan e uno strumento di supporto alla lettura documentale. Non prende il posto delle valutazioni professionali richieste prima di una decisione in asta."
      >
        <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
          {notReplacement.map((item) => (
            <div key={item} className="rounded-3xl border border-zinc-800 bg-zinc-900/80 p-7 text-zinc-300">
              {item}
            </div>
          ))}
        </div>
      </PublicSection>

      <PublicSection
        eyebrow="Limiti"
        title="Limiti operativi"
        description="La piattaforma migliora il pre-screening, ma resta vincolata al materiale caricato e alla sua qualita."
      >
        <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
          {limits.map((item) => (
            <div key={item} className="rounded-3xl border border-zinc-800 bg-zinc-900/80 p-7 text-zinc-300">
              {item}
            </div>
          ))}
        </div>
      </PublicSection>

      <PublicSection
        eyebrow="Contatto"
        title="Percorso di supporto"
        description="Per richieste di supporto puoi scrivere a nexodifyforyou@gmail.com. Le segnalazioni vengono gestite in relazione al problema riportato e allo stato operativo del servizio."
      />
    </PublicSiteChrome>
  );
};

export default Supporto;
