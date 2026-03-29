export const creditBands = [
  '1-20 pagine = 4 crediti',
  '21-40 pagine = 7 crediti',
  '41-60 pagine = 10 crediti',
  '61-80 pagine = 13 crediti',
  '81-100 pagine = 16 crediti'
];

export const creditRules = [
  'Elaborazione fallita = 0 crediti',
  'Riapertura di un’analisi esistente = 0 crediti',
  'Caricamento di un nuovo file o di una nuova versione = nuovo consumo crediti',
  'I crediti in abbonamento scadono ogni mese',
  'I crediti dei pack una tantum restano validi 12 mesi'
];

export const includedToday = [
  'Analisi strutturata della perizia',
  'Semaforo rischio',
  'Criticita legali',
  'Costi e oneri da verificare',
  'Ogni segnalazione importante e collegata alle pagine della perizia',
  'Report strutturato per uno screening piu difendibile'
];

export const excludedToday = [
  'Assistente sulla Perizia: non incluso oggi nei pacchetti',
  'Image Forensics: non incluso oggi nei pacchetti',
  'Entrambe le funzioni restano roadmap / in arrivo'
];

export const packageFaq = [
  {
    question: 'Quando vengono scalati i crediti?',
    answer:
      'Il consumo avviene quando viene avviata una nuova elaborazione su un file caricato o su una nuova versione del documento.'
  },
  {
    question: 'Cosa include oggi il piano Free?',
    answer:
      'Include 12 crediti, cioe fino a 3 perizie standard da 1-20 pagine. Le fasce pagina piu alte consumano piu crediti secondo la tabella pubblica.'
  },
  {
    question: 'Cosa succede se l’analisi fallisce?',
    answer:
      'Se l’elaborazione non va a buon fine non viene scalato alcun credito.'
  },
  {
    question: 'Qual e la differenza tra pack una tantum e abbonamento?',
    answer:
      'I crediti in abbonamento hanno cadenza mensile; i crediti acquistati come pack una tantum restano disponibili per 12 mesi.'
  },
  {
    question: 'La riapertura di un’analisi consuma crediti?',
    answer:
      'No. La semplice riapertura di un’analisi esistente non genera nuovo consumo.'
  },
  {
    question: 'Assistente e Image Forensics sono inclusi?',
    answer:
      'No. Oggi il valore dei pacchetti riguarda il core product documentale; Assistente sulla Perizia e Image Forensics restano funzioni in arrivo.'
  }
];
