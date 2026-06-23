// Customer-facing verification-task model.
//
// Turns a critical / attention signal (a topic + the perito's finding + the
// pages where the evidence lives) into an ACTIONABLE task that tells the
// customer:
//   1. what exactly must be verified,
//   2. why it matters,
//   3. on which page(s) the perizia contains the evidence,
//   4. who should verify it (tecnico, geometra, legale, custode, Comune, ...),
//   5. how urgent it is (prima dell'offerta / prima del saldo / da monitorare).
//
// A bare "Controllare p.X" is intentionally NOT a valid task: every task always
// carries a what/why so the customer understands the action, not just a page.

// Ordered list: the first profile whose `match` hits the topic+text wins.
const TOPIC_PROFILES = [
  {
    match: /urbanist|\babus|difform(?:it|i)\w*\s*urban|sanator|condon|sanabil/,
    title: 'Regolarità urbanistica e commerciabilità',
    what: 'Verificare se la non conformità indicata in perizia è sanabile e se limita la commerciabilità dell’immobile.',
    why: 'Può incidere sulla possibilità di rivendere, finanziare o regolarizzare il bene.',
    who: 'Tecnico/geometra + ufficio tecnico comunale',
    urgency: 'prima dell’offerta',
  },
  {
    match: /catastal/,
    title: 'Conformità catastale',
    what: 'Verificare la corrispondenza tra stato di fatto, planimetrie e dati catastali e l’eventuale costo di allineamento.',
    why: 'Le difformità catastali possono richiedere aggiornamenti a carico dell’acquirente e incidere sul rogito.',
    who: 'Geometra/tecnico',
    urgency: 'prima dell’offerta',
  },
  {
    match: /occupat|opponibil|possesso|liberazion|sgombero|rilascio|inquilin|conduttore/,
    title: 'Stato di occupazione e tempi di liberazione',
    what: 'Verificare chi occupa l’immobile, con quale titolo, e i tempi/costi stimabili di liberazione.',
    why: 'Può incidere su disponibilità effettiva, tempi di possesso e costi pratici.',
    who: 'Custode/delegato + legale',
    urgency: 'prima dell’offerta',
  },
  {
    match: /agibil|abitabil/,
    title: 'Agibilità e abitabilità',
    what: 'Verificare la presenza del certificato di agibilità e le eventuali condizioni o costi per ottenerlo.',
    why: 'L’assenza di agibilità può limitare l’uso, la rivendita e la finanziabilità del bene.',
    who: 'Tecnico/geometra + ufficio tecnico comunale',
    urgency: 'prima dell’offerta',
  },
  {
    match: /\bape\b|prestazione\s+energetic/,
    title: 'Prestazione energetica (APE)',
    what: 'Verificare la presenza e la validità dell’APE e l’eventuale costo di aggiornamento.',
    why: 'L’APE è obbligatorio per la vendita e incide sugli obblighi documentali.',
    who: 'Tecnico/certificatore energetico',
    urgency: 'da monitorare',
  },
  {
    match: /formalit|ipotec|trascrizion|cancellazion|gravam/,
    title: 'Formalità e cancellazioni',
    what: 'Verificare nel decreto/avviso quali formalità vengono cancellate e quali, se presenti, restano opponibili.',
    why: 'Non tutte le somme indicate in perizia sono costi dell’acquirente.',
    who: 'Delegato alla vendita / legale',
    urgency: 'prima del saldo',
  },
  {
    match: /pignorament|esecuzion|\bprocedur/,
    title: 'Stato della procedura',
    what: 'Verificare nel fascicolo della procedura lo stato di pignoramento/esecuzione e i suoi effetti sulla vendita.',
    why: 'Definisce cosa viene cancellato con il decreto di trasferimento e cosa resta a carico dell’acquirente.',
    who: 'Delegato alla vendita / legale',
    urgency: 'prima dell’offerta',
  },
  {
    match: /spese\s+condominial|condominio|condominial|arretrat|moros/,
    title: 'Spese condominiali arretrate',
    what: 'Verificare l’importo delle spese condominiali arretrate e quali competono all’aggiudicatario.',
    why: 'L’aggiudicatario può rispondere delle spese dell’anno in corso e di quello precedente.',
    who: 'Amministratore di condominio + delegato',
    urgency: 'prima del saldo',
  },
  {
    match: /servit|\bvincol|usi civici|censo|livello/,
    title: 'Servitù e vincoli',
    what: 'Verificare l’esistenza e la portata di servitù, vincoli o usi civici che gravano sul bene.',
    why: 'Possono limitare l’uso del bene o imporre oneri non immediatamente evidenti.',
    who: 'Tecnico/geometra + legale',
    urgency: 'prima dell’offerta',
  },
  {
    match: /\bquota\b|compropriet|diritto\s+reale/,
    title: 'Quota e diritto reale',
    what: 'Verificare quale quota e quale diritto reale sono posti in vendita.',
    why: 'L’acquisto di una quota parziale può limitare il pieno godimento del bene.',
    who: 'Legale',
    urgency: 'prima dell’offerta',
  },
  {
    match: /\bcost\w*|\boner\w*|importi|spese|deprezzam|stima|economic|debenz/,
    title: 'Importi economici da verificare',
    what: 'Verificare quali importi sono già scontati nella stima e quali possono restare a carico dell’aggiudicatario.',
    why: 'Evita di sommare due volte deprezzamenti già inclusi nel prezzo o di ignorare spese future reali.',
    who: 'Geometra/consulente aste',
    urgency: 'prima dell’offerta',
  },
  {
    match: /leggibil|illeggibil|document\w*\s*(?:non|incompl|illeggibil)|unreadable|coeren/,
    title: 'Leggibilità e completezza documentale',
    what: 'Verificare manualmente nella perizia originale i punti che l’analisi automatica non ha potuto leggere con certezza.',
    why: 'Dati documentali non coerenti o illeggibili possono nascondere criticità rilevanti.',
    who: 'Tecnico/geometra',
    urgency: 'prima dell’offerta',
  },
];

const DEFAULT_PROFILE = {
  title: 'Punto da verificare prima dell’offerta',
  what: 'Verificare nella perizia il punto segnalato e i suoi effetti pratici ed economici.',
  why: 'Può incidere sulla convenienza o sulla fattibilità dell’acquisto.',
  who: 'Tecnico/geometra',
  urgency: 'prima dell’offerta',
};

const profileForTopic = (topic, text = '') => {
  const blob = `${topic || ''} ${text || ''}`.toLowerCase();
  return TOPIC_PROFILES.find((p) => p.match.test(blob)) || DEFAULT_PROFILE;
};

const normalizePages = (pages) => {
  if (!Array.isArray(pages)) return [];
  return [...new Set(pages.map((p) => Number(p)).filter((p) => Number.isFinite(p) && p > 0))]
    .sort((a, b) => a - b);
};

const pagesFromEvidence = (evidence) => {
  if (!Array.isArray(evidence)) return [];
  return normalizePages(evidence.map((e) => Number(e?.page)));
};

// Accepts either a {topic, text, pages} row (from the live page) or a generic
// {title, detail, evidence} item (from the print model) and normalizes it.
const toRow = (item = {}) => {
  const topic = String(item.topic || item.title || '').trim();
  const text = String(item.text || item.detail || item.title || '').trim();
  const pages = Array.isArray(item.pages) && item.pages.length > 0
    ? normalizePages(item.pages)
    : pagesFromEvidence(item.evidence);
  return { topic, text, pages };
};

// Some upstream topic labels are generic placeholders; never surface them as a
// task title — fall back to the profile title instead.
const GENERIC_TOPIC_RE = /^(voce critica|da verificare|punto critico|non specificat\w*)$/i;

// Build a single actionable verification task from a criticità/attention row.
export const buildVerificationTask = (item = {}) => {
  const { topic, text, pages } = toRow(item);
  const profile = profileForTopic(topic, text);
  const title = topic && !GENERIC_TOPIC_RE.test(topic) ? topic : profile.title;
  // Preserve the perito's own finding as supporting context when it adds signal
  // beyond the generic topic label (and is not a bare page reference).
  const cleanText = text.replace(/\bcontrollare\s+p\.?\s*[\d,\s]+\.?/gi, '').trim();
  const detail = cleanText && cleanText.toLowerCase() !== title.toLowerCase() && cleanText.length > 8
    ? text
    : '';
  return {
    title_it: title,
    what_to_verify_it: profile.what,
    why_it_matters_it: profile.why,
    pages,
    who_should_verify_it: profile.who,
    urgency: profile.urgency,
    detail_it: detail,
  };
};

// A task is "sufficient" only if it tells the customer what + why, not just a
// page reference. Used by tests and as a defensive guard.
export const isSufficientVerificationTask = (task) => Boolean(
  task
  && task.what_to_verify_it && task.what_to_verify_it.trim().length > 12
  && task.why_it_matters_it && task.why_it_matters_it.trim().length > 12
  && !/^controllare\s+p\.?\s*\d/i.test((task.what_to_verify_it || '').trim()),
);

// Build a deduped list of verification tasks from a set of criticità/attention
// items (rows or generic items) plus optional synthetic extra items.
export const buildVerificationTasks = (items = [], { extras = [] } = {}) => {
  const all = [...(Array.isArray(items) ? items : []), ...(Array.isArray(extras) ? extras : [])];
  const byTitle = new Map();
  for (const item of all) {
    if (!item) continue;
    const task = buildVerificationTask(item);
    if (!isSufficientVerificationTask(task)) continue;
    const key = task.title_it.toLowerCase();
    const existing = byTitle.get(key);
    if (!existing) {
      byTitle.set(key, { ...task, pages: [...task.pages] });
      continue;
    }
    existing.pages = normalizePages([...existing.pages, ...task.pages]);
    if (!existing.detail_it && task.detail_it) existing.detail_it = task.detail_it;
  }
  return Array.from(byTitle.values());
};

// Synthetic seed for a generic "money to verify" task when the Money Map carries
// signals to verify but no explicit criticità row covers costs.
export const buildCostVerificationSeed = (pages = []) => ({
  topic: 'Importi economici da verificare',
  text: 'Importi economici da verificare prima dell’offerta.',
  pages: normalizePages(pages),
});

// Synthetic seed for a "formalità / cancellazioni" task driven by procedural
// amounts (ipoteche, pignoramenti, formalità) present in the Money Map.
export const buildFormalitiesVerificationSeed = (pages = []) => ({
  topic: 'Formalità e cancellazioni',
  text: 'Formalità e importi procedurali da verificare nel decreto/avviso di vendita.',
  pages: normalizePages(pages),
});

export const __test__ = { profileForTopic, toRow, TOPIC_PROFILES, DEFAULT_PROFILE };
