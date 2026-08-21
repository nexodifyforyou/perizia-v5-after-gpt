import React, { createContext, useContext, useMemo } from 'react';

// ---------------------------------------------------------------------------
// Report-clarity bilingual layer (frontend) — Italian-first + English.
//
// Italian is AUTHORITATIVE and visually primary. English is a smaller/muted
// translation shown directly underneath (never two columns, never duplicated
// cards). It is a PROGRESSIVE ENHANCEMENT: static UI strings resolve from the
// reviewed dictionary below (deterministic, no network); dynamic free-text
// spans resolve from a translation map fetched LAZILY after the Italian report
// renders. English NEVER feeds any tone/status/severity/readiness decision —
// this module only produces display strings.
// ---------------------------------------------------------------------------

// Reviewed static dictionary. Mirrors backend correctness_v2/translation.py
// STATIC_GLOSSARY for the fixed UI chrome (section titles, six-state labels, IA
// headers, field labels). Values are English translations of the exact Italian.
export const STATIC_GLOSSARY = {
  // IA section headers
  'Cosa stai acquistando': 'What you are buying',
  'Nessun blocco automatico rilevato, ma sono presenti informazioni in conflitto da verificare.':
    'No automatic blocking issue was detected, but conflicting information requires verification.',
  'Numeri principali': 'Key figures',
  'Valutazione sintetica': 'Summary assessment',
  'Stato di occupazione': 'Occupancy status',
  'Cosa verificare prima di procedere': 'What to verify before proceeding',
  'Verifiche essenziali prima di procedere': 'Essential checks before proceeding',
  'Altre verifiche': 'Other checks',
  'Conformità e documenti tecnici': 'Compliance and technical documents',
  'Formalità e cancellazioni': 'Encumbrances and cancellations',
  'Altri elementi da conoscere': 'Other things to know',
  'Fonti decisive dalla perizia': 'Key evidence from the appraisal',
  'Conferme fornite dall\'utente': 'Confirmations you provided',
  'Stato delle verifiche': 'Verification status',
  'Incertezze e conflitti': 'Uncertainties and conflicts',
  'Report parziale': 'Partial report',
  // Six state labels
  'Dichiarato dalla perizia': 'Declared by the appraisal',
  'Confermato dall\'utente': 'Confirmed by you',
  'Da verificare': 'To be verified',
  'Da chiarire': 'To be clarified',
  'Non dichiarato': 'Not declared',
  'In conflitto tra le fonti': 'Conflicting between sources',
  'Non determinabile dalla sola perizia': 'Not determinable from the appraisal alone',
  // Conformity/status labels
  'Conforme secondo la perizia': 'Compliant according to the appraisal',
  'Regolarizzabile secondo la perizia': 'Regularizable according to the appraisal',
  'Non conforme secondo la perizia': 'Non-compliant according to the appraisal',
  'Completato': 'Completed',
  'Conferma necessaria': 'Confirmation required',
  'Verifica tecnica richiesta': 'Technical review required',
  'Da rivedere': 'To review',
  'Non sono sicuro': 'I am not sure',
  // Readiness labels
  'Conferme necessarie': 'Confirmations required',
  'Verifiche professionali aperte': 'Open professional checks',
  'Pronto per l\'esportazione': 'Ready for export',
  // Esito wording
  'Nessuna verifica bloccante emersa dalla perizia': 'No blocking issues emerged from the appraisal',
  'Verifiche necessarie prima di procedere': 'Checks required before proceeding',
  'Nessun elemento bloccante': 'No blocking issues',
  'Verifiche necessarie': 'Checks required',
  // Field labels
  'Tribunale': 'Court',
  'Procedura/RGE': 'Procedure/RGE',
  'Lotto': 'Lot',
  'Lotto selezionato': 'Selected lot',
  'Indirizzo': 'Address',
  'Tipologia': 'Property type',
  'Diritto/quota': 'Right/share',
  'Occupazione': 'Occupancy',
  'Stato': 'Status',
  'Perché conta': 'Why it matters',
  'Cosa verificare': 'What to verify',
  'Costo': 'Cost',
  'Tempistica': 'Timing',
  'Totale': 'Total',
  'Importo iscritto': 'Registered amount',
  'Importi da chiarire': 'Amounts to clarify',
  'Costi potenzialmente a carico dell\'acquirente': 'Costs potentially borne by the buyer',
  'Scenari alternativi indicati dalla perizia': 'Alternative scenarios indicated by the appraisal',
  'Prezzo base d\'asta': 'Auction base price',
  'Già cancellata': 'Already cancelled',
  'Da cancellare a cura della procedura': 'To be cancelled by the procedure',
  // Enum-sourced fixed labels (area groups + conflict/incerto topics/reasons).
  // Deterministic — mirrors backend translation.py so they never hit Gemini and
  // stay consistent across every report.
  'Edilizia': 'Building regulations',
  'Catastale': 'Cadastral',
  'Urbanistica': 'Urban planning',
  'Vincoli di edilizia convenzionata o pubblica': 'Subsidised or public-housing restrictions',
  'Corrispondenza catastale/atto': 'Cadastral/deed correspondence',
  'Impianti — gas': 'Systems — gas',
  'Impianti — elettrico': 'Systems — electrical',
  'Impianti': 'Systems',
  'Agibilità/abitabilità': 'Habitability/fitness for use',
  'APE/energia': 'EPC/energy',
  'Elemento segnalato dalla perizia': 'Element flagged by the appraisal',
  'Tipologia dell\'immobile': 'Property type',
  'Valore finale della perizia': 'Final appraisal value',
  'Dato economico': 'Financial datum',
  'Conformità tecnica': 'Technical compliance',
  'Formalità': 'Encumbrances/formalities',
  'Elemento della perizia': 'Element of the appraisal',
  'Le fonti della perizia riportano valori discordanti: è necessaria una verifica.':
    "The appraisal's sources report conflicting values: verification is required.",
  'Possibile sovrapposizione di dati tra lotti diversi: da verificare a quale lotto si riferisce il dato.':
    'Possible overlap of data between different lots: verify which lot the datum refers to.',
  'Elemento discordante tra le fonti della perizia: da verificare.':
    "Conflicting element between the appraisal's sources: to be verified.",
  'La perizia non consente di determinare con certezza lo stato di occupazione dell\'immobile.':
    "The appraisal does not allow the property's occupancy status to be determined with certainty.",
  'La tipologia dell\'immobile non è determinabile con certezza dalla sola perizia.':
    'The property type is not determinable with certainty from the appraisal alone.',
  'Il valore finale non è determinabile con certezza dalla sola perizia.':
    'The final value is not determinable with certainty from the appraisal alone.',
  'Estratto decisivo non disponibile': 'Key excerpt not available',
};

const normalize = (s) => String(s || '').replace(/\s+/g, ' ').trim().toLowerCase();

const STATIC_BY_NORM = Object.fromEntries(
  Object.entries(STATIC_GLOSSARY).map(([k, v]) => [normalize(k), v]),
);

// Context carries the clarity flag + a translate(it) -> en|null function. The
// default (clarity off, tr returns null) makes every consumer render Italian
// only — byte-for-byte today when the backend does not send `clarity_enabled`.
export const ClarityContext = createContext({ clarity: false, tr: () => null });

export const useClarity = () => useContext(ClarityContext);

// Build the translate function from the lazily-fetched dynamic map. Static UI
// strings win first (deterministic); dynamic map covers free-text spans.
export const buildTranslator = (dynamicMap) => (itText) => {
  const key = normalize(itText);
  if (!key) return null;
  if (STATIC_BY_NORM[key]) return STATIC_BY_NORM[key];
  if (dynamicMap && dynamicMap.has(key)) return dynamicMap.get(key) || null;
  return null;
};

export const ClarityProvider = ({ clarity, translations, children }) => {
  const value = useMemo(() => {
    const map = new Map();
    for (const t of Array.isArray(translations) ? translations : []) {
      if (t && t.source && t.en) map.set(normalize(t.source), String(t.en));
    }
    return { clarity: Boolean(clarity), tr: buildTranslator(map) };
  }, [clarity, translations]);
  return <ClarityContext.Provider value={value}>{children}</ClarityContext.Provider>;
};

// Muted English line rendered under an Italian span. Renders nothing when
// clarity is off or no translation is available (Italian stays authoritative).
export const EnUnder = ({ it, className = '' }) => {
  const { clarity, tr } = useClarity();
  if (!clarity) return null;
  const en = tr(it);
  if (!en) return null;
  return (
    <span
      lang="en"
      data-testid="cv2-en"
      className={`mt-0.5 block text-xs italic text-zinc-500 ${className}`}
    >
      {en}
    </span>
  );
};

// Italian primary + optional English underneath, for a text span.
export const Bi = ({ it, as: Tag = 'span', className = '', enClassName = '' }) => {
  if (it === null || it === undefined || it === '') return null;
  return (
    <Tag className={className}>
      <span lang="it">{it}</span>
      <EnUnder it={it} className={enClassName} />
    </Tag>
  );
};
