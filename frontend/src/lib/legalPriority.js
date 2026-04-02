const toComparableText = (value) =>
  String(value ?? '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();

export const LEGAL_KIND_BY_CATEGORY = {
  occupazione: 'material_blocker',
  pignoramento_esecuzione: 'execution_context',
  ipoteca_formalita: 'material_blocker',
  difformita_urb_cat: 'material_blocker',
  agibilita_docs: 'caution_watch',
  accesso_vincolo: 'caution_watch',
  servitu_usi_civici: 'background_note'
};

export const LEGAL_CATEGORY_LABELS = {
  occupazione: 'Occupazione',
  pignoramento_esecuzione: 'Pignoramento / Esecuzione',
  ipoteca_formalita: 'Ipoteca / Formalità pregiudizievoli',
  difformita_urb_cat: 'Difformità urbanistico-catastali',
  agibilita_docs: 'Agibilità / documentazione tecnica critica',
  accesso_vincolo: 'Accesso / vincolo da verificare',
  servitu_usi_civici: 'Servitù / usi civici'
};

export const LEGAL_KIND_RANK = {
  material_blocker: 0,
  caution_watch: 1,
  execution_context: 2,
  background_note: 3,
  neutral_truth: 4
};

export const TOP_LEVEL_ATTENTION_KINDS = ['material_blocker', 'caution_watch', 'execution_context'];

export const canonicalizeAttentionFieldKey = (key, title = '', detail = '') => {
  const seed = toComparableText(`${key} ${title} ${detail}`);
  if (/(regolarita_urbanistica|urbanistica)/.test(seed)) return 'regolarita_urbanistica';
  if (/(conformita_catastale|catasto|catastale)/.test(seed)) return 'conformita_catastale';
  if (/(stato_occupativo|occupazione|occupancy|opponibilita_occupazione)/.test(seed)) return 'stato_occupativo';
  if (/(agibilita|abitabilita|agibilita_docs)/.test(seed)) return 'agibilita';
  if (/(pignorament|esecuzione immobiliare)/.test(seed)) return 'pignoramento_esecuzione';
  if (/(ipotec|formalit|trascrizion)/.test(seed)) return 'ipoteca_formalita';
  if (/(accesso|mappal|atto di vincolo|vincoli ancora vigenti)/.test(seed)) return 'accesso_vincolo';
  if (/(servitu|usi civici|censo|livello)/.test(seed)) return 'servitu_usi_civici';
  return key || '';
};

export const inferCanonicalLegalCategory = ({ key = '', title = '', detail = '', evidenceText = '' }) => {
  const primaryText = toComparableText(`${title} ${detail}`);
  const fallbackText = toComparableText(evidenceText);
  const texts = [primaryText, fallbackText].filter(Boolean);
  for (const text of texts) {
    if (/(occupat|debitore|coniuge|opponibil)/.test(text)) return 'occupazione';
    if (/(pignorament|esecuzione immobiliare)/.test(text)) return 'pignoramento_esecuzione';
    if (/(ipotec|formalit|trascrizion)/.test(text)) return 'ipoteca_formalita';
    if (/(difform|catast|urbanistic|regolarit)/.test(text)) return 'difformita_urb_cat';
    if (/(agibil|abitabil|ape|impiant|documentaz)/.test(text)) return 'agibilita_docs';
    if (/(accesso|mappal|atto di vincolo|vincoli ancora vigenti|a carico del proprietario)/.test(text)) return 'accesso_vincolo';
    if (/(servitu|usi civici|censo|livello)/.test(text)) return 'servitu_usi_civici';
  }
  const canonicalKey = canonicalizeAttentionFieldKey(key, title, detail);
  if (canonicalKey === 'regolarita_urbanistica' || canonicalKey === 'conformita_catastale') return 'difformita_urb_cat';
  if (canonicalKey === 'stato_occupativo') return 'occupazione';
  if (canonicalKey === 'agibilita') return 'agibilita_docs';
  if (LEGAL_KIND_BY_CATEGORY[canonicalKey]) return canonicalKey;
  return null;
};

export const isPositiveOrNeutralLegalTruth = (key, detail) => {
  const canonicalKey = canonicalizeAttentionFieldKey(key, '', detail);
  const text = toComparableText(detail);
  if (!text) return false;
  if (canonicalKey === 'regolarita_urbanistica') {
    if (/non emergono abusi/.test(text)) return true;
    return /(conforme|regolare)/.test(text) && !/(non conform|abus|difform|irregolar)/.test(text);
  }
  if (canonicalKey === 'conformita_catastale') {
    return /(conforme|regolare)/.test(text) && !/(non conform|difform|irregolar)/.test(text);
  }
  if (canonicalKey === 'stato_occupativo') {
    return /(libero|non occupato|disponibile)/.test(text) && !/(occupato|locato|opponibil|detent)/.test(text);
  }
  if (canonicalKey === 'agibilita') {
    return /(presente|rilasciat|agibil|abitabil)/.test(text) && !/(non|assen|manc|irregolar)/.test(text);
  }
  return false;
};

export const isWeakBackgroundLegalSummary = (textRaw) => {
  const text = toComparableText(textRaw);
  if (!text) return false;
  return /(servitu|usi civici|censo|livello|vincolo|contesto esecutivo|procedura esecutiva)/.test(text);
};

export const buildCanonicalLegalPriorityMeta = ({ key = '', title = '', detail = '', evidenceText = '' }) => {
  const canonicalKey = canonicalizeAttentionFieldKey(key, title, detail);
  const category = inferCanonicalLegalCategory({ key: canonicalKey, title, detail, evidenceText });
  const kind = isPositiveOrNeutralLegalTruth(canonicalKey, detail)
    ? 'neutral_truth'
    : (LEGAL_KIND_BY_CATEGORY[category] || 'background_note');
  return {
    canonicalKey,
    category,
    kind,
    semanticKey: `${category || canonicalKey || 'uncategorized'}|${toComparableText(detail) || toComparableText(title)}`
  };
};

export const pickCanonicalTopAttentionItem = (items = []) => {
  const normalized = Array.isArray(items) ? items.filter((item) => item && typeof item === 'object') : [];
  const eligible = normalized.filter((item) => TOP_LEVEL_ATTENTION_KINDS.includes(item.kind));
  if (eligible.length === 0) return null;
  return [...eligible].sort((a, b) => {
    const rankDelta = (LEGAL_KIND_RANK[a.kind] ?? 9) - (LEGAL_KIND_RANK[b.kind] ?? 9);
    if (rankDelta !== 0) return rankDelta;
    const scoreA = Number.isFinite(a.decisionScore) ? a.decisionScore : -1;
    const scoreB = Number.isFinite(b.decisionScore) ? b.decisionScore : -1;
    if (scoreA !== scoreB) return scoreB - scoreA;
    return 0;
  })[0] || null;
};

export const getCanonicalTopAttentionItems = (items = []) => {
  const normalized = Array.isArray(items) ? items.filter((item) => item && typeof item === 'object') : [];
  const eligible = normalized.filter((item) => TOP_LEVEL_ATTENTION_KINDS.includes(item.kind));
  if (eligible.length === 0) return [];
  const topRank = eligible.reduce((best, item) => Math.min(best, LEGAL_KIND_RANK[item.kind] ?? 9), 9);
  return [...eligible]
    .filter((item) => (LEGAL_KIND_RANK[item.kind] ?? 9) === topRank)
    .sort((a, b) => {
      const scoreA = Number.isFinite(a.decisionScore) ? a.decisionScore : -1;
      const scoreB = Number.isFinite(b.decisionScore) ? b.decisionScore : -1;
      if (scoreA !== scoreB) return scoreB - scoreA;
      const labelA = String(a.killer || a.title || '').trim();
      const labelB = String(b.killer || b.title || '').trim();
      return labelA.localeCompare(labelB, 'it');
    });
};

export const getCanonicalTopAttentionText = ({ topAttentionItem = null, primaryText = '', fallbackText = '' }) => {
  const primary = String(primaryText ?? '').trim();
  const fallback = String(fallbackText ?? '').trim();
  if (topAttentionItem) {
    return String(topAttentionItem.killer || topAttentionItem.title || primary || fallback).trim();
  }
  if (isWeakBackgroundLegalSummary(primary) && fallback) return fallback;
  return primary || fallback;
};
