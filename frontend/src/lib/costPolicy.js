const MISSING_TEXT = 'Non specificato in perizia';

const safeRender = (value, fallback = MISSING_TEXT) => {
  if (value === null || value === undefined) return fallback;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return fallback;
    const upper = trimmed.toUpperCase();
    if (['NONE', 'N/A', 'NOT_SPECIFIED', 'NOT_SPECIFIED_IN_PERIZIA', 'UNKNOWN', 'TBD', 'NULL'].includes(upper)) {
      return fallback;
    }
    return trimmed;
  }
  if (typeof value === 'number') return Number.isFinite(value) ? `${value}` : fallback;
  if (typeof value === 'boolean') return value ? 'Si' : 'No';
  if (Array.isArray(value)) {
    const parts = value.map((item) => safeRender(item, '')).filter(Boolean);
    return parts.length > 0 ? parts.join(', ') : fallback;
  }
  if (typeof value === 'object') {
    if (value.formatted !== undefined) return safeRender(value.formatted, fallback);
    if (value.value !== undefined) return safeRender(value.value, fallback);
    if (value.status_it !== undefined) return safeRender(value.status_it, fallback);
    if (value.status !== undefined) return safeRender(value.status, fallback);
    if (value.label_it !== undefined) return safeRender(value.label_it, fallback);
  }
  return `${value}` || fallback;
};

const normalizeComparableText = (value) => safeRender(value, '')
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .replace(/\s+/g, ' ')
  .trim()
  .toLowerCase();

const parseNumericEuro = (value) => {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value !== 'string') return null;
  const cleaned = value.replace(/[^\d,.-]/g, '').trim();
  if (!cleaned) return null;
  const lastComma = cleaned.lastIndexOf(',');
  const lastDot = cleaned.lastIndexOf('.');
  const decimalIndex = Math.max(lastComma, lastDot);
  if (decimalIndex === -1) {
    const parsed = Number.parseFloat(cleaned);
    return Number.isFinite(parsed) ? parsed : null;
  }
  const decimalDigits = cleaned.slice(decimalIndex + 1).replace(/[^\d]/g, '');
  const hasExplicitDecimal = decimalDigits.length > 0 && decimalDigits.length <= 2;
  const decimalSeparator = decimalIndex === lastComma ? ',' : '.';
  const thousandsSeparator = decimalSeparator === ',' ? '.' : ',';
  const normalized = hasExplicitDecimal
    ? `${cleaned.slice(0, decimalIndex).replace(new RegExp(`\\${thousandsSeparator}`, 'g'), '')}.${decimalDigits}`
    : cleaned.replace(/[.,]/g, '');
  const parsed = Number.parseFloat(normalized);
  return Number.isFinite(parsed) ? parsed : null;
};

const getEvidence = (obj) => {
  if (!obj) return [];
  if (Array.isArray(obj)) return obj.filter((item) => item && typeof item === 'object');
  if (Array.isArray(obj.evidence)) return obj.evidence.filter((item) => item && typeof item === 'object');
  return [];
};

const mergeEvidence = (...lists) => {
  const out = [];
  const seen = new Set();
  lists.forEach((list) => {
    const evidence = getEvidence(list);
    evidence.forEach((entry) => {
      const key = `${safeRender(entry?.page, '')}|${safeRender(entry?.quote, '').slice(0, 160)}|${safeRender(entry?.search_hint, '').slice(0, 80)}`;
      if (seen.has(key)) return;
      seen.add(key);
      out.push(entry);
    });
  });
  return out.slice(0, 2);
};

const getItemEvidence = (item) => mergeEvidence(item?.fonte_perizia, item?.evidence, item);

const extractEuroAmounts = (text) => {
  const raw = String(text || '');
  const matches = [];
  const patterns = [/€\s*([0-9][0-9\.\,\s]{0,18})/g, /([0-9][0-9\.\,\s]{0,18})\s*€/g];
  patterns.forEach((pattern) => {
    for (const match of raw.matchAll(pattern)) {
      const parsed = parseNumericEuro(match[1]);
      if (parsed !== null && parsed > 0) matches.push(parsed);
    }
  });
  return [...new Set(matches.map((value) => Math.round(value * 100) / 100))];
};

const isPositiveOrNeutralNonCost = (textRaw) => {
  const text = normalizeComparableText(textRaw);
  if (!text) return false;
  if (/(non emergono abusi|conforme|regolare|libero|non occupato|disponibile|agibile|ape presente|documentazione completa)/.test(text)) return true;
  return /(non sono presenti|assenza di|nessuna occorrenza)/.test(text) && /(vincoli|usi civici|spese|formalita|ipoteca|pignoramento)/.test(text);
};

const isValuationOrLegalOnlyContext = (textRaw) => {
  const text = normalizeComparableText(textRaw);
  if (!text) return false;
  if (/(prezzo base|valore di stima|valore finale|deprezzament|euro\/mq|rendita|reg gen|reg part|importo:|ipoteca|mutuo fondiario|pignoramento|formalita a carico)/.test(text)) return true;
  return false;
};

const isBuyerBurdenText = (textRaw) => {
  const text = normalizeComparableText(textRaw);
  if (!text || isPositiveOrNeutralNonCost(text) || isValuationOrLegalOnlyContext(text)) return false;
  return /(regolarizzazione urbanistica|oneri di regolarizzazione|spesa per regolarizzazione|sanatoria|condono|pratica edilizia da regolarizzare|abuso da sanare|spese tecniche|oneri tecnici|istruttoria|spese condominiali|morosita condominiale|debito condominiale|insoluti condominiali|arretrati verso il condominio|liberazione|rilascio|sgombero|costi di liberazione|spese per liberazione|allineamento catastal|aggiornamento catastal|variazione catastal|regolarizzazione catastal|ripristin|messa in sicurezza|bonifica|amianto|fibro cemento|completamento lavori)/.test(text);
};

const isExplicitBuyerCost = (item) => {
  const amount = parseNumericEuro(item?.stima_euro);
  if (amount === null || amount <= 0) return false;
  const evidence = getItemEvidence(item);
  if (evidence.length === 0) return false;
  const textBlob = [
    safeRender(item?.label_it || item?.label || item?.voce, ''),
    safeRender(item?.stima_nota, ''),
    ...evidence.map((ev) => `${safeRender(ev?.quote, '')} ${safeRender(ev?.search_hint, '')}`)
  ].join(' ');
  if (!isBuyerBurdenText(textBlob)) return false;
  const amountsInText = extractEuroAmounts(textBlob);
  return amountsInText.length === 1;
};

const formatMoney = (value) => {
  const numeric = parseNumericEuro(value);
  if (numeric === null) return MISSING_TEXT;
  return `€ ${new Intl.NumberFormat('it-IT', { maximumFractionDigits: 0, minimumFractionDigits: 0, useGrouping: true }).format(Math.round(numeric))}`;
};

const buildScopedLabel = (item) => {
  const lotNumber = safeRender(item?.lot_number, '');
  const beneNumber = safeRender(item?.bene_number, '');
  const label = safeRender(item?.label_it || item?.label || item?.voce || item?.title, '');
  const scope = [
    lotNumber ? `Lotto ${lotNumber}` : '',
    beneNumber ? `Bene ${beneNumber}` : ''
  ].filter(Boolean).join(' - ');
  return [scope, label].filter(Boolean).join(' - ');
};

const collectQualitativeCandidates = (moneyBox) => {
  const root = Array.isArray(moneyBox?.qualitative_burdens) ? moneyBox.qualitative_burdens : [];
  const lotItems = Array.isArray(moneyBox?.lots)
    ? moneyBox.lots.flatMap((lot) => {
        const items = Array.isArray(lot?.items) ? lot.items : (Array.isArray(lot?.burdens) ? lot.burdens : []);
        return items.map((item) => ({ ...item, lot_number: item?.lot_number || lot?.lot_number }));
      })
    : [];
  return [...root, ...lotItems];
};

export const buildCustomerCostPolicy = (result) => {
  const moneyBox = result?.section_3_money_box?.items ? result.section_3_money_box : (result?.money_box || {});
  const valuation = result?.panoramica_contract?.valuation_waterfall || {};
  const rawItems = Array.isArray(moneyBox?.items) ? moneyBox.items.filter((item) => item && typeof item === 'object') : [];

  const explicitBuyerCosts = [];
  const explicitSeen = new Set();
  rawItems.forEach((item, index) => {
    if (!isExplicitBuyerCost(item)) return;
    const amount = parseNumericEuro(item?.stima_euro);
    const label = buildScopedLabel(item);
    const key = `${label}|${Math.round(amount || 0)}`;
    if (explicitSeen.has(key)) return;
    explicitSeen.add(key);
    explicitBuyerCosts.push({
      ...item,
      __policy_key: `explicit-${index}`,
      __policy_label: label,
      __policy_amount: amount,
      __policy_note: 'Costo buyer-side esplicitamente quantificato nella perizia.',
      __policy_evidence: getItemEvidence(item),
    });
  });

  const groundedUnquantifiedBurdens = [];
  const burdenSeen = new Set();
  collectQualitativeCandidates(moneyBox).forEach((item, index) => {
    const evidence = mergeEvidence(item?.evidence, item);
    const textBlob = [
      buildScopedLabel(item),
      safeRender(item?.stima_nota || item?.note_it || item?.note || item?.detail || item?.burden_type, ''),
      ...evidence.map((ev) => `${safeRender(ev?.quote, '')} ${safeRender(ev?.search_hint, '')}`)
    ].join(' ');
    if (!evidence.length || !isBuyerBurdenText(textBlob)) return;
    const label = buildScopedLabel(item);
    if (!label) return;
    const key = normalizeComparableText(label);
    if (burdenSeen.has(key)) return;
    burdenSeen.add(key);
    groundedUnquantifiedBurdens.push({
      key: `burden-${index}`,
      label,
      note: 'Onere buyer-side grounded in perizia, non quantificato in modo difendibile.',
      evidence,
    });
  });

  const explicitTotal = explicitBuyerCosts.reduce((sum, item) => sum + (item.__policy_amount || 0), 0);
  let totalSummary;
  if (explicitBuyerCosts.length > 0) {
    totalSummary = {
      kind: 'explicit_total',
      text: formatMoney(explicitTotal),
      note: groundedUnquantifiedBurdens.length > 0
        ? 'Totale di soli costi buyer-side esplicitamente supportati; oneri non quantificati esclusi dal totale.'
        : 'Totale di soli costi buyer-side esplicitamente supportati.',
    };
  } else if (groundedUnquantifiedBurdens.length > 0) {
    totalSummary = {
      kind: 'non_quantified',
      text: 'NON QUANTIFICATO IN PERIZIA',
      note: 'Sono presenti oneri buyer-side grounded, ma la perizia non supporta un totale numerico difendibile.',
    };
  } else {
    totalSummary = {
      kind: 'none',
      text: 'Nessun costo extra lato acquirente difendibile rilevato nella perizia.',
      note: 'Deprezzamenti e segnali legali restano separati dalle superfici costi buyer-side.',
    };
  }

  return {
    valuationAdjustments: {
      amount: parseNumericEuro(valuation?.deprezzamenti_eur) !== null
        ? formatMoney(valuation?.deprezzamenti_eur)
        : '',
      amountNumeric: parseNumericEuro(valuation?.deprezzamenti_eur),
      evidence: getEvidence(valuation?.evidence?.deprezzamenti_eur),
      note: 'Deprezzamento di perizia: non equivale automaticamente a cassa extra lato acquirente.',
    },
    explicitBuyerCosts,
    groundedUnquantifiedBurdens,
    totalSummary,
  };
};
