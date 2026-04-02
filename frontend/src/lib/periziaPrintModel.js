import { parseSurfaceNumber } from './surfaceFormatting';
import {
  buildCanonicalLegalPriorityMeta,
  getCanonicalTopAttentionItems,
  isPositiveOrNeutralLegalTruth,
  isWeakBackgroundLegalSummary,
  LEGAL_KIND_RANK,
  pickCanonicalTopAttentionItem
} from './legalPriority';
import { buildCustomerCostPolicy } from './costPolicy';

const MISSING_TEXT = 'Non specificato in perizia';
const EURO_FORMATTER = new Intl.NumberFormat('it-IT', {
  maximumFractionDigits: 0,
  minimumFractionDigits: 0,
  useGrouping: 'always',
});
const AREA_FORMATTER = new Intl.NumberFormat('it-IT', {
  maximumFractionDigits: 2,
  minimumFractionDigits: 0,
  useGrouping: 'always',
});

export const normalizeAnalysisResponse = (payload) => {
  if (!payload || typeof payload !== 'object') return payload;
  if (payload.result) return { ...payload, __result_path: 'result' };

  const candidates = [
    payload.analysis,
    payload.data,
    payload.payload,
    payload.analysis?.data,
    payload.analysis?.payload,
    payload.data?.analysis,
  ].filter(Boolean);

  for (const candidate of candidates) {
    if (candidate && typeof candidate === 'object' && candidate.result) {
      return { ...payload, ...candidate, result: candidate.result };
    }
  }

  if (payload.analysis && typeof payload.analysis === 'object') {
    return { ...payload, ...payload.analysis };
  }
  if (payload.data && typeof payload.data === 'object') {
    return { ...payload, ...payload.data };
  }
  return payload;
};

const normalizePlaceholder = (value) => {
  if (value === null || value === undefined) return MISSING_TEXT;
  if (typeof value === 'string') {
    const upper = value.trim().toUpperCase();
    if (!upper || ['NONE', 'N/A', 'NOT_SPECIFIED', 'NOT_SPECIFIED_IN_PERIZIA', 'UNKNOWN', 'TBD', 'NULL'].includes(upper)) {
      return MISSING_TEXT;
    }
  }
  return value;
};

export const safeRender = (value, fallback = MISSING_TEXT) => {
  const normalized = normalizePlaceholder(value);
  if (normalized === MISSING_TEXT) return fallback;
  if (typeof normalized === 'string') return normalized;
  if (typeof normalized === 'number') return `${normalized}`;
  if (typeof normalized === 'boolean') return normalized ? 'Si' : 'No';
  if (Array.isArray(normalized)) {
    const parts = normalized.map((item) => safeRender(item, '')).filter(Boolean);
    return parts.length ? parts.join(', ') : fallback;
  }
  if (typeof normalized === 'object') {
    if (normalized.formatted !== undefined) return safeRender(normalized.formatted, fallback);
    if (normalized.value !== undefined) return safeRender(normalized.value, fallback);
    if (normalized.status_it !== undefined) return safeRender(normalized.status_it, fallback);
    if (normalized.status !== undefined) return safeRender(normalized.status, fallback);
    if (normalized.label_it !== undefined) return safeRender(normalized.label_it, fallback);
    if (normalized.full !== undefined) return safeRender(normalized.full, fallback);
    return fallback;
  }
  return `${normalized}` || fallback;
};

export const parseNumericEuro = (value) => {
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

const formatMoney = (value) => {
  const numeric = parseNumericEuro(value);
  if (numeric === null) return safeRender(value, MISSING_TEXT);
  return `€ ${EURO_FORMATTER.format(Math.round(numeric))}`;
};

const getEvidence = (obj) => {
  if (!obj) return [];
  if (Array.isArray(obj)) return obj.filter((item) => item && typeof item === 'object');
  if (Array.isArray(obj.evidence)) return obj.evidence;
  return [];
};

const getPrimaryEvidence = (...sources) => {
  for (const source of sources) {
    const evidence = getEvidence(source);
    if (evidence.length > 0) return evidence.slice(0, 2);
  }
  return [];
};

const pickFirstNonEmpty = (...values) => {
  for (const value of values) {
    if (value !== null && value !== undefined && value !== '') return value;
  }
  return null;
};

const isMeaningfulValue = (value) => {
  if (value === null || value === undefined) return false;
  if (typeof value === 'string') {
    const upper = value.trim().toUpperCase();
    return Boolean(
      upper &&
      !['NONE', 'N/A', 'NOT_SPECIFIED', 'NOT_SPECIFIED_IN_PERIZIA', 'UNKNOWN', 'TBD', 'NULL', MISSING_TEXT.toUpperCase()].includes(upper)
    );
  }
  if (Array.isArray(value)) return value.some((item) => isMeaningfulValue(item));
  if (typeof value === 'object') {
    return [
      value?.detail_it,
      value?.status_it,
      value?.status,
      value?.formatted,
      value?.value,
      value?.label_it,
      value?.full
    ].some((item) => isMeaningfulValue(item));
  }
  return true;
};

const getFieldStateValue = (state) => {
  if (!state || typeof state !== 'object') return null;
  if (isMeaningfulValue(state?.detail_it)) return state.detail_it;
  if (isMeaningfulValue(state?.status_it)) return state.status_it;
  if (isMeaningfulValue(state?.formatted)) return state.formatted;
  if (isMeaningfulValue(state?.value?.detail_it)) return state.value.detail_it;
  if (isMeaningfulValue(state?.value?.status_it)) return state.value.status_it;
  if (isMeaningfulValue(state?.value?.formatted)) return state.value.formatted;
  if (state.value !== null && state.value !== undefined && state.value !== '') return state.value;
  return null;
};

const getLegacyDetailValue = (value) => {
  if (!value || typeof value !== 'object') return null;
  return pickFirstNonEmpty(value?.detail_it, value?.status_it, value?.status, value?.formatted, value?.value);
};

const getRichDisplayValue = (state, legacyValue, ...fallbacks) =>
  pickFirstNonEmpty(getFieldStateValue(state), getLegacyDetailValue(legacyValue), ...fallbacks);

const formatSurfaceValue = (value) => {
  if (!isMeaningfulValue(value)) return '';
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    const measured = pickFirstNonEmpty(
      value?.formatted,
      value?.value,
      value?.detail_it,
      value?.status_it,
      value?.status
    );
    const unit = safeRender(pickFirstNonEmpty(value?.unit, value?.uom), '').trim() || 'mq';
    const numeric = parseSurfaceNumber(safeRender(measured, ''));
    if (numeric !== null) return `${AREA_FORMATTER.format(numeric)} ${unit}`.trim();
    return safeRender(measured, '').trim();
  }
  const rendered = safeRender(value, '').trim();
  const numeric = parseSurfaceNumber(rendered);
  if (numeric === null) return rendered;
  const explicitUnit = /m²/i.test(rendered) ? 'm²' : (/\bmq\b/i.test(rendered) ? 'mq' : 'mq');
  return `${AREA_FORMATTER.format(numeric)} ${explicitUnit}`.trim();
};

const escapeRegExp = (value) => String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const splitQuotaFromDiritto = (dirittoValue, quotaValue) => {
  const diritto = safeRender(dirittoValue, '').trim();
  const quota = safeRender(quotaValue, '').trim();
  if (!diritto) return '';
  if (!quota) return diritto;
  const cleaned = diritto
    .replace(new RegExp(`(?:quota\\s*)?${escapeRegExp(quota)}`, 'ig'), '')
    .replace(/[|,;:/-]+$/g, '')
    .trim();
  return cleaned || diritto;
};

const getFieldStateEvidence = (state, fallback = null) => {
  if (state && Array.isArray(state.evidence) && state.evidence.length > 0) return state.evidence;
  return getEvidence(fallback);
};

const formatAuctionBasePrice = (...values) => {
  for (const value of values) {
    const numeric = parseNumericEuro(value);
    if (numeric !== null) return formatMoney(numeric);
    const rendered = safeRender(value, '').trim();
    if (rendered) return rendered;
  }
  return MISSING_TEXT;
};

const normalizeSeverity = (value) => {
  const upper = safeRender(value, '').toUpperCase();
  if (['CRITICAL', 'ERROR', 'RED', 'ROSSO', 'CRITICO'].includes(upper)) return 'Critico';
  if (['WARNING', 'WARN', 'AMBER', 'ATTENZIONE', 'GIALLO'].includes(upper)) return 'Attenzione';
  if (['INFO', 'LOW', 'GREEN', 'VERDE', 'OK'].includes(upper)) return 'Info';
  return 'Da verificare';
};

const formatCatastoCompact = (catasto) => {
  if (!catasto) return '';
  if (typeof catasto === 'string') return safeRender(catasto, '');
  if (typeof catasto !== 'object') return '';
  const renderEntry = (entry, options = {}) => {
    const bits = [];
    const foglio = safeRender(pickFirstNonEmpty(entry?.foglio, catasto?.foglio), '').trim();
    const particella = safeRender(pickFirstNonEmpty(entry?.particella, catasto?.particella), '').trim();
    const sub = safeRender(pickFirstNonEmpty(entry?.sub, entry?.subalterno, entry?.subalterno_numero, entry?.numero), '').trim();
    const categoria = safeRender(pickFirstNonEmpty(entry?.categoria, entry?.categoria_catastale), '').trim();
    if (!options.omitShared && foglio) bits.push(`Fg. ${foglio}`);
    if (!options.omitShared && particella) bits.push(`Part. ${particella}`);
    if (sub) bits.push(`Sub. ${sub}`);
    if (categoria) bits.push(`Cat. ${categoria}`);
    return bits.join(' - ');
  };

  const subalterni = Array.isArray(catasto.subalterni)
    ? catasto.subalterni.map((entry) => renderEntry(entry, { omitShared: true })).filter(Boolean)
    : [];
  if (subalterni.length > 0) {
    const prefix = renderEntry(catasto)
      .split(' - ')
      .filter((part) => part.startsWith('Fg.') || part.startsWith('Part.'));
    const renderedSubs = [...new Set(subalterni)];
    return prefix.length > 0
      ? `${prefix.join(' - ')} - ${renderedSubs.join('; ')}`
      : renderedSubs.join('; ');
  }
  return renderEntry(catasto);
};

const compactEvidenceLabel = (evidence) => {
  if (!Array.isArray(evidence) || evidence.length === 0) return '';
  const pages = [...new Set(evidence.map((item) => item?.page).filter(Boolean))];
  return pages.length ? `p. ${pages.join(', ')}` : '';
};

const compactEvidenceQuote = (evidence) => {
  if (!Array.isArray(evidence) || evidence.length === 0) return '';
  let quote = safeRender(evidence[0]?.quote || evidence[0]?.search_hint, '').replace(/\s+/g, ' ').trim();
  if (!quote) return '';
  quote = quote
    .replace(/Astalegale\.net.*$/i, '')
    .replace(/E['’]\s*vietata.*$/i, '')
    .replace(/Lotto\s+n\.\s*\d+.*$/i, '')
    .replace(/Per ulteriori informazioni.*$/i, '')
    .replace(/^[,;.\s-]+/, '')
    .replace(/[,;.\s-]+$/, '')
    .trim();
  if (!quote) return '';
  return quote.length > 110 ? `${quote.slice(0, 107)}...` : quote;
};

const normalizeComparableText = (value) => safeRender(value, '')
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .replace(/\s+/g, ' ')
  .trim()
  .toLowerCase();

const mergeEvidence = (...sources) => {
  const out = [];
  const seen = new Set();
  sources.forEach((source) => {
    getEvidence(source).forEach((item) => {
      const key = `${item?.page || ''}|${safeRender(item?.quote || item?.search_hint, '').slice(0, 180)}`;
      if (!key.trim() || seen.has(key)) return;
      seen.add(key);
      out.push(item);
    });
  });
  return out;
};

const evidenceMatchesCatasto = (evidence, renderedValue) => {
  if (!renderedValue) return false;
  const quote = normalizeComparableText(compactEvidenceQuote(evidence));
  if (!quote) return false;
  const expectedTokens = renderedValue
    .split(' - ')
    .map((chunk) => normalizeComparableText(chunk).replace(/\./g, '').trim())
    .filter(Boolean);
  return expectedTokens.every((token) => !token || quote.includes(token.replace(/\./g, '')));
};

const parseCatastoFromEvidence = (evidence) => {
  const quote = compactEvidenceQuote(evidence);
  if (!quote) return '';
  const foglio = quote.match(/Fg\.?\s*([0-9]+)/i)?.[1] || '';
  const particella = quote.match(/Part\.?\s*([0-9]+)/i)?.[1] || '';
  const sub = quote.match(/Sub\.?\s*([0-9]+)/i)?.[1] || '';
  const categoria = quote.match(/(?:Categoria|Cat\.)\s*([A-Z0-9]+)/i)?.[1] || '';
  const parsed = formatCatastoCompact({ foglio, particella, sub, categoria });
  return parsed.includes('Fg.') && parsed.includes('Part.') && (sub || categoria) ? parsed : '';
};

const classifyAgibilitaSignal = (value) => {
  const text = normalizeComparableText(value);
  if (!text) return '';
  if (
    text.includes('non risulta agibile') ||
    text.includes('non e presente l’abitabilita') ||
    text.includes('non e presente l\'abitabilita') ||
    text.includes('assente')
  ) {
    return 'absent';
  }
  if (text.includes('risulta agibile') || text.includes('agibile')) {
    return 'present';
  }
  return '';
};

const buildAgibilitaDetail = (bene, abusi, fieldStates) => {
  const evidenceObj = bene?.evidence && typeof bene.evidence === 'object' ? bene.evidence : {};
  const mergedEvidence = mergeEvidence(
    evidenceObj?.agibilita,
    evidenceObj?.note,
    fieldStates?.agibilita,
    abusi?.agibilita
  );
  const renderedStatus = safeRender(
    pickFirstNonEmpty(bene?.agibilita, bene?.abitabilita, abusi?.agibilita?.status, fieldStates?.agibilita?.value),
    ''
  );
  const statusSignal = classifyAgibilitaSignal(renderedStatus);
  const evidenceSignals = mergedEvidence.map((item) => classifyAgibilitaSignal(item?.quote || item?.search_hint)).filter(Boolean);
  const hasPresent = evidenceSignals.includes('present');
  const hasAbsent = evidenceSignals.includes('absent') || statusSignal === 'absent';

  if ((hasPresent && hasAbsent) || (statusSignal === 'absent' && hasPresent)) {
    return {
      value: 'Da verificare',
      note: 'Indicazioni contrastanti sulla agibilita nella perizia.',
      evidence: mergedEvidence.slice(0, 2),
    };
  }
  if (hasAbsent || statusSignal === 'absent') {
    const absentEvidence = mergedEvidence.filter((item) => classifyAgibilitaSignal(item?.quote || item?.search_hint) === 'absent');
    return {
      value: 'Assente',
      note: '',
      evidence: (absentEvidence.length > 0 ? absentEvidence : mergedEvidence).slice(0, 2),
    };
  }
  if (hasPresent) {
    const presentEvidence = mergedEvidence.filter((item) => classifyAgibilitaSignal(item?.quote || item?.search_hint) === 'present');
    return {
      value: 'Da verificare',
      note: 'Nel testo compaiono riferimenti non univoci sulla agibilita.',
      evidence: presentEvidence.slice(0, 2),
    };
  }
  return {
    value: renderedStatus,
    note: '',
    evidence: mergedEvidence.slice(0, 2),
  };
};

const buildClientFacingDriver = (semaforo, decision) => {
  const base = normalizeComparableText(semaforo?.driver?.value || semaforo?.reason_it);
  const decisionText = normalizeComparableText(decision?.summary_it);
  if (decisionText.includes('dati asta')) {
    return "Prima dell'offerta e' consigliabile confermare i dati d'asta sul portale ufficiale della procedura.";
  }
  if (base.includes('revisione manuale') || base.includes('analisi automatica parziale')) {
    return "Prima dell'offerta e' consigliabile una verifica documentale puntuale sui profili principali della perizia.";
  }
  return safeRender(semaforo?.driver?.value || semaforo?.reason_it, "Prima dell'offerta e' consigliabile una verifica documentale puntuale.");
};

const buildCostBuckets = (result) => {
  const policy = buildCustomerCostPolicy(result);
  return {
    valuationAdjustments: policy.valuationAdjustments,
    explicitCostMentions: policy.explicitBuyerCosts.map((item) => ({
      key: item.__policy_key,
      label: item.__policy_label,
      amount: formatMoney(item.__policy_amount),
      note: item.__policy_note,
      evidence: item.__policy_evidence || [],
    })),
    groundedUnquantifiedBurdens: policy.groundedUnquantifiedBurdens.map((item) => ({
      key: item.key,
      label: item.label,
      note: item.note,
      evidence: item.evidence || [],
    })),
    totalSummary: policy.totalSummary,
  };
};

const buildLegalItems = (result) => {
  const fieldStates = result.field_states || {};
  const section9 = Array.isArray(result.section_9_legal_killers?.top_items) && result.section_9_legal_killers.top_items.length > 0
    ? result.section_9_legal_killers.top_items
    : (Array.isArray(result.section_9_legal_killers?.items) ? result.section_9_legal_killers.items : []);
  const items = [];
  const isWeakLegalFallback = (title, detail, evidence) => {
    const combined = `${normalizeComparableText(title)} ${normalizeComparableText(detail)}`;
    if (combined.includes('servitu') || combined.includes('usi civici') || combined.includes('censo') || combined.includes('livello')) {
      return true;
    }
    const quotes = getEvidence(evidence).map((item) => normalizeComparableText(item?.quote || item?.search_hint));
    if (quotes.length === 0) return false;
    const allWeak = quotes.every((quote) =>
      !quote ||
      quote.includes('................................') ||
      quote.includes('servitu censo livello usi civici') ||
      quote.includes('eventuali vincoli e servitu passive o attive')
    );
    return allWeak;
  };

  const pushItem = (key, title, status, detail, evidence) => {
    const renderedTitle = safeRender(title, '');
    const renderedDetail = safeRender(detail, '');
    if (!renderedTitle || !renderedDetail || renderedDetail === MISSING_TEXT) return;
    if (isPositiveOrNeutralLegalTruth(key, renderedDetail)) return;
    if (!safeRender(key, '').startsWith('section9-') && isWeakLegalFallback(renderedTitle, renderedDetail, evidence)) return;
    const evidenceText = getEvidence(evidence)
      .map((item) => `${safeRender(item?.quote, '')} ${safeRender(item?.search_hint, '')}`)
      .join(' ');
    const canonicalMeta = buildCanonicalLegalPriorityMeta({
      key,
      title: renderedTitle,
      detail: renderedDetail,
      evidenceText
    });
    if (canonicalMeta.kind === 'neutral_truth') return;
    const { category, kind, semanticKey } = canonicalMeta;
    const existing = items.find((item) =>
      item.key === key ||
      item.semanticKey === semanticKey ||
      (item.title === renderedTitle && item.detail === renderedDetail)
    );
    if (existing) {
      existing.evidence = [...new Map([...existing.evidence, ...(Array.isArray(evidence) ? evidence.slice(0, 2) : [])].map((ev, idx) => [`${safeRender(ev?.page, '')}|${safeRender(ev?.quote, '')}|${idx}`, ev])).values()].slice(0, 2);
      if (renderedDetail.length > existing.detail.length) existing.detail = renderedDetail;
      return;
    }
    items.push({
      key,
      semanticKey,
      title: renderedTitle,
      status: normalizeSeverity(status),
      detail: renderedDetail,
      evidence: Array.isArray(evidence) ? evidence.slice(0, 2) : [],
      kind,
    });
  };

  pushItem('urbanistica', 'Regolarita urbanistica', fieldStates.regolarita_urbanistica?.status, fieldStates.regolarita_urbanistica?.value, fieldStates.regolarita_urbanistica?.evidence);
  pushItem('catasto', 'Conformita catastale', fieldStates.conformita_catastale?.status, fieldStates.conformita_catastale?.value, fieldStates.conformita_catastale?.evidence);
  pushItem('agibilita', 'Agibilita', fieldStates.agibilita?.status, fieldStates.agibilita?.value, fieldStates.agibilita?.evidence);
  pushItem('occupazione', 'Stato occupativo', fieldStates.stato_occupativo?.status, fieldStates.stato_occupativo?.value, fieldStates.stato_occupativo?.evidence);
  pushItem('opponibilita', 'Opponibilita occupazione', fieldStates.opponibilita_occupazione?.status, fieldStates.opponibilita_occupazione?.value, fieldStates.opponibilita_occupazione?.evidence);
  pushItem('delivery-timeline', 'Tempistica liberazione', fieldStates.delivery_timeline?.status, fieldStates.delivery_timeline?.value, fieldStates.delivery_timeline?.evidence);

  section9.forEach((item, index) => {
    pushItem(
      `section9-${index}`,
      item?.killer || item?.label_it || item?.label,
      item?.status_it || item?.status,
      item?.reason_it || item?.action_required_it || item?.action,
      item?.evidence
    );
  });

  const hasHigherPriorityLegalItem = items.some((item) => item.kind === 'material_blocker' || item.kind === 'caution_watch');
  const filtered = hasHigherPriorityLegalItem
    ? items.filter((item) => item.kind !== 'background_note')
    : items;
  return filtered.sort((a, b) => {
    const rankDelta = (LEGAL_KIND_RANK[a.kind] ?? 9) - (LEGAL_KIND_RANK[b.kind] ?? 9);
    if (rankDelta !== 0) return rankDelta;
    return a.title.localeCompare(b.title, 'it');
  });
};

const buildSharedRightsNote = (lot) => {
  if (!lot || typeof lot !== 'object') return '';
  const direct = safeRender(
    pickFirstNonEmpty(
      lot?.shared_rights_note,
      lot?.shared_rights,
      lot?.quota_note,
      lot?.note_diritto,
      lot?.notes_diritto
    ),
    ''
  ).trim();
  if (direct) return direct;
  const riskNotes = Array.isArray(lot?.risk_notes) ? lot.risk_notes : [];
  const match = riskNotes.find((note) => {
    const text = safeRender(note, '').toLowerCase();
    return text.includes('stradella') || text.includes('strada privata') || text.includes('quota 1/4') || text.includes('corte comune');
  });
  return safeRender(match, '').trim();
};

const buildDetails = (result) => {
  const detailScope = safeRender(result.detail_scope, '').toUpperCase();
  const lots = Array.isArray(result.lots) ? result.lots : [];
  if (detailScope === 'LOT_FIRST' && lots.length > 1) {
    return lots.map((lot, index) => {
      const evidenceObj = lot?.evidence && typeof lot.evidence === 'object' ? lot.evidence : {};
      const subordinateBeni = Array.isArray(lot?.beni)
        ? lot.beni.map((bene, beneIndex) => [
            bene?.bene_number ? `Bene ${bene.bene_number}` : `Bene ${beneIndex + 1}`,
            safeRender(bene?.tipologia, ''),
            safeRender(pickFirstNonEmpty(bene?.ubicazione, bene?.indirizzo, bene?.short_location), ''),
          ].filter(Boolean).join(' - '))
        : [];
      const sharedRightsNote = buildSharedRightsNote(lot);
      return {
        key: `lot-${lot?.lot_number || index + 1}`,
        title: `Lotto ${lot?.lot_number || index + 1}${safeRender(lot?.tipologia, '') ? ` - ${safeRender(lot?.tipologia, '')}` : ''}`,
        location: safeRender(lot?.ubicazione, ''),
        piano: '',
        superficie: formatSurfaceValue(pickFirstNonEmpty(lot?.superficie_convenzionale_mq, lot?.superficie_convenzionale, lot?.superficie_mq)),
        valoreStima: formatMoney(lot?.valore_stima_eur),
        topEvidence: getPrimaryEvidence(evidenceObj?.ubicazione, evidenceObj?.tipologia, evidenceObj?.superficie, evidenceObj?.valore_stima),
        detailRows: [
          { label: 'Diritto reale', value: splitQuotaFromDiritto(safeRender(lot?.diritto_reale, ''), safeRender(lot?.quota, '')), evidence: getPrimaryEvidence(evidenceObj?.diritto_reale) },
          { label: 'Quota', value: safeRender(lot?.quota, ''), evidence: getPrimaryEvidence(evidenceObj?.quota, evidenceObj?.diritto_reale) },
          { label: 'Diritti condivisi', value: sharedRightsNote, evidence: getPrimaryEvidence(evidenceObj?.note) },
          { label: 'Prezzo base', value: safeRender(lot?.prezzo_base_eur, ''), evidence: getPrimaryEvidence(evidenceObj?.prezzo_base) },
          { label: 'Stato occupativo', value: safeRender(lot?.occupancy_status || lot?.stato_occupativo, ''), evidence: getPrimaryEvidence(evidenceObj?.occupancy_status) },
          { label: 'Catasto', value: formatCatastoCompact(lot?.catasto) || safeRender(lot?.catasto, ''), evidence: getPrimaryEvidence(evidenceObj?.catasto) },
          { label: 'Stato conservativo', value: safeRender(lot?.stato_conservativo, ''), evidence: getPrimaryEvidence(evidenceObj?.stato_conservativo) },
          { label: 'Rischi principali', value: Array.isArray(lot?.risk_notes) ? lot.risk_notes.map((note) => safeRender(note, '')).filter(Boolean).slice(0, 3).join(' | ') : '', evidence: getPrimaryEvidence(evidenceObj?.note) },
          { label: 'Beni subordinati', value: subordinateBeni.join(' | '), evidence: [] },
        ].filter((row) => row.value),
        impiantiRows: [],
        declarationRows: [],
      };
    });
  }
  const panoramicaContract = result.panoramica_contract || {};
  const contractBeni = Array.isArray(panoramicaContract.lot_composition) ? panoramicaContract.lot_composition : [];
  const sourceBeni = Array.isArray(result.beni) ? result.beni : (Array.isArray(result.lots?.[0]?.beni) ? result.lots[0].beni : []);
  const fieldStates = result.field_states || {};
  const abusi = result.section_5_abusi_conformita?.conformita_urbanistica ? result.section_5_abusi_conformita : (result.abusi_edilizi_conformita || {});
  const occupativo = result.section_6_stato_occupativo?.status ? result.section_6_stato_occupativo : (result.stato_occupativo || {});
  const quota = safeRender(
    pickFirstNonEmpty(
      getFieldStateValue(fieldStates.quota),
      result.dati_certi_del_lotto?.quota,
      result.section_4_dati_certi?.quota
    ),
    ''
  );
  const dirittoReale = splitQuotaFromDiritto(
    safeRender(
      pickFirstNonEmpty(
        getFieldStateValue(fieldStates.diritto_reale),
        result.dati_certi_del_lotto?.diritto_reale,
        result.section_4_dati_certi?.diritto_reale
      ),
      ''
    ),
    quota
  );
  const fallbackSurface = pickFirstNonEmpty(
    result.dati_certi_del_lotto?.superficie?.value,
    result.dati_certi_del_lotto?.superficie,
    formatSurfaceValue(getFieldStateValue(fieldStates.superficie))
  );

  const byNumber = new Map();
  [...contractBeni, ...sourceBeni].forEach((bene, index) => {
    const beneNumber = parseNumericEuro(pickFirstNonEmpty(bene?.bene_number, bene?.numero_bene, bene?.numero, index + 1)) || index + 1;
    const current = byNumber.get(beneNumber) || {};
    const currentEvidence = current?.evidence && typeof current.evidence === 'object' ? current.evidence : {};
    const nextEvidence = bene?.evidence && typeof bene.evidence === 'object' ? bene.evidence : {};
    byNumber.set(beneNumber, {
      ...current,
      ...bene,
      evidence: { ...currentEvidence, ...nextEvidence },
      bene_number: beneNumber
    });
  });

  return [...byNumber.values()]
    .sort((a, b) => (a.bene_number || 0) - (b.bene_number || 0))
    .map((bene, index) => {
      const evidenceObj = bene?.evidence && typeof bene.evidence === 'object' ? bene.evidence : {};
      const occupazioneValue = pickFirstNonEmpty(
        getFieldStateValue(fieldStates.stato_occupativo),
        bene?.occupancy_status,
        bene?.stato_occupativo,
        bene?.occupazione_status,
        occupativo?.status_it,
        occupativo?.status
      );
      const urbanisticaValue = pickFirstNonEmpty(
        getRichDisplayValue(fieldStates.regolarita_urbanistica, abusi?.conformita_urbanistica),
        bene?.urbanistica,
        bene?.regolarita_urbanistica,
        bene?.conformita_urbanistica
      );
      const apeValue = pickFirstNonEmpty(
        getRichDisplayValue(fieldStates.ape, abusi?.ape),
        bene?.ape,
      );
      const elettricoDeclarationValue = pickFirstNonEmpty(
        bene?.dichiarazioni?.dichiarazione_impianto_elettrico,
        bene?.dichiarazioni_impianti?.elettrico,
        getFieldStateValue(fieldStates.dichiarazione_impianto_elettrico),
        getLegacyDetailValue(abusi?.impianti?.elettrico)
      );
      const idricoDeclarationValue = pickFirstNonEmpty(
        bene?.dichiarazioni?.dichiarazione_impianto_idrico,
        bene?.dichiarazioni_impianti?.idrico,
        getFieldStateValue(fieldStates.dichiarazione_impianto_idrico),
        getLegacyDetailValue(abusi?.impianti?.idrico)
      );
      const gasDeclarationValue = pickFirstNonEmpty(
        bene?.dichiarazioni?.dichiarazione_impianto_gas,
        bene?.dichiarazioni_impianti?.gas,
        getFieldStateValue(fieldStates.dichiarazione_impianto_gas),
        getLegacyDetailValue(abusi?.impianti?.gas)
      );
      const catastoFromEvidence = parseCatastoFromEvidence(evidenceObj?.catasto);
      const catastoValue = formatCatastoCompact(bene?.catasto) || catastoFromEvidence;
      const catastoEvidence = evidenceMatchesCatasto(evidenceObj?.catasto, catastoValue)
        ? getPrimaryEvidence(evidenceObj?.catasto)
        : [];
      const agibilitaDetail = buildAgibilitaDetail(bene, abusi, fieldStates);
      const detailRows = [
        { label: 'Diritto reale', value: dirittoReale, evidence: [] },
        { label: 'Quota', value: quota, evidence: [] },
        { label: 'Stato occupativo', value: safeRender(occupazioneValue, ''), evidence: getPrimaryEvidence(evidenceObj?.occupancy_status, getFieldStateEvidence(fieldStates.stato_occupativo, occupativo)) },
        { label: 'Catasto', value: catastoValue, evidence: catastoEvidence },
        { label: 'Urbanistica', value: safeRender(urbanisticaValue, ''), evidence: getPrimaryEvidence(evidenceObj?.urbanistica, getFieldStateEvidence(fieldStates.regolarita_urbanistica, abusi?.conformita_urbanistica)) },
        { label: 'Agibilita / Abitabilita', value: agibilitaDetail.value, note: agibilitaDetail.note, evidence: agibilitaDetail.evidence },
        { label: 'Stato conservativo', value: safeRender(bene?.stato_conservativo?.status_it || bene?.stato_conservativo?.general_condition_it || bene?.stato_conservativo, ''), evidence: getPrimaryEvidence(bene?.stato_conservativo, evidenceObj?.stato_conservativo) },
        { label: 'APE', value: safeRender(apeValue, ''), evidence: getPrimaryEvidence(evidenceObj?.ape, getFieldStateEvidence(fieldStates.ape, abusi?.ape)) },
      ].filter((row) => row.value);

      const impiantiRows = ['elettrico', 'idrico', 'termico']
        .map((key) => ({
          label: key[0].toUpperCase() + key.slice(1),
          value: safeRender(bene?.impianti?.[key]?.status_it || bene?.impianti?.[key]?.status || bene?.impianti?.[key], ''),
          evidence: getPrimaryEvidence(bene?.impianti?.[key], evidenceObj?.impianti?.[key]),
        }))
        .filter((row) => row.value);

      const declarationRows = [
        { label: 'Dichiarazione impianto elettrico', value: safeRender(elettricoDeclarationValue, ''), evidence: getPrimaryEvidence(evidenceObj?.dichiarazioni?.dichiarazione_impianto_elettrico, evidenceObj?.dichiarazioni_impianti?.elettrico, getFieldStateEvidence(fieldStates.dichiarazione_impianto_elettrico, abusi?.impianti?.elettrico)) },
        { label: 'Dichiarazione impianto termico', value: safeRender(bene?.dichiarazioni?.dichiarazione_impianto_termico || bene?.dichiarazioni_impianti?.termico, ''), evidence: getPrimaryEvidence(evidenceObj?.dichiarazioni?.dichiarazione_impianto_termico, evidenceObj?.dichiarazioni_impianti?.termico) },
        { label: 'Dichiarazione impianto idrico', value: safeRender(idricoDeclarationValue, ''), evidence: getPrimaryEvidence(evidenceObj?.dichiarazioni?.dichiarazione_impianto_idrico, evidenceObj?.dichiarazioni_impianti?.idrico, getFieldStateEvidence(fieldStates.dichiarazione_impianto_idrico, abusi?.impianti?.idrico)) },
        { label: 'Dichiarazione impianto gas', value: safeRender(gasDeclarationValue, ''), evidence: getPrimaryEvidence(evidenceObj?.dichiarazioni?.dichiarazione_impianto_gas, evidenceObj?.dichiarazioni_impianti?.gas, getFieldStateEvidence(fieldStates.dichiarazione_impianto_gas, abusi?.impianti?.gas)) },
      ].filter((row) => row.value);

      return {
        key: `bene-${bene?.bene_number || index + 1}`,
        title: `Bene ${bene?.bene_number || index + 1}${safeRender(bene?.tipologia, '') ? ` - ${safeRender(bene?.tipologia, '')}` : ''}`,
        location: safeRender(pickFirstNonEmpty(bene?.short_location, bene?.ubicazione, bene?.indirizzo), ''),
        piano: safeRender(bene?.piano, ''),
        superficie: formatSurfaceValue(
          pickFirstNonEmpty(
            bene?.superficie_convenzionale_mq,
            bene?.superficie_convenzionale,
            bene?.superficie_mq,
            fallbackSurface
          )
        ),
        valoreStima: formatMoney(pickFirstNonEmpty(bene?.valore_stima_eur, bene?.valore_stima_bene, bene?.valore_di_stima_bene)),
        topEvidence: getPrimaryEvidence(evidenceObj?.location_piano, evidenceObj?.superficie_mq, evidenceObj?.valore_stima_eur),
        detailRows,
        impiantiRows,
        declarationRows,
      };
    });
};

const buildFlags = (result, legalItems = []) => {
  const rawFlags = Array.isArray(result.section_11_red_flags) && result.section_11_red_flags.length > 0
    ? result.section_11_red_flags
    : (Array.isArray(result.red_flags_operativi) ? result.red_flags_operativi : []);
  const items = [];
  const semanticFlagKey = (title, detail) => {
    const combined = `${normalizeComparableText(title)} ${normalizeComparableText(detail)}`;
    if (combined.includes('dati asta') || combined.includes('data e ora asta') || combined.includes('verifica dati asta')) {
      return 'auction-data';
    }
    return '';
  };
  const pushItem = (key, title, detail, severity, evidence) => {
    const renderedTitle = safeRender(title, '');
    const renderedDetail = safeRender(detail, '');
    if (!renderedTitle || !renderedDetail) return;
    const semanticKey = semanticFlagKey(renderedTitle, renderedDetail);
    const existing = items.find((item) =>
      item.key === key ||
      (item.title === renderedTitle && item.detail === renderedDetail) ||
      (semanticKey && item.semanticKey === semanticKey)
    );
    if (existing) {
      existing.evidence = mergeEvidence(existing.evidence, evidence).slice(0, 2);
      if (renderedDetail.length > existing.detail.length) existing.detail = renderedDetail;
      if (renderedTitle.length > existing.title.length) existing.title = renderedTitle;
      return;
    }
    items.push({
      key,
      semanticKey,
      title: renderedTitle,
      detail: renderedDetail,
      severity: normalizeSeverity(severity),
      evidence: Array.isArray(evidence) ? evidence.slice(0, 2) : [],
    });
  };

  rawFlags.forEach((flag, index) => {
    if (typeof flag === 'string') {
      pushItem(`flag-${index}`, `Segnalazione ${index + 1}`, flag, 'AMBER', []);
      return;
    }
    const label = normalizeComparableText(flag?.flag_it || flag?.label || flag?.title_it || flag?.title);
    if (label.includes('manual review') || label.includes('revisione manuale')) return;
    pushItem(
      `flag-${index}`,
      flag?.flag_it || flag?.label || flag?.title_it || flag?.title,
      flag?.action_it || flag?.explanation || flag?.detail || flag?.reason_it,
      flag?.severity || flag?.status || 'AMBER',
      flag?.evidence
    );
  });

  const canonicalPrintAttentionItems = getCanonicalTopAttentionItems(legalItems);

  canonicalPrintAttentionItems.forEach((item, index) => {
    if (!item || item.kind === 'background_note' || item.kind === 'execution_context') return;
    pushItem(
      `legal-${index}`,
      item.title,
      item.detail,
      item.status,
      item.evidence
    );
  });

  const severityOrder = { Critico: 0, Attenzione: 1, Info: 2, 'Da verificare': 3 };
  return items.sort((a, b) => (severityOrder[a.severity] || 9) - (severityOrder[b.severity] || 9));
};

export const buildPeriziaPrintReportModel = (rawAnalysis) => {
  const analysis = normalizeAnalysisResponse(rawAnalysis) || {};
  const result = analysis.result || {};
  const fieldStates = result.field_states || {};
  const section4 = result.section_4_dati_certi || {};
  const reportHeader = result.report_header?.procedure ? result.report_header : (result.case_header || {});
  const semaforo = result.section_1_semaforo_generale?.status ? result.section_1_semaforo_generale : (result.semaforo_generale || {});
  const narratedDecision = result.decision_rapida_narrated && typeof result.decision_rapida_narrated === 'object'
    ? result.decision_rapida_narrated
    : {};
  const decision = result.decision_rapida_client || result.section_2_decisione_rapida || {};
  const summary = result.summary_for_client || {};
  const panoramicaContract = result.panoramica_contract || {};
  const lotSummary = panoramicaContract.lot_summary || {};
  const valuation = panoramicaContract.valuation_waterfall || {};
  const lotComposition = Array.isArray(panoramicaContract.lots_overview) && panoramicaContract.lots_overview.length > 0
    ? panoramicaContract.lots_overview
    : (Array.isArray(panoramicaContract.lot_composition) ? panoramicaContract.lot_composition : []);
  const normalizedSurface = pickFirstNonEmpty(
    result.dati_certi_del_lotto?.superficie?.value,
    result.dati_certi_del_lotto?.superficie,
    formatSurfaceValue(getFieldStateValue(fieldStates.superficie))
  );
  const details = buildDetails(result);
  const costBuckets = buildCostBuckets(result);
  const legalItems = buildLegalItems(result);
  const flags = buildFlags(result, legalItems);
  const rawCoverSummaryIt = safeRender(
    pickFirstNonEmpty(
      narratedDecision.it,
      decision.summary_it,
      summary.summary_it
    ),
    ''
  );
  const topAttentionLegalItem = pickCanonicalTopAttentionItem(legalItems);
  const coverSummaryIt = topAttentionLegalItem
    ? safeRender(topAttentionLegalItem.title, rawCoverSummaryIt)
    : isWeakBackgroundLegalSummary(rawCoverSummaryIt)
      ? safeRender(decision.summary_it || summary.summary_it, rawCoverSummaryIt)
      : rawCoverSummaryIt;
  const overviewDecisionIt = topAttentionLegalItem
    ? safeRender(topAttentionLegalItem.title, rawCoverSummaryIt)
    : safeRender(
      pickFirstNonEmpty(
        narratedDecision.it,
        decision.summary_it,
        summary.summary_it
      ),
      ''
    );
  const coverAddress = safeRender(
    pickFirstNonEmpty(
      lotSummary.ubicazione,
      lotSummary.address?.value,
      lotSummary.address,
      reportHeader.address?.value,
      reportHeader.address?.full,
      reportHeader.address
    ),
    MISSING_TEXT
  );

  return {
    title: safeRender(analysis.case_title || analysis.file_name, 'Analisi Perizia'),
    fileName: safeRender(analysis.file_name, ''),
    createdAt: analysis.created_at ? new Date(analysis.created_at).toLocaleString('it-IT') : '',
    cover: {
      procedura: safeRender(reportHeader.procedure?.value || reportHeader.procedure || lotSummary.procedura, MISSING_TEXT),
      tribunale: safeRender(reportHeader.tribunale?.value || reportHeader.tribunale || lotSummary.tribunale, MISSING_TEXT),
      lotto: safeRender(reportHeader.lotto?.value || reportHeader.lotto || lotSummary.lotto_label, MISSING_TEXT),
      indirizzo: coverAddress,
      semaforo: safeRender(semaforo.status_label || semaforo.status_it || semaforo.status, 'AMBER'),
      summaryIt: coverSummaryIt,
    },
    overview: {
      driver: buildClientFacingDriver(semaforo, decision),
      decisionIt: overviewDecisionIt,
      metrics: [
        { label: 'Numero lotti', value: safeRender(panoramicaContract.lots_count, '') },
        { label: 'Valore di stima', value: formatMoney(valuation.valore_stima_eur) },
        { label: 'Deprezzamenti', value: formatMoney(valuation.deprezzamenti_eur) },
        { label: 'Valore finale', value: formatMoney(valuation.valore_finale_eur) },
        {
          label: 'Prezzo base',
          value: formatAuctionBasePrice(
            lotSummary.prezzo_base_eur,
            valuation.prezzo_base_eur,
            getFieldStateValue(fieldStates.prezzo_base_asta),
            section4?.prezzo_base_asta?.formatted,
            section4?.prezzo_base_asta?.value,
            result.dati_certi_del_lotto?.prezzo_base_asta?.formatted,
            result.dati_certi_del_lotto?.prezzo_base_asta?.value
          ),
        },
      ].filter((item) => item.value && item.value !== MISSING_TEXT),
      composition: lotComposition.map((item, index) => ({
        key: `overview-bene-${index}`,
        title: item?.lot_number
          ? `Lotto ${item.lot_number}`
          : (item?.bene_number ? `Bene ${item.bene_number}` : `Bene ${index + 1}`),
        type: safeRender(item?.tipologia, ''),
        location: safeRender(item?.short_location || item?.ubicazione, ''),
        piano: safeRender(item?.piano, ''),
        superficie: formatSurfaceValue(pickFirstNonEmpty(item?.superficie_convenzionale_mq, item?.superficie_convenzionale, item?.superficie_mq, lotComposition.length === 1 ? normalizedSurface : null)),
        valoreStima: formatMoney(item?.valore_stima_eur || item?.prezzo_base_eur),
        evidence: getPrimaryEvidence(item?.evidence?.location_piano, item?.evidence?.valore_stima_eur, item?.evidence?.ubicazione),
      })),
    },
    costs: costBuckets,
    legal: legalItems,
    details,
    flags,
    disclaimer: {
      it: safeRender(summary.disclaimer_it, 'Documento informativo. Non costituisce consulenza legale. Consultare un professionista qualificato.'),
      en: safeRender(summary.disclaimer_en, 'Informational document. Not legal advice. Consult a qualified professional.'),
    },
  };
};

export const summarizeEvidence = (evidence) => ({
  pages: compactEvidenceLabel(evidence),
  quote: compactEvidenceQuote(evidence),
});
