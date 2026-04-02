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
    if (value.detail_it !== undefined) return safeRender(value.detail_it, fallback);
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

const classifyAgibilitaSignal = (value) => {
  const text = normalizeComparableText(value);
  if (!text) return '';
  if (
    text.includes('non risulta agibile') ||
    text.includes('non e presente l’abitabilita') ||
    text.includes("non e presente l'abitabilita") ||
    text.includes('assente')
  ) {
    return 'absent';
  }
  if (text.includes('risulta agibile') || text.includes('agibile') || text.includes('presente')) {
    return 'present';
  }
  return '';
};

export const resolveAgibilitaDetail = ({ candidateValues = [], evidenceSources = [] }) => {
  const renderedStatus = candidateValues
    .map((value) => safeRender(value, '').trim())
    .find(Boolean) || '';
  const mergedEvidence = mergeEvidence(...evidenceSources);
  const statusSignal = classifyAgibilitaSignal(renderedStatus);
  const evidenceSignals = mergedEvidence
    .map((item) => classifyAgibilitaSignal(`${safeRender(item?.quote, '')} ${safeRender(item?.search_hint, '')}`))
    .filter(Boolean);
  const hasPresent = statusSignal === 'present' || evidenceSignals.includes('present');
  const hasAbsent = statusSignal === 'absent' || evidenceSignals.includes('absent');

  if (hasPresent && hasAbsent) {
    return {
      value: 'Da verificare',
      note: 'Indicazioni contrastanti sulla agibilita nella perizia.',
      evidence: mergedEvidence,
    };
  }
  if (hasAbsent) {
    const absentEvidence = mergedEvidence.filter((item) => classifyAgibilitaSignal(`${safeRender(item?.quote, '')} ${safeRender(item?.search_hint, '')}`) === 'absent');
    return {
      value: 'Assente',
      note: '',
      evidence: (absentEvidence.length > 0 ? absentEvidence : mergedEvidence),
    };
  }
  if (renderedStatus) {
    return {
      value: renderedStatus,
      note: '',
      evidence: mergedEvidence,
    };
  }
  if (hasPresent) {
    return {
      value: 'PRESENTE',
      note: '',
      evidence: mergedEvidence,
    };
  }
  return {
    value: '',
    note: '',
    evidence: mergedEvidence,
  };
};
