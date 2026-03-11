const MISSING_TEXT = 'Non specificato in perizia';
const EURO_FORMATTER = new Intl.NumberFormat('it-IT', {
  maximumFractionDigits: 0,
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
  const cleaned = value.replace(/[^\d,.-]/g, '');
  if (!cleaned) return null;
  const normalized = cleaned.includes(',') ? cleaned.replace(/\./g, '').replace(',', '.') : cleaned;
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
  const bits = [];
  const foglio = safeRender(catasto.foglio, '').trim();
  const particella = safeRender(catasto.particella, '').trim();
  const sub = safeRender(catasto.sub, '').trim();
  const categoria = safeRender(catasto.categoria, '').trim();
  if (foglio) bits.push(`Fg. ${foglio}`);
  if (particella) bits.push(`Part. ${particella}`);
  if (sub) bits.push(`Sub. ${sub}`);
  if (categoria) bits.push(`Cat. ${categoria}`);
  return bits.join(' - ');
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

const classifyExplicitCost = (item) => {
  const evidence = mergeEvidence(item?.fonte_perizia, item);
  const quote = normalizeComparableText(compactEvidenceQuote(evidence));
  const amount = parseNumericEuro(item?.stima_euro);
  if (amount === null || amount <= 0 || !quote) return null;
  if (quote.includes('valore finale di stima') || quote.includes('prezzo base')) return null;
  if (quote.includes('mancata garanzia')) return null;
  if (quote.includes('regolarizzazione urbanistica 23000')) return null;

  let label = '';
  if (quote.includes('completamento lavori') && amount === 15000) label = 'Completamento lavori';
  else if (quote.includes('pratiche per abitabilita') && amount === 5000) label = 'Pratiche per abitabilita';
  else if (quote.includes('completamento lavori') && !quote.includes('pratiche per abitabilita')) label = 'Completamento lavori';
  else if (quote.includes('pratiche per abitabilita')) label = 'Pratiche per abitabilita';
  else if (quote.includes('oblazione')) label = 'Oblazione art. 36-bis';
  else if (quote.includes('sanatoria') || quote.includes('spese di massima presunte')) label = 'Sanatoria urbanistica';
  else if (quote.includes('abitabilita')) label = 'Pratiche per abitabilita';
  else return null;

  return {
    key: `${label}-${amount}`,
    label,
    amount: formatMoney(amount),
    note: 'Costo esplicitamente citato nella perizia.',
    evidence: evidence.slice(0, 2),
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

const buildCostBuckets = (result, panoramicaContract) => {
  const moneyBox = result.section_3_money_box?.items ? result.section_3_money_box : (result.money_box || {});
  const valuation = panoramicaContract?.valuation_waterfall || {};
  if (safeRender(moneyBox?.policy, '').toUpperCase() === 'LOT_CONSERVATIVE') {
    const lotItemsSource = Array.isArray(moneyBox?.lots) ? moneyBox.lots : [];
    const flatQualitativeItems = lotItemsSource.flatMap((lot) => {
      const lotNumber = safeRender(lot?.lot_number, '');
      const items = Array.isArray(lot?.items)
        ? lot.items
        : (Array.isArray(lot?.burdens) ? lot.burdens : []);
      return items.map((item, index) => ({
        key: `lot-${lotNumber || 'x'}-${index}`,
        label: lotNumber ? `Lotto ${lotNumber} - ${safeRender(item?.label_it || item?.label || item?.title, 'Voce qualitativa')}` : safeRender(item?.label_it || item?.label || item?.title, 'Voce qualitativa'),
        amount: 'Non quantificato',
        note: safeRender(item?.note_it || item?.note || item?.detail || item?.burden_type || "Oneri potenziali a carico dell'acquirente, non quantificati nella perizia.", ''),
        evidence: getPrimaryEvidence(item?.evidence, lot?.evidence),
      }));
    });
    const rootQualitativeItems = flatQualitativeItems.length > 0
      ? flatQualitativeItems
      : (Array.isArray(moneyBox?.items) ? moneyBox.items : []).map((item, index) => ({
          key: `qual-${index}`,
          label: [
            safeRender(item?.lot_number, '') ? `Lotto ${safeRender(item?.lot_number, '')}` : '',
            safeRender(item?.label_it || item?.label || item?.title, 'Voce qualitativa')
          ].filter(Boolean).join(' - '),
          amount: 'Non quantificato',
          note: safeRender(item?.note_it || item?.note || item?.detail || "Oneri potenziali a carico dell'acquirente, non quantificati nella perizia.", ''),
          evidence: getPrimaryEvidence(item?.evidence),
        }));
    return {
      valuationAdjustments: {
        amount: formatMoney(valuation.deprezzamenti_eur),
        evidence: getPrimaryEvidence(valuation?.evidence?.deprezzamenti_eur),
        note: 'Deprezzamento di perizia: catena economica del lotto, non tabella di extra-costi.',
      },
      scenarioRange: '',
      explicitCostMentions: rootQualitativeItems,
      nexodifyEstimateItems: [],
    };
  }
  const items = Array.isArray(moneyBox.items) ? moneyBox.items : [];
  const canonical = items.filter((item) => /^[A-H]$/i.test(safeRender(item?.code || item?.voce, '')));
  const explicitMap = new Map();
  items
    .filter((item) => /^S3C/i.test(safeRender(item?.code, '')) || safeRender(item?.code, '') === 'A')
    .forEach((item) => {
      const classified = classifyExplicitCost(item);
      if (!classified || explicitMap.has(classified.key)) return;
      explicitMap.set(classified.key, classified);
    });
  const explicit = [...explicitMap.values()];

  const nexodify = canonical.map((item) => ({
    key: safeRender(item?.code, 'cost-item'),
    code: safeRender(item?.code, ''),
    label: safeRender(item?.label_it || item?.label || item?.voce, ''),
    amount: formatMoney(item?.stima_euro),
    note: '',
    evidence: [],
    isQuantified: parseNumericEuro(item?.stima_euro) !== null,
    sourceLabel: normalizeComparableText(item?.source || item?.fonte_perizia?.value || item?.fonte_perizia),
  }));

  const unknownItems = nexodify.filter((item) => !item.isQuantified);
  const quantifiedItems = nexodify.filter((item) => item.isQuantified && item.sourceLabel.includes('market'));
  const groupedUnknown = unknownItems.length > 0 ? {
    key: 'nexodify-unknown',
    label: 'Voci da quantificare con verifica tecnica',
    amount: MISSING_TEXT,
    note: `${unknownItems.slice(0, 4).map((item) => item.label).join(', ')}${unknownItems.length > 4 ? ' e altre voci' : ''}.`,
    evidence: [],
  } : null;

  const range = moneyBox.total_extra_costs_range || {};
  const rangeMin = parseNumericEuro(range.min_eur);
  const rangeMax = parseNumericEuro(range.max_eur);

  return {
    valuationAdjustments: {
      amount: formatMoney(valuation.deprezzamenti_eur),
      evidence: getPrimaryEvidence(valuation?.evidence?.deprezzamenti_eur),
      note: 'Deprezzamento di perizia: non equivale automaticamente a cassa extra lato acquirente.',
    },
    scenarioRange: rangeMin !== null && rangeMax !== null ? `${formatMoney(rangeMin)} - ${formatMoney(rangeMax)}` : '',
    explicitCostMentions: explicit.sort((a, b) => (parseNumericEuro(b.amount) || 0) - (parseNumericEuro(a.amount) || 0)),
    nexodifyEstimateItems: groupedUnknown ? [...quantifiedItems, groupedUnknown] : quantifiedItems,
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
    if (isWeakLegalFallback(renderedTitle, renderedDetail, evidence)) return;
    if (items.some((item) => item.key === key || (item.title === renderedTitle && item.detail === renderedDetail))) return;
    items.push({
      key,
      title: renderedTitle,
      status: normalizeSeverity(status),
      detail: renderedDetail,
      evidence: Array.isArray(evidence) ? evidence.slice(0, 2) : [],
    });
  };

  pushItem('urbanistica', 'Regolarita urbanistica', fieldStates.regolarita_urbanistica?.status, fieldStates.regolarita_urbanistica?.value, fieldStates.regolarita_urbanistica?.evidence);
  pushItem('catasto', 'Conformita catastale', fieldStates.conformita_catastale?.status, fieldStates.conformita_catastale?.value, fieldStates.conformita_catastale?.evidence);
  pushItem('agibilita', 'Agibilita', fieldStates.agibilita?.status, fieldStates.agibilita?.value, fieldStates.agibilita?.evidence);
  pushItem('occupazione', 'Stato occupativo', fieldStates.stato_occupativo?.status, fieldStates.stato_occupativo?.value, fieldStates.stato_occupativo?.evidence);

  section9.forEach((item, index) => {
    pushItem(
      `section9-${index}`,
      item?.killer || item?.label_it || item?.label,
      item?.status_it || item?.status,
      item?.reason_it || item?.action_required_it || item?.action,
      item?.evidence
    );
  });

  return items;
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
        superficie: safeRender(lot?.superficie_mq, '') ? `${safeRender(lot?.superficie_mq, '')} mq` : '',
        valoreStima: formatMoney(lot?.valore_stima_eur),
        topEvidence: getPrimaryEvidence(evidenceObj?.ubicazione, evidenceObj?.tipologia, evidenceObj?.superficie, evidenceObj?.valore_stima),
        detailRows: [
          { label: 'Diritto reale', value: safeRender(lot?.diritto_reale, ''), evidence: getPrimaryEvidence(evidenceObj?.diritto_reale) },
          { label: 'Quota / diritti condivisi', value: sharedRightsNote, evidence: getPrimaryEvidence(evidenceObj?.diritto_reale, evidenceObj?.note) },
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
  const dirittoReale = safeRender(result.dati_certi_del_lotto?.diritto_reale || result.section_4_dati_certi?.diritto_reale, '');

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
      const catastoFromEvidence = parseCatastoFromEvidence(evidenceObj?.catasto);
      const catastoValue = catastoFromEvidence || formatCatastoCompact(bene?.catasto);
      const catastoEvidence = evidenceMatchesCatasto(evidenceObj?.catasto, catastoValue)
        ? getPrimaryEvidence(evidenceObj?.catasto)
        : [];
      const agibilitaDetail = buildAgibilitaDetail(bene, abusi, fieldStates);
      const detailRows = [
        { label: 'Diritto reale', value: dirittoReale, evidence: [] },
        { label: 'Stato occupativo', value: safeRender(pickFirstNonEmpty(bene?.stato_occupativo, occupativo?.status_it, occupativo?.status), ''), evidence: getPrimaryEvidence(evidenceObj?.occupancy_status, fieldStates.stato_occupativo) },
        { label: 'Catasto', value: catastoValue, evidence: catastoEvidence },
        { label: 'Urbanistica', value: safeRender(pickFirstNonEmpty(bene?.urbanistica, bene?.conformita_urbanistica, abusi?.conformita_urbanistica?.status), ''), evidence: getPrimaryEvidence(evidenceObj?.urbanistica, fieldStates.regolarita_urbanistica) },
        { label: 'Agibilita / Abitabilita', value: agibilitaDetail.value, note: agibilitaDetail.note, evidence: agibilitaDetail.evidence },
        { label: 'Stato conservativo', value: safeRender(bene?.stato_conservativo?.status_it || bene?.stato_conservativo?.general_condition_it || bene?.stato_conservativo, ''), evidence: getPrimaryEvidence(bene?.stato_conservativo, evidenceObj?.stato_conservativo) },
        { label: 'APE', value: safeRender(pickFirstNonEmpty(bene?.ape, abusi?.ape?.status, fieldStates.ape?.value), ''), evidence: getPrimaryEvidence(evidenceObj?.ape, fieldStates.ape) },
      ].filter((row) => row.value);

      const impiantiRows = ['elettrico', 'idrico', 'termico']
        .map((key) => ({
          label: key[0].toUpperCase() + key.slice(1),
          value: safeRender(bene?.impianti?.[key]?.status_it || bene?.impianti?.[key]?.status || bene?.impianti?.[key], ''),
          evidence: getPrimaryEvidence(bene?.impianti?.[key], evidenceObj?.impianti?.[key]),
        }))
        .filter((row) => row.value);

      const declarationRows = [
        { label: 'Dichiarazione impianto elettrico', value: safeRender(bene?.dichiarazioni?.dichiarazione_impianto_elettrico || bene?.dichiarazioni_impianti?.elettrico, ''), evidence: getPrimaryEvidence(evidenceObj?.dichiarazioni?.dichiarazione_impianto_elettrico, evidenceObj?.dichiarazioni_impianti?.elettrico) },
        { label: 'Dichiarazione impianto termico', value: safeRender(bene?.dichiarazioni?.dichiarazione_impianto_termico || bene?.dichiarazioni_impianti?.termico, ''), evidence: getPrimaryEvidence(evidenceObj?.dichiarazioni?.dichiarazione_impianto_termico, evidenceObj?.dichiarazioni_impianti?.termico) },
        { label: 'Dichiarazione impianto idrico', value: safeRender(bene?.dichiarazioni?.dichiarazione_impianto_idrico || bene?.dichiarazioni_impianti?.idrico, ''), evidence: getPrimaryEvidence(evidenceObj?.dichiarazioni?.dichiarazione_impianto_idrico, evidenceObj?.dichiarazioni_impianti?.idrico) },
      ].filter((row) => row.value);

      return {
        key: `bene-${bene?.bene_number || index + 1}`,
        title: `Bene ${bene?.bene_number || index + 1}${safeRender(bene?.tipologia, '') ? ` - ${safeRender(bene?.tipologia, '')}` : ''}`,
        location: safeRender(pickFirstNonEmpty(bene?.short_location, bene?.ubicazione, bene?.indirizzo), ''),
        piano: safeRender(bene?.piano, ''),
        superficie: parseNumericEuro(bene?.superficie_mq) !== null ? `${parseNumericEuro(bene?.superficie_mq).toLocaleString('it-IT')} mq` : safeRender(bene?.superficie_mq, ''),
        valoreStima: formatMoney(pickFirstNonEmpty(bene?.valore_stima_eur, bene?.valore_stima_bene, bene?.valore_di_stima_bene)),
        topEvidence: getPrimaryEvidence(evidenceObj?.location_piano, evidenceObj?.superficie_mq, evidenceObj?.valore_stima_eur),
        detailRows,
        impiantiRows,
        declarationRows,
      };
    });
};

const buildFlags = (result) => {
  const rawFlags = Array.isArray(result.section_11_red_flags) && result.section_11_red_flags.length > 0
    ? result.section_11_red_flags
    : (Array.isArray(result.red_flags_operativi) ? result.red_flags_operativi : []);
  const userMessages = Array.isArray(result.user_messages) ? result.user_messages : [];
  const fieldStates = result.field_states || {};
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

  if (normalizeComparableText(fieldStates.regolarita_urbanistica?.value).includes('difform')) {
    pushItem(
      'flag-urbanistica',
      'Difformita urbanistiche e catastali',
      'La perizia segnala incongruenze nello stato di fatto e nella conformita catastale.',
      'AMBER',
      mergeEvidence(fieldStates.regolarita_urbanistica, fieldStates.conformita_catastale)
    );
  }
  if (normalizeComparableText(fieldStates.agibilita?.value).includes('assente')) {
    pushItem(
      'flag-agibilita',
      'Agibilita da chiarire prima dell\'offerta',
      'La documentazione segnala assenza o incertezza sulla abitabilita/agibilita.',
      'AMBER',
      getEvidence(fieldStates.agibilita)
    );
  }
  if (normalizeComparableText(fieldStates.stato_occupativo?.value).includes('occupato')) {
    pushItem(
      'flag-occupazione',
      'Immobile occupato',
      'Lo stato occupativo richiede verifica operativa su liberazione e tempi.',
      'AMBER',
      getEvidence(fieldStates.stato_occupativo)
    );
  }
  if (fieldStates.dati_asta?.status === 'NOT_FOUND') {
    pushItem(
      'flag-dati-asta',
      'Dati asta mancanti nel documento',
      'Data e ora asta vanno confermate sul portale ufficiale della procedura.',
      'AMBER',
      getEvidence(fieldStates.dati_asta)
    );
  }

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

  userMessages.forEach((msg, index) => {
    const severity = safeRender(msg?.severity, '').toUpperCase();
    if (!['WARNING', 'ERROR', 'CRITICAL', 'AMBER', 'RED'].includes(severity)) return;
    const label = normalizeComparableText(msg?.title_it || msg?.code);
    if (label.includes('revisione manuale') || label.includes('manual_review')) return;
    pushItem(
      `message-${index}`,
      msg?.title_it || msg?.code,
      msg?.body_it || msg?.reason_it,
      msg?.severity,
      msg?.evidence
    );
  });

  const severityOrder = { Critico: 0, Attenzione: 1, Info: 2, 'Da verificare': 3 };
  return items.sort((a, b) => (severityOrder[a.severity] || 9) - (severityOrder[b.severity] || 9));
};

export const buildPeriziaPrintReportModel = (rawAnalysis) => {
  const analysis = normalizeAnalysisResponse(rawAnalysis) || {};
  const result = analysis.result || {};
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
  const details = buildDetails(result);
  const costBuckets = buildCostBuckets(result, panoramicaContract);
  const legalItems = buildLegalItems(result);
  const flags = buildFlags(result);
  const coverSummaryIt = safeRender(
    pickFirstNonEmpty(
      narratedDecision.it,
      decision.summary_it,
      summary.summary_it
    ),
    ''
  );
  const overviewDecisionIt = safeRender(
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
        { label: 'Prezzo base', value: formatMoney(valuation.prezzo_base_eur || lotSummary.prezzo_base_eur) },
      ].filter((item) => item.value && item.value !== MISSING_TEXT),
      composition: lotComposition.map((item, index) => ({
        key: `overview-bene-${index}`,
        title: item?.lot_number
          ? `Lotto ${item.lot_number}`
          : (item?.bene_number ? `Bene ${item.bene_number}` : `Bene ${index + 1}`),
        type: safeRender(item?.tipologia, ''),
        location: safeRender(item?.short_location || item?.ubicazione, ''),
        piano: safeRender(item?.piano, ''),
        superficie: parseNumericEuro(item?.superficie_mq) !== null ? `${parseNumericEuro(item?.superficie_mq).toLocaleString('it-IT')} mq` : '',
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
