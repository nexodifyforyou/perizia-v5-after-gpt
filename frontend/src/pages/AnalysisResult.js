import React, { useState, useEffect } from 'react';
import { useParams, Link, useNavigate, useLocation } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { Sidebar, SemaforoBadge } from './Dashboard';
import { Button } from '../components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../components/ui/tabs';
import { EvidenceBadge, EvidenceDetail, DataValueWithEvidence } from '../components/EvidenceDisplay';
import HeadlineVerifyModal from '../components/HeadlineVerifyModal';
import { 
  FileText, 
  AlertTriangle, 
  CheckCircle, 
  XCircle,
  HelpCircle,
  DollarSign,
  Scale,
  Home,
  Users,
  FileCheck,
  ArrowLeft,
  FileDown,
  Quote,
  Trash2,
  X
} from 'lucide-react';
import axios from 'axios';
import { toast } from 'sonner';
import { downloadPdfBlob } from '../utils/pdfDownload';
import { parseSurfaceNumber } from '../lib/surfaceFormatting';

const API_URL = process.env.REACT_APP_BACKEND_URL;

// Helper function to normalize placeholder values
const normalizePlaceholder = (value) => {
  if (value === null || value === undefined) return 'Non specificato in perizia';
  if (typeof value === 'string') {
    const upper = value.toUpperCase();
    if (upper === 'NONE' || upper === 'N/A' || upper === 'NOT_SPECIFIED_IN_PERIZIA' || 
        upper === 'NOT_SPECIFIED' || upper === 'UNKNOWN' || value === '') {
      return 'Non specificato in perizia';
    }
  }
  return value;
};

const normalizeSpacedOCR = (value) => {
  if (typeof value !== 'string') return value;
  const tokens = value.split(/\s+/).filter(Boolean);
  const out = [];
  let buffer = [];
  const flush = () => {
    if (buffer.length > 0) {
      out.push(buffer.join(''));
      buffer = [];
    }
  };
  tokens.forEach((token) => {
    if (token.length === 1 && /[A-Z]/.test(token)) {
      buffer.push(token);
    } else {
      flush();
      out.push(token);
    }
  });
  flush();
  return out.join(' ').replace(/\s{2,}/g, ' ').trim();
};

const normalizeAnalysisResponse = (payload) => {
  if (!payload || typeof payload !== 'object') return payload;
  if (payload.result) return { ...payload, __result_path: 'result' };

  const candidates = [
    payload.analysis,
    payload.data,
    payload.payload,
    payload.analysis?.data,
    payload.analysis?.payload,
    payload.data?.analysis
  ].filter(Boolean);

  for (const candidate of candidates) {
    if (candidate && typeof candidate === 'object' && candidate.result) {
      const resultPath =
        candidate === payload.analysis ? 'analysis.result' :
        candidate === payload.data ? 'data.result' :
        candidate === payload.payload ? 'payload.result' :
        candidate === payload.analysis?.data ? 'analysis.data.result' :
        candidate === payload.analysis?.payload ? 'analysis.payload.result' :
        candidate === payload.data?.analysis ? 'data.analysis.result' :
        'unknown.result';
      return { ...payload, ...candidate, result: candidate.result, __result_path: resultPath };
    }
  }

  if (payload.analysis && typeof payload.analysis === 'object') {
    return { ...payload, ...payload.analysis, __result_path: 'analysis' };
  }
  if (payload.data && typeof payload.data === 'object') {
    return { ...payload, ...payload.data, __result_path: 'data' };
  }

  return payload;
};

// Helper function to safely render any value - replaces placeholders
const safeRender = (value, fallback = 'Non specificato in perizia') => {
  const normalized = normalizePlaceholder(value);
  if (normalized === 'Non specificato in perizia') return fallback === 'N/A' ? 'Non specificato in perizia' : fallback;
  
  if (typeof normalized === 'string') return normalized;
  if (typeof normalized === 'number') return normalized.toString();
  if (typeof normalized === 'boolean') return normalized ? 'Sì' : 'No';
  if (Array.isArray(normalized)) {
    if (normalized.length === 0) return fallback;
    return normalized.map(v => safeRender(v, '')).filter(Boolean).join(', ') || fallback;
  }
  if (typeof normalized === 'object') {
    if (normalized.value !== undefined) return safeRender(normalized.value, fallback);
    if (normalized.status) return safeRender(normalized.status, fallback);
    if (normalized.formatted) return safeRender(normalized.formatted, fallback);
    if (normalized.label_it) return safeRender(normalized.label_it, fallback);
    if (normalized.full) return safeRender(normalized.full, fallback);
    return fallback;
  }
  return String(normalized) || fallback;
};

// Format money value - handles TBD and numbers
const formatMoney = (value) => {
  if (value === 'TBD' || value === null || value === undefined) return 'TBD';
  if (typeof value === 'number') return `€${value.toLocaleString()}`;
  if (typeof value === 'string' && !isNaN(parseFloat(value))) {
    return `€${parseFloat(value).toLocaleString()}`;
  }
  return value;
};

const AREA_FORMATTER = new Intl.NumberFormat('it-IT', {
  minimumFractionDigits: 0,
  maximumFractionDigits: 2
});

const parseNumericEuro = (value) => {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value !== 'string') return null;
  const cleaned = value.replace(/[^\d,.-]/g, '').trim();
  if (!cleaned) return null;

  const lastComma = cleaned.lastIndexOf(',');
  const lastDot = cleaned.lastIndexOf('.');
  const decimalIndex = Math.max(lastComma, lastDot);

  if (decimalIndex === -1) {
    const parsed = parseFloat(cleaned);
    return Number.isFinite(parsed) ? parsed : null;
  }

  const decimalDigits = cleaned.slice(decimalIndex + 1).replace(/[^\d]/g, '');
  const hasExplicitDecimal = decimalDigits.length > 0 && decimalDigits.length <= 2;
  const decimalSeparator = decimalIndex === lastComma ? ',' : '.';
  const thousandsSeparator = decimalSeparator === ',' ? '.' : ',';

  const normalized = hasExplicitDecimal
    ? `${cleaned.slice(0, decimalIndex).replace(new RegExp(`\\${thousandsSeparator}`, 'g'), '')}.${decimalDigits}`
    : cleaned.replace(/[.,]/g, '');

  const parsed = parseFloat(normalized);
  return Number.isFinite(parsed) ? parsed : null;
};

const isMeaningfulValue = (value) => {
  if (value === null || value === undefined) return false;
  if (typeof value === 'string') {
    const upper = value.trim().toUpperCase();
    return Boolean(
      upper &&
      !['NONE', 'N/A', 'NOT_SPECIFIED_IN_PERIZIA', 'NOT_SPECIFIED', 'UNKNOWN', 'TBD', 'NULL', 'NON SPECIFICATO IN PERIZIA'].includes(upper)
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

const normalizeUiSeverity = (value, fallback = 'AMBER') => {
  const normalized = safeRender(value, fallback).toUpperCase();
  if (['RED', 'ROSSO', 'CRITICAL', 'CRITICO', 'ERROR'].includes(normalized)) return 'RED';
  if (['INFO', 'LOW', 'GREEN', 'VERDE', 'OK'].includes(normalized)) return 'INFO';
  return 'AMBER';
};

const formatMeasuredValue = (value, defaultUnit = '') => {
  const rendered = safeRender(value, '').trim();
  if (!rendered) return '';
  const numeric = parseSurfaceNumber(rendered);
  if (numeric === null) return rendered;
  const explicitUnit = /m²/i.test(rendered) ? 'm²' : (/\bmq\b/i.test(rendered) ? 'mq' : '');
  const unit = explicitUnit || defaultUnit;
  return `${AREA_FORMATTER.format(numeric)}${unit ? ` ${unit}` : ''}`.trim();
};

const formatSurfaceDisplay = (...values) => {
  for (const value of values) {
    if (!isMeaningfulValue(value)) continue;
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      const measured = formatMeasuredValue(
        pickFirstNonEmpty(
          value?.formatted,
          value?.value,
          value?.detail_it,
          value?.status_it,
          value?.status
        ),
        safeRender(pickFirstNonEmpty(value?.unit, value?.uom), '').trim() || 'mq'
      );
      if (measured) return measured;
    }
    const measured = formatMeasuredValue(value, 'mq');
    if (measured) return measured;
  }
  return '';
};

const normalizeComparableText = (value) => safeRender(value, '')
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .replace(/\s+/g, ' ')
  .trim()
  .toLowerCase();

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

const getShortText = (value, max = 110) => {
  const text = safeRender(value, '').trim();
  if (!text) return '';
  return text.length > max ? `${text.slice(0, max)}...` : text;
};

const pickFirstNonEmpty = (...values) => {
  for (const value of values) {
    if (value !== null && value !== undefined && value !== '') return value;
  }
  return null;
};

// Get evidence from an object that might have it
const getEvidence = (obj) => {
  if (!obj) return [];
  if (Array.isArray(obj.evidence)) return obj.evidence;
  if (Array.isArray(obj)) return obj.filter(e => e.page || e.quote);
  return [];
};

const getItemEvidence = (item) => {
  if (!item || typeof item !== 'object') return [];
  if (Array.isArray(item.evidence)) return item.evidence;
  if (Array.isArray(item.fonte_perizia?.evidence)) return item.fonte_perizia.evidence;
  return [];
};

const cleanEvidenceWhitespace = (text) => {
  if (typeof text !== 'string') return '';
  return text
    .replace(/\r\n?/g, '\n')
    .replace(/[ \t]+/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .replace(/[ ]*\n[ ]*/g, '\n')
    .trim();
};

const trimEvidencePunctuationNoise = (text) => {
  if (typeof text !== 'string') return '';
  return text
    .replace(/^[\s"'`“”‘’.,;:!?()[\]{}\-–—_]+/, '')
    .replace(/[\s"'`“”‘’.,;:!?()[\]{}\-–—_]+$/, '')
    .trim();
};

const smartTruncateText = (text, maxChars = 220) => {
  if (typeof text !== 'string') return '';
  if (text.length <= maxChars) return text;
  const boundarySlice = text.slice(0, maxChars + 1);
  const cutAt = boundarySlice.lastIndexOf(' ');
  const trimmed = (cutAt > Math.floor(maxChars * 0.65) ? boundarySlice.slice(0, cutAt) : text.slice(0, maxChars)).trim();
  return `${trimmed}…`;
};

const formatPrimaryEvidenceQuote = (evidence) => {
  const first = Array.isArray(evidence) && evidence.length > 0 && evidence[0] && typeof evidence[0] === 'object'
    ? evidence[0]
    : null;
  if (!first) {
    return { quote: '', searchHint: '', pages: [] };
  }
  const cleaned = trimEvidencePunctuationNoise(cleanEvidenceWhitespace(safeRender(first.quote, '')));
  const truncated = cleaned ? smartTruncateText(cleaned, 220) : '';
  const pages = [...new Set(
    (Array.isArray(evidence) ? evidence : [])
      .map((item) => item?.page)
      .filter((page) => Number.isFinite(Number(page)))
      .map((page) => Number(page))
  )].sort((a, b) => a - b);
  return {
    quote: truncated,
    searchHint: cleanEvidenceWhitespace(safeRender(first.search_hint, '')),
    pages
  };
};

const normalizeOverviewValue = (value, fallback = 'Non specificato in perizia') => {
  if (typeof value === 'string') {
    const upper = value.trim().toUpperCase();
    if (upper === 'TBD' || upper === 'UNKNOWN') return fallback;
  }
  return safeRender(value, fallback);
};

const classifyMoneyBoxItem = (item) => {
  const evidence = getItemEvidence(item);
  const source = safeRender(item?.source, '').toUpperCase();
  const code = safeRender(item?.code || item?.voce, '').toUpperCase();
  const hasMarketRange = item?.market_range_eur && typeof item.market_range_eur === 'object' &&
    typeof item.market_range_eur.min === 'number' && typeof item.market_range_eur.max === 'number';
  const isEstimated = hasMarketRange || source === 'MARKET_ESTIMATE';
  const isDocumentBacked = !isEstimated && (
    evidence.length > 0 ||
    source === 'PERIZIA' ||
    source === 'STEP3_CANDIDATES' ||
    code.startsWith('S3C')
  );
  return { isDocumentBacked, isEstimated };
};

const PanoramicaDataValueCard = ({ label, value, evidence, valueClassName = 'text-zinc-100' }) => {
  const normalizedValue = normalizeOverviewValue(value);
  const { quote, searchHint, pages } = formatPrimaryEvidenceQuote(evidence);
  return (
    <div className="p-4 bg-zinc-950 rounded-lg border border-zinc-800">
      <div className="flex items-center justify-between gap-2">
        <p className="text-xs font-mono text-zinc-500">{label}</p>
        {pages.length > 0 && (
          <div className="flex flex-wrap justify-end gap-1">
            {pages.map((page) => (
              <span key={`${label}_p_${page}`} className="text-[10px] px-1.5 py-0.5 rounded border border-gold/40 text-gold font-mono">
                p.{page}
              </span>
            ))}
          </div>
        )}
      </div>
      <p className={`mt-1 font-medium ${valueClassName}`}>{normalizedValue}</p>
      {quote && (
        <blockquote className="mt-2 border-l-2 border-gold/30 pl-2 text-xs text-zinc-400 italic whitespace-pre-wrap">
          "{quote}"
        </blockquote>
      )}
      {searchHint && (
        <p className="mt-1 text-[11px] text-zinc-500 whitespace-pre-wrap">
          Trova: {searchHint}
        </p>
      )}
    </div>
  );
};

const normalizeEstrattoSections = (estrattoQuality) => {
  const rawSections = Array.isArray(estrattoQuality?.sections)
    ? estrattoQuality.sections
    : (estrattoQuality?.sections && typeof estrattoQuality.sections === 'object'
      ? Object.values(estrattoQuality.sections)
      : []);

  return rawSections.map((section, sectionIndex) => {
    const sectionItems = Array.isArray(section?.items)
      ? section.items
      : (section?.fields && typeof section.fields === 'object'
        ? Object.entries(section.fields).map(([key, value]) => ({ key, ...(value || {}) }))
        : []);
    return {
      ...section,
      __key: section?.key || section?.name || section?.title_it || `section_${sectionIndex}`,
      __title: section?.title_it || section?.name || `Sezione ${sectionIndex + 1}`,
      __items: sectionItems.map((item, itemIndex) => ({
        ...item,
        __label: item?.label_it || item?.label || item?.name || item?.key || `Voce ${itemIndex + 1}`,
        __value: item?.value ?? item?.formatted ?? item?.status ?? item?.text ?? item?.voce ?? '',
        __evidence: getItemEvidence(item)
      }))
    };
  });
};

const getSemaforoLabels = (statusRaw) => {
  const status = safeRender(statusRaw, 'AMBER').toUpperCase();
  if (status === 'GREEN') return { it: 'BASSO RISCHIO', en: 'LOW RISK' };
  if (status === 'RED') return { it: 'ALTO RISCHIO', en: 'HIGH RISK' };
  return { it: 'ATTENZIONE', en: 'CAUTION' };
};

const getInjectedMessageStyle = (severityRaw) => {
  const severity = safeRender(severityRaw, 'INFO').toUpperCase();
  if (severity === 'WARNING' || severity === 'BLOCKER') {
    return {
      severity,
      badgeClass: 'bg-amber-500/20 text-amber-300 border-amber-500/40',
      cardClass: 'border-amber-500/40 bg-amber-500/5'
    };
  }
  return {
    severity,
    badgeClass: 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40',
    cardClass: 'border-emerald-500/40 bg-emerald-500/5'
  };
};

const MessageInjectionCard = ({ msg }) => {
  if (!msg || typeof msg !== 'object') return null;
  const style = getInjectedMessageStyle(msg?.severity);
  const evidence = Array.isArray(msg?.evidence) ? msg.evidence : [];
  const nextStepsIt = Array.isArray(msg?.next_steps_it) ? msg.next_steps_it : [];
  const nextStepsEn = Array.isArray(msg?.next_steps_en) ? msg.next_steps_en : [];
  const firstEvidence = evidence[0];

  return (
    <div className={`mb-4 p-4 rounded-lg border ${style.cardClass}`}>
      <div className="flex items-center justify-between gap-3 mb-2">
        <p className="text-sm font-semibold text-zinc-100">{safeRender(msg?.title_it, 'Messaggio')}</p>
        <span className={`px-2 py-0.5 text-[10px] font-mono rounded border ${style.badgeClass}`}>{style.severity}</span>
      </div>
      <p className="text-sm text-zinc-300">{safeRender(msg?.body_it, '')}</p>
      <p className="text-xs text-zinc-500 mt-1">{safeRender(msg?.title_en, '')}</p>
      <p className="text-xs text-zinc-500">{safeRender(msg?.body_en, '')}</p>
      {nextStepsIt.length > 0 && (
        <ul className="list-disc list-inside text-sm text-zinc-300 mt-3 space-y-1">
          {nextStepsIt.map((step, stepIdx) => (
            <li key={`it_${safeRender(msg?.code, 'MSG')}_${stepIdx}`}>{safeRender(step, '')}</li>
          ))}
        </ul>
      )}
      {nextStepsEn.length > 0 && (
        <ul className="list-disc list-inside text-xs text-zinc-500 mt-2 space-y-1">
          {nextStepsEn.map((step, stepIdx) => (
            <li key={`en_${safeRender(msg?.code, 'MSG')}_${stepIdx}`}>{safeRender(step, '')}</li>
          ))}
        </ul>
      )}
      {firstEvidence?.page && firstEvidence?.quote && (
        <p className="text-xs text-zinc-500 mt-3 italic border-l-2 border-gold/30 pl-2">
          p.{firstEvidence.page} — {safeRender(firstEvidence.quote, '')}
        </p>
      )}
    </div>
  );
};

// Money Box Item Component with Evidence - supports both old and ROMA STANDARD formats
const MoneyBoxItem = ({ item }) => {
  const [expandedEvidence, setExpandedEvidence] = useState(false);
  const evidence = getItemEvidence(item);
  const hasEvidence = evidence.length > 0;
  const label = item.label_it || item.voce || item.label || 'Voce';
  const code = safeRender(item.code || item.voce, '');
  const euroValue = parseNumericEuro(item.stima_euro);
  const marketRange = item?.market_range_eur && typeof item.market_range_eur === 'object'
    ? item.market_range_eur
    : null;
  const hasMarketRange = marketRange && typeof marketRange.min === 'number' && typeof marketRange.max === 'number';
  const source = safeRender(item?.source, '').toUpperCase();
  const isVerde = hasEvidence || source === 'PERIZIA' || source === 'STEP3_CANDIDATES' || safeRender(code, '').toUpperCase().startsWith('S3C');
  const isOro = hasMarketRange || source === 'MARKET_ESTIMATE';
  const displayValue = euroValue !== null
    ? `€${euroValue.toLocaleString()}`
    : (hasMarketRange ? `€${marketRange.min.toLocaleString()} - €${marketRange.max.toLocaleString()}` : 'Non disponibile');
  const shortNota = getShortText(item.stima_nota, 110);
  const firstEvidence = evidence[0];
  const firstEvidenceQuote = safeRender(firstEvidence?.quote, '');
  const evidencePreview = firstEvidenceQuote.length > 180 ? `${firstEvidenceQuote.slice(0, 180)}...` : firstEvidenceQuote;
  const evidenceText = expandedEvidence ? firstEvidenceQuote : evidencePreview;
  const cardToneClass = isOro
    ? 'border-gold/40 bg-gold/5'
    : (isVerde ? 'border-emerald-500/35 bg-emerald-500/5' : 'border-zinc-800 bg-zinc-950/50');
  const amountClass = isOro ? 'text-gold' : (isVerde ? 'text-emerald-300' : 'text-zinc-300');

  return (
    <div className={`p-4 rounded-lg border ${cardToneClass}`}>
      <div className="flex items-start justify-between gap-3 mb-2">
        <div className="flex-1">
          <p className="text-sm font-medium text-zinc-100">
            {code ? `${code} · ` : ''}{label}
          </p>
          <div className="mt-1 flex items-center gap-2">
            {isVerde && !isOro && (
              <span className="inline-block text-[10px] px-2 py-0.5 rounded border border-emerald-500/40 text-emerald-300">
                Verde
              </span>
            )}
            {isOro && (
              <span className="inline-block text-[10px] px-2 py-0.5 rounded border border-gold/40 text-gold">
                Oro
              </span>
            )}
          </div>
          {hasMarketRange && euroValue === null && (
            <span className="inline-block mt-1 text-[10px] px-2 py-0.5 rounded border border-gold/40 text-gold">
              Stima Nexodify
            </span>
          )}
          {shortNota && <p className="text-xs text-zinc-500 mt-1">{shortNota}</p>}
        </div>
        <div className="text-right">
          <span className={`font-mono text-sm font-bold ${amountClass}`}>{displayValue}</span>
        </div>
      </div>
      {hasEvidence ? (
        <div className="mt-2 p-2 bg-zinc-900 rounded border-l-2 border-gold/30">
          <p className="text-xs text-zinc-400 italic whitespace-pre-wrap">
            {firstEvidence?.page ? `p.${firstEvidence.page} — ` : ''}
            {evidenceText || 'Evidenza disponibile'}
          </p>
          {firstEvidenceQuote.length > 180 && (
            <button
              type="button"
              onClick={() => setExpandedEvidence((prev) => !prev)}
              className="mt-1 text-[11px] text-gold hover:underline"
            >
              {expandedEvidence ? 'Mostra meno' : 'Mostra evidenza completa'}
            </button>
          )}
        </div>
      ) : (
        hasMarketRange && (
          <p className="text-xs text-zinc-500 italic mt-2">(non presente in perizia)</p>
        )
      )}
    </div>
  );
};

// Legal Killer Item Component with Evidence - supports both old and ROMA STANDARD formats
const LegalKillerItem = ({ name, data }) => {
  const status = normalizeUiSeverity(data?.status, 'AMBER');
  const evidence = getEvidence(data);
  const hasEvidence = evidence.length > 0;
  const pages = hasEvidence ? [...new Set(evidence.map(e => e.page).filter(Boolean))] : [];
  
  const getStatusIcon = (status) => {
    if (status === 'RED') return <XCircle className="w-5 h-5 text-red-400" />;
    if (status === 'INFO') return <HelpCircle className="w-5 h-5 text-zinc-400" />;
    return <AlertTriangle className="w-5 h-5 text-amber-400" />;
  };

  const getStatusBg = (status) => {
    if (status === 'RED') return 'bg-red-500/10 border-red-500/30';
    if (status === 'INFO') return 'bg-zinc-800/80 border-zinc-700';
    return 'bg-amber-500/10 border-amber-500/30';
  };

  const formatName = (name) => {
    // If it's a long killer name from new format, use it directly
    if (name.includes(' ')) return name;
    // Old format - convert underscore to space
    return name.replace(/_/g, ' ').split(' ').map(w => 
      w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()
    ).join(' ');
  };
  
  // Get action text - new format uses 'action', old uses 'action_required_it'
  const actionText = data?.action || data?.action_required_it || '';
  
  // Get the display name - use 'killer' from new format or formatted name
  const displayName = data?.killer || formatName(name);
  const contextLabel = safeRender(data?.contextLabel, '');

  return (
    <div className={`p-4 rounded-lg border ${getStatusBg(status)}`}>
      <div className="flex items-start gap-3">
        {getStatusIcon(status)}
        <div className="flex-1">
          <div className="flex items-center gap-2">
            <p className="text-sm font-medium text-zinc-100">{displayName}</p>
            {contextLabel && (
              <span className="text-[10px] px-2 py-0.5 rounded border border-zinc-700 text-zinc-400 uppercase tracking-wide">
                {contextLabel}
              </span>
            )}
            {hasEvidence && (
              <span className="text-xs font-mono text-gold flex items-center gap-1">
                <FileText className="w-3 h-3" />
                p. {pages.join(', ')}
              </span>
            )}
          </div>
          {actionText && (
            <p className="text-xs text-zinc-500 mt-1">{safeRender(actionText)}</p>
          )}
          {hasEvidence && evidence[0]?.quote && (
            <p className="text-xs text-zinc-400 mt-2 italic border-l-2 border-gold/20 pl-2">
              "{evidence[0].quote.substring(0, 100)}..."
            </p>
          )}
        </div>
        <span className="font-mono text-xs px-2 py-1 rounded bg-zinc-800">{status}</span>
      </div>
    </div>
  );
};

// Red Flag Item Component with Evidence
const RedFlagItem = ({ flag }) => {
  const severity = safeRender(flag.severity, 'AMBER');
  const evidence = getEvidence(flag);
  const hasEvidence = evidence.length > 0;
  const pages = hasEvidence ? [...new Set(evidence.map(e => e.page).filter(Boolean))] : [];
  
  return (
    <div className={`p-4 rounded-lg border ${
      severity === 'RED' ? 'border-red-500/30 bg-red-500/5' : 'border-amber-500/30 bg-amber-500/5'
    }`}>
      <div className="flex items-start gap-3">
        <AlertTriangle className={`w-5 h-5 flex-shrink-0 ${
          severity === 'RED' ? 'text-red-400' : 'text-amber-400'
        }`} />
        <div className="flex-1">
          <div className="flex items-center gap-2 flex-wrap">
            <p className="text-sm font-medium text-zinc-100">{safeRender(flag.flag_it, flag.message_it || flag.code || 'Flag')}</p>
            {hasEvidence && (
              <span className="text-xs font-mono text-gold flex items-center gap-1">
                <FileText className="w-3 h-3" />
                p. {pages.join(', ')}
              </span>
            )}
          </div>
          <p className="text-xs text-zinc-500 mt-1">{safeRender(flag.flag_en, flag.message_en || '')}</p>
          {(flag.action_it || flag.action_en) && (
            <p className="text-xs text-zinc-400 mt-2">
              <span className="text-gold">Azione:</span> {safeRender(flag.action_it, flag.action_en || '')}
            </p>
          )}
          {hasEvidence && evidence[0]?.quote && (
            <div className="mt-2 p-2 bg-zinc-900 rounded border-l-2 border-gold/30">
              <p className="text-xs text-zinc-400 italic">"{evidence[0].quote}"</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

const RedFlagMatrixItem = ({ item }) => {
  const severity = normalizeUiSeverity(item?.severity, 'AMBER');
  const evidence = Array.isArray(item?.evidence) ? item.evidence : [];
  const pages = [...new Set(evidence.map((e) => e?.page).filter(Boolean))];
  const toneClass = severity === 'RED'
    ? 'border-red-500/30 bg-red-500/5'
    : severity === 'INFO'
      ? 'border-zinc-700 bg-zinc-800/60'
      : 'border-amber-500/30 bg-amber-500/5';
  const kindLabel = safeRender(item?.kindLabel, '');

  return (
    <div className={`p-3 rounded-lg border ${toneClass}`}>
      <div className="flex items-start justify-between gap-2">
        <div>
          <div className="flex flex-wrap items-center gap-2">
            <p className="text-sm font-medium text-zinc-100">{safeRender(item?.label, 'Rischio')}</p>
            {kindLabel && (
              <span className="text-[10px] px-2 py-0.5 rounded border border-zinc-700 text-zinc-400 uppercase tracking-wide">
                {kindLabel}
              </span>
            )}
          </div>
          {item?.explanation && <p className="text-xs text-zinc-400 mt-1">{safeRender(item.explanation, '')}</p>}
        </div>
        {evidence.length > 0 && <EvidenceBadge evidence={evidence} />}
      </div>
      {pages.length > 0 && (
        <p className="text-[11px] text-zinc-500 mt-2">
          Evidenza: p. {pages.join(', ')}
        </p>
      )}
    </div>
  );
};

// Multi-Lot Selector Component
const MultiLotSelector = ({ lots, selectedLot, onSelectLot }) => {
  if (!lots || lots.length <= 1) return null;
  
  return (
      <div className="mb-6 rounded-lg border border-gold/30 bg-gradient-to-r from-gold/10 to-amber-500/10 p-4">
      <div className="flex items-center gap-2 mb-3">
        <Home className="w-5 h-5 text-gold" />
        <h3 className="text-lg font-semibold text-zinc-100">Perizia Multi-Lotto ({lots.length} lotti)</h3>
      </div>
      
      {/* Compact Lots Table */}
      <div className="mb-4 overflow-x-auto">
        <table className="responsive-data-table text-sm">
          <thead>
            <tr className="border-b border-zinc-700">
              <th className="text-left py-2 px-3 text-zinc-400">Lotto</th>
              <th className="text-left py-2 px-3 text-zinc-400">Prezzo Base</th>
              <th className="text-left py-2 px-3 text-zinc-400">Ubicazione</th>
              <th className="text-left py-2 px-3 text-zinc-400">Superficie</th>
              <th className="text-left py-2 px-3 text-zinc-400">Diritto</th>
            </tr>
          </thead>
          <tbody>
            {lots.map((lot, idx) => (
              <tr 
                key={lot.lot_number || idx}
                className={`border-b border-zinc-800 cursor-pointer transition-colors ${
                  selectedLot === idx ? 'bg-gold/20' : 'hover:bg-zinc-800/50'
                }`}
                onClick={() => onSelectLot(idx)}
              >
                <td className="py-2 px-3 font-mono text-gold">Lotto {lot.lot_number}</td>
                <td className="py-2 px-3 font-mono text-emerald-400">{lot.prezzo_base_eur || 'TBD'}</td>
                <td className="py-2 px-3 text-zinc-300">{(lot.ubicazione || 'NON SPECIFICATO').substring(0, 40)}...</td>
                <td className="py-2 px-3 text-zinc-300">{formatSurfaceDisplay(lot.superficie_convenzionale_mq, lot.superficie_convenzionale, lot.superficie_mq) || 'TBD'}</td>
                <td className="py-2 px-3 text-zinc-300">{splitQuotaFromDiritto(lot.diritto_reale || 'NON SPECIFICATO', lot.quota).substring(0, 20)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      
      {/* Lot Selector Dropdown */}
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
        <label className="text-sm text-zinc-400">Seleziona Lotto:</label>
        <select 
          value={selectedLot}
          onChange={(e) => onSelectLot(parseInt(e.target.value))}
          className="bg-zinc-900 border border-zinc-700 rounded-lg px-4 py-2 text-zinc-100 focus:border-gold focus:outline-none"
        >
          {lots.map((lot, idx) => (
            <option key={lot.lot_number || idx} value={idx}>
              Lotto {lot.lot_number} - {lot.prezzo_base_eur || 'TBD'}
            </option>
          ))}
        </select>
      </div>
    </div>
  );
};

const AnalysisResult = () => {
  const { analysisId } = useParams();
  const location = useLocation();
  const navigate = useNavigate();
  const { user, logout } = useAuth();
  const [analysis, setAnalysis] = useState(null);
  const [loading, setLoading] = useState(true);
  const [downloading, setDownloading] = useState(false);
  const [showDeleteModal, setShowDeleteModal] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const [selectedLotIndex, setSelectedLotIndex] = useState(0);
  const [activeTab, setActiveTab] = useState('overview');
  const [headlineModal, setHeadlineModal] = useState({ open: false, fieldKey: null });

  const fetchAnalysis = async () => {
    const params = new URLSearchParams(location.search);
    if (params.get('debug') === '1' && typeof window !== 'undefined' && window.__DEBUG_ANALYSIS_PAYLOAD__) {
      setAnalysis(normalizeAnalysisResponse(window.__DEBUG_ANALYSIS_PAYLOAD__));
      setLoading(false);
      return;
    }
    try {
      const response = await axios.get(`${API_URL}/api/analysis/perizia/${analysisId}`, {
        withCredentials: true
      });
      setAnalysis(normalizeAnalysisResponse(response.data));
    } catch (error) {
      toast.error('Impossibile caricare l\'analisi');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchAnalysis();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [analysisId]);

  const openHeadlineModal = (fieldKey) => {
    setHeadlineModal({ open: true, fieldKey });
  };

  const closeHeadlineModal = () => {
    setHeadlineModal({ open: false, fieldKey: null });
  };

  const handleDownloadPDF = async () => {
    setDownloading(true);
    try {
      const downloadJson = async () => {
        let payload = analysis;
        try {
          const jsonResponse = await axios.get(`${API_URL}/api/analysis/perizia/${analysisId}`, {
            withCredentials: true
          });
          payload = jsonResponse.data;
        } catch (jsonError) {
          if (!payload) throw jsonError;
        }

        const jsonBlob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json;charset=utf-8' });
        const url = window.URL.createObjectURL(jsonBlob);
        const link = document.createElement('a');
        link.href = url;
        link.setAttribute('download', `perizia_${analysisId}.json`);
        document.body.appendChild(link);
        link.click();
        link.remove();
        window.URL.revokeObjectURL(url);
      };

      let pdfBlob = null;
      let contentType = '';

      try {
        const response = await axios.get(`${API_URL}/api/analysis/perizia/${analysisId}/pdf`, {
          withCredentials: true,
          responseType: 'blob'
        });
        pdfBlob = response.data;
        contentType = (response.headers?.['content-type'] || '').toLowerCase();
      } catch (pdfError) {
        pdfBlob = null;
      }

      let hasPdfHeader = false;
      if (pdfBlob instanceof Blob) {
        try {
          const signatureBuffer = await pdfBlob.slice(0, 4).arrayBuffer();
          const signatureBytes = new Uint8Array(signatureBuffer);
          hasPdfHeader = String.fromCharCode(...signatureBytes) === '%PDF';
        } catch (signatureError) {
          hasPdfHeader = false;
        }
      }

      if (contentType.includes('application/pdf') && hasPdfHeader) {
        downloadPdfBlob(pdfBlob, analysisId);
        toast.success('Report scaricato!');
        return;
      }

      console.info(`[DownloadFallback] Invalid PDF response for ${analysisId}; downloading JSON fallback.`);
      toast.info('PDF non disponibile: scarico JSON. / PDF unavailable: downloading JSON.');
      await downloadJson();
    } catch (error) {
      toast.error('Errore durante il download');
    } finally {
      setDownloading(false);
    }
  };

  const handleDelete = async () => {
    setIsDeleting(true);
    try {
      await axios.delete(`${API_URL}/api/analysis/perizia/${analysisId}`, {
        withCredentials: true
      });
      toast.success('Analisi eliminata con successo');
      navigate('/history');
    } catch (error) {
      toast.error('Errore durante l\'eliminazione');
    } finally {
      setIsDeleting(false);
      setShowDeleteModal(false);
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-[#09090b] flex items-center justify-center">
        <div className="text-center">
          <div className="w-12 h-12 border-4 border-gold border-t-transparent rounded-full animate-spin mx-auto mb-4"></div>
          <p className="text-zinc-400">Caricamento analisi...</p>
        </div>
      </div>
    );
  }

  if (!analysis) {
    return (
      <div className="min-h-screen bg-[#09090b]">
        <Sidebar user={user} logout={logout} />
        <main className="px-4 pb-8 pt-24 sm:px-6 lg:ml-64 lg:px-8 lg:pt-8">
          <div className="text-center py-16">
            <FileText className="w-16 h-16 text-zinc-700 mx-auto mb-4" />
            <h2 className="text-2xl font-serif text-zinc-100 mb-2">Analisi non trovata</h2>
            <Link to="/history" className="text-gold hover:underline">Torna allo storico</Link>
          </div>
        </main>
      </div>
    );
  }

  // Extract result data - ROMA STANDARD format with backwards compatibility
  const result = analysis.result || {};
  
  // NEW ROMA STANDARD sections (primary)
  const reportHeader = result.report_header || {};
  const section1 = result.section_1_semaforo_generale || {};
  const section2 = result.section_2_decisione_rapida || {};
  const section3 = result.section_3_money_box || {};
  const section4 = result.section_4_dati_certi || {};
  const section5 = result.section_5_abusi_conformita || {};
  const section6 = result.section_6_stato_occupativo || {};
  const section7 = result.section_7_stato_conservativo || {};
  const section8 = result.section_8_formalita || {};
  const section9 = result.section_9_legal_killers || {};
  const section10 = result.section_10_indice_convenienza || {};
  const section11 = result.section_11_red_flags || [];
  const section12 = result.section_12_checklist_pre_offerta || [];
  const summaryData = result.summary_for_client || {};
  const qaPass = result.qa_pass || {};
  
  // Get lots array for multi-lot support
  const lots = result.lots || [];
  const isMultiLot = lots.length > 1;
  const detailScope = safeRender(result.detail_scope, '').toUpperCase();
  const isLotFirstDetailScope = detailScope === 'LOT_FIRST' && isMultiLot;
  const selectedLot = lots[selectedLotIndex] || null;
  
  // Map to display variables - prioritize NEW format, fallback to OLD
  const caseHeader = reportHeader.procedure ? reportHeader : (result.case_header || {});
  const semaforo = section1.status ? section1 : (result.semaforo_generale || {});
  const decision = section2.summary_it ? section2 : (result.decision_rapida_client || {});
  const narratedDecision = (result.decision_rapida_narrated && typeof result.decision_rapida_narrated === 'object')
    ? result.decision_rapida_narrated
    : null;
  const hasNarratedDecision = Boolean(narratedDecision && (narratedDecision.it || narratedDecision.en));
  const decisionSourceLabel = hasNarratedDecision ? 'Narrated' : 'Deterministic';
  const decisionIt = hasNarratedDecision
    ? safeRender(narratedDecision.it, 'Analisi completata')
    : safeRender(decision.summary_it, 'Analisi completata');
  const decisionEn = hasNarratedDecision
    ? safeRender(narratedDecision.en, '')
    : safeRender(decision.summary_en, '');
  const decisionBulletsIt = hasNarratedDecision && Array.isArray(narratedDecision.bullets_it)
    ? narratedDecision.bullets_it.filter((b) => typeof b === 'string' && b.trim())
    : [];
  const decisionBulletsEn = hasNarratedDecision && Array.isArray(narratedDecision.bullets_en)
    ? narratedDecision.bullets_en.filter((b) => typeof b === 'string' && b.trim())
    : [];
  const moneyBox = section3.items ? section3 : (result.money_box || {});
  const estrattoQuality = result.estratto_quality || {};
  
  // For multi-lot: use selected lot data, otherwise use section4
  const dati = selectedLot ? {
    prezzo_base_asta: { value: selectedLot.prezzo_base_value, formatted: selectedLot.prezzo_base_eur, evidence: selectedLot.evidence?.prezzo_base || [] },
    ubicazione: { value: selectedLot.ubicazione, evidence: selectedLot.evidence?.ubicazione || [] },
    diritto_reale: {
      value: pickFirstNonEmpty(selectedLot.diritto_reale, section4?.diritto_reale?.value, section4?.diritto_reale, result.dati_certi_del_lotto?.diritto_reale?.value, result.dati_certi_del_lotto?.diritto_reale),
      evidence: selectedLot.evidence?.diritto_reale || getEvidence(section4?.diritto_reale) || []
    },
    quota: {
      value: pickFirstNonEmpty(selectedLot.quota, section4?.quota?.value, section4?.quota, result.dati_certi_del_lotto?.quota?.value, result.dati_certi_del_lotto?.quota),
      evidence: selectedLot.evidence?.quota || getEvidence(section4?.quota) || selectedLot.evidence?.diritto_reale || []
    },
    superficie: {
      value: pickFirstNonEmpty(
        selectedLot.superficie_convenzionale_mq,
        selectedLot.superficie_convenzionale,
        section4?.superficie?.value,
        section4?.superficie,
        result.dati_certi_del_lotto?.superficie?.value,
        result.dati_certi_del_lotto?.superficie,
        selectedLot.superficie_mq
      ),
      evidence: selectedLot.evidence?.superficie || getEvidence(section4?.superficie) || []
    },
    superficie_catastale: { value: selectedLot.superficie_mq, evidence: selectedLot.evidence?.superficie || [] },
    tipologia: { value: selectedLot.tipologia, evidence: [] }
  } : (section4.prezzo_base_asta ? section4 : (result.dati_certi_del_lotto || {}));
  
  const abusi = section5.conformita_urbanistica ? section5 : (result.abusi_edilizi_conformita || {});
  const occupativo = section6.status ? section6 : (result.stato_occupativo || {});
  const conservativo = section7.condizione_generale ? section7 : (result.stato_conservativo || {});
  const formalita = section8.ipoteche ? section8 : (result.formalita || {});
  const legalKillerItems = Array.isArray(section9.top_items) && section9.top_items.length > 0
    ? section9.top_items
    : (Array.isArray(section9.items) ? section9.items : []);
  const legalKillers = legalKillerItems.length > 0
    ? { ...section9, items: legalKillerItems }
    : (result.legal_killers_checklist || {});
  const indice = section10.prezzo_base ? section10 : (result.indice_di_convenienza || {});
  const redFlags = Array.isArray(section11) && section11.length > 0 ? section11 : 
                   (Array.isArray(result.red_flags_operativi) ? result.red_flags_operativi : []);
  const checklist = Array.isArray(section12) && section12.length > 0 ? section12 : 
                    (Array.isArray(result.checklist_pre_offerta) ? result.checklist_pre_offerta : []);
  const summary = summaryData;
  const qa = qaPass.status ? qaPass : (result.qa || {});
  const fieldStates = result.field_states || {};
  const datiAsta = result.dati_asta || result.dati_certi_del_lotto?.dati_asta || result.dati_certi?.dati_asta;
  const resultPathUsed = analysis?.__result_path || (analysis?.result ? 'result' : null);
  const estrattoSections = normalizeEstrattoSections(estrattoQuality);
  const estrattoSectionMap = estrattoSections.reduce((acc, section) => {
    const key = safeRender(section?.heading_key || section?.__key, '').toLowerCase();
    if (key) acc[key] = section;
    return acc;
  }, {});
  const estrattoSectionDefs = [
    { key: 'occupancy', headingIt: 'Stato occupativo', headingEn: 'Occupancy status' },
    { key: 'ape', headingIt: 'APE', headingEn: 'Energy certificate' },
    { key: 'abusi_agibilita', headingIt: 'Abusi/Agibilità', headingEn: 'Building issues/Habitability' },
    { key: 'impianti', headingIt: 'Stato Conservativo/Impianti', headingEn: 'Condition/Systems' },
    { key: 'catasto', headingIt: 'Catasto', headingEn: 'Cadastral details' },
    { key: 'formalita', headingIt: 'Formalità', headingEn: 'Liens and encumbrances' },
    { key: 'dati_asta', headingIt: 'Dati asta', headingEn: 'Auction data' }
  ];
  const estrattoLegalSection = estrattoSectionMap.legal || null;
  const userMessages = Array.isArray(result.user_messages) ? result.user_messages : [];
  const userMessagesByCode = userMessages.reduce((acc, msg) => {
    const code = safeRender(msg?.code, '').toUpperCase();
    if (!code) return acc;
    if (!acc[code]) acc[code] = [];
    acc[code].push(msg);
    return acc;
  }, {});
  const docTextOkMessage = (userMessagesByCode.DOC_TEXT_OK || [])[0] || null;
  const semaforoStatus = safeRender(semaforo.status || result?.semaforo?.status, 'AMBER').toUpperCase();
  const semaforoLabels = getSemaforoLabels(semaforoStatus);
  const panoramicaContract = result.panoramica_contract && typeof result.panoramica_contract === 'object'
    ? result.panoramica_contract
    : null;
  const contractLotSummary = panoramicaContract?.lot_summary && typeof panoramicaContract.lot_summary === 'object'
    ? panoramicaContract.lot_summary
    : null;
  const contractLotSummaryEvidence = contractLotSummary?.evidence && typeof contractLotSummary.evidence === 'object'
    ? contractLotSummary.evidence
    : {};

  const getFieldState = (key) => fieldStates?.[key] || null;
  const getFieldEvidence = (key, fallback) => {
    const state = getFieldState(key);
    if (state && Array.isArray(state.evidence) && state.evidence.length > 0) return state.evidence;
    return getEvidence(fallback);
  };
  const getFieldValue = (key, fallback) => {
    const state = getFieldState(key);
    if (state && state.value !== undefined && state.value !== null && state.value !== '') {
      return state.value;
    }
    if (key === 'tribunale') return normalizeSpacedOCR(fallback);
    return fallback;
  };
  const formatFieldStateDisplay = (key, fallback) => {
    const state = getFieldState(key);
    if (state?.status === 'NOT_FOUND') return 'Non specificato in perizia';
    return safeRender(getFieldValue(key, fallback), 'Non specificato in perizia');
  };

  const beni = (selectedLot && selectedLot.beni) || result.beni || [];
  const legacyLotCompositionItems = (Array.isArray(beni) ? beni : [])
    .map((bene, idx) => {
      const beneNumber = pickFirstNonEmpty(bene?.bene_number, bene?.numero_bene, bene?.numero, bene?.bene_id, idx + 1);
      const tipologia = pickFirstNonEmpty(bene?.tipologia, bene?.type, bene?.tipo, bene?.categoria, bene?.categoria_catastale);
      const locationRaw = pickFirstNonEmpty(
        bene?.ubicazione,
        bene?.indirizzo,
        bene?.address?.value,
        bene?.address,
        bene?.localizzazione,
        bene?.piano ? `Piano ${safeRender(bene.piano, '')}` : null
      );
      const superficieRaw = pickFirstNonEmpty(
        bene?.superficie_convenzionale_mq,
        bene?.superficie_convenzionale,
        bene?.superficie_mq,
        bene?.mq,
        bene?.superficie
      );
      const superficieDisplay = formatSurfaceDisplay(superficieRaw) || safeRender(superficieRaw, '');
      const valoreStimaRaw = pickFirstNonEmpty(
        bene?.valore_di_stima_bene,
        bene?.valore_stima_bene,
        bene?.valore_di_stima,
        bene?.valore_stima,
        bene?.stima_euro,
        bene?.valore_euro,
        bene?.valore
      );
      const valoreStimaValue = parseNumericEuro(valoreStimaRaw);
      const valoreStimaDisplay = valoreStimaValue !== null
        ? `€${valoreStimaValue.toLocaleString()}`
        : safeRender(valoreStimaRaw, '');
      const evidence = getEvidence(bene);
      const hasData = Boolean(
        tipologia ||
        locationRaw ||
        (superficieDisplay && superficieDisplay !== 'Non specificato in perizia') ||
        (valoreStimaDisplay && valoreStimaDisplay !== 'Non specificato in perizia')
      );
      return {
        key: bene?.bene_id || bene?.bene_number || idx,
        title: beneNumber ? `Bene ${beneNumber}` : `Bene ${idx + 1}`,
        tipologia: safeRender(tipologia, ''),
        location: safeRender(locationRaw, ''),
        superficie: superficieDisplay,
        valoreStima: valoreStimaDisplay,
        evidence,
        hasData
      };
    })
    .filter((item) => item.hasData);
  const contractLotComposition = Array.isArray(panoramicaContract?.lot_composition)
    ? panoramicaContract.lot_composition
    : [];
  const contractLotCompositionItems = contractLotComposition.map((item, idx) => {
    const evidenceObj = item?.evidence && typeof item.evidence === 'object' ? item.evidence : {};
    const condensedEvidence = [
      ...(Array.isArray(evidenceObj.location_piano) ? evidenceObj.location_piano : []),
      ...(Array.isArray(evidenceObj.superficie_mq) ? evidenceObj.superficie_mq : []),
      ...(Array.isArray(evidenceObj.valore_stima_eur) ? evidenceObj.valore_stima_eur : [])
    ].slice(0, 2);
    const superficieValue = parseNumericEuro(item?.superficie_mq);
    const valoreStimaValue = parseNumericEuro(item?.valore_stima_eur);
    return {
      key: item?.bene_number ?? idx,
      title: item?.bene_number ? `Bene ${item.bene_number}` : `Bene ${idx + 1}`,
      tipologia: safeRender(item?.tipologia, ''),
      location: safeRender(item?.short_location, ''),
      piano: safeRender(item?.piano, ''),
      superficie: formatSurfaceDisplay(item?.superficie_mq) || '',
      valoreStima: valoreStimaValue !== null ? `€${valoreStimaValue.toLocaleString()}` : '',
      evidence: condensedEvidence,
      hasData: Boolean(
        item?.bene_number ||
        item?.tipologia ||
        item?.short_location ||
        item?.piano ||
        superficieValue !== null ||
        valoreStimaValue !== null
      )
    };
  }).filter((item) => item.hasData);
  const lotCompositionItems = contractLotCompositionItems.length > 0
    ? contractLotCompositionItems
    : legacyLotCompositionItems;

  const mergeEvidence = (...lists) => {
    const out = [];
    const seen = new Set();
    lists.forEach((list) => {
      if (!Array.isArray(list)) return;
      list.forEach((entry) => {
        if (!entry || typeof entry !== 'object') return;
        const key = `${entry.page || ''}|${safeRender(entry.quote, '').slice(0, 120)}|${safeRender(entry.search_hint, '').slice(0, 120)}`;
        if (seen.has(key)) return;
        seen.add(key);
        out.push(entry);
      });
    });
    return out;
  };

  const getFieldLegacyDetail = (value) => {
    if (!value || typeof value !== 'object') return null;
    return pickFirstNonEmpty(value?.detail_it, value?.status_it, value?.status, value?.formatted, value?.value);
  };

  const getFieldStateDisplayValue = (state) => {
    if (!state || typeof state !== 'object') return null;
    const raw = pickFirstNonEmpty(
      state?.detail_it,
      state?.status_it,
      state?.formatted,
      state?.value?.detail_it,
      state?.value?.status_it,
      state?.value?.formatted,
      state?.value,
      state?.status
    );
    return isMeaningfulValue(raw) ? raw : null;
  };

  const getRichFieldDisplayValue = (key, legacyValue, ...fallbacks) => {
    const stateValue = getFieldStateDisplayValue(getFieldState(key));
    const legacyDetail = getFieldLegacyDetail(legacyValue);
    if (key === 'superficie') {
      return normalizeDettagliValue(formatSurfaceDisplay(stateValue, legacyDetail, ...fallbacks));
    }
    return normalizeDettagliValue(pickFirstNonEmpty(stateValue, legacyDetail, ...fallbacks));
  };

  const getSurfaceDisplayValue = (key, legacyValue, ...fallbacks) =>
    key === 'superficie'
      ? normalizeOverviewValue(formatSurfaceDisplay(getRichFieldDisplayValue(key, legacyValue, ...fallbacks)))
      : normalizeOverviewValue(getRichFieldDisplayValue(key, legacyValue, ...fallbacks));

  const formatCatastoCompact = (catasto) => {
    if (!catasto) return '';
    if (typeof catasto === 'string') return safeRender(catasto, '');
    if (typeof catasto !== 'object') return '';
    const renderEntry = (entry, options = {}) => {
      const fg = safeRender(pickFirstNonEmpty(entry?.foglio, catasto?.foglio), '').trim();
      const part = safeRender(pickFirstNonEmpty(entry?.particella, catasto?.particella), '').trim();
      const sub = safeRender(pickFirstNonEmpty(entry?.sub, entry?.subalterno, entry?.subalterno_numero, entry?.numero), '').trim();
      const cat = safeRender(pickFirstNonEmpty(entry?.categoria, entry?.categoria_catastale), '').trim();
      const parts = [];
      if (!options.omitShared && fg) parts.push(`Fg. ${fg}`);
      if (!options.omitShared && part) parts.push(`Part. ${part}`);
      if (sub) parts.push(`Sub. ${sub}`);
      if (cat) parts.push(`Cat. ${cat}`);
      return parts.join(' - ');
    };

    const subalterni = Array.isArray(catasto.subalterni)
      ? catasto.subalterni.map((entry) => renderEntry(entry, { omitShared: true })).filter(Boolean)
      : [];
    const rootValue = renderEntry(catasto);
    const sharedPrefix = renderEntry(catasto, { omitShared: false })
      .split(' - ')
      .filter((part) => part.startsWith('Fg.') || part.startsWith('Part.'));

    if (subalterni.length > 0) {
      const renderedSubs = [...new Set(subalterni)];
      if (sharedPrefix.length > 0) {
        return `${sharedPrefix.join(' - ')} - ${renderedSubs.join('; ')}`;
      }
      return renderedSubs.join('; ');
    }
    return rootValue;
  };

  const normalizeDettagliValue = (value) => {
    const text = safeRender(value, '').trim();
    if (!text) return '';
    const upper = text.toUpperCase();
    if (
      upper === 'TBD' ||
      upper === 'UNKNOWN' ||
      upper === 'NOT_SPECIFIED' ||
      upper === 'NOT_SPECIFIED_IN_PERIZIA' ||
      upper === 'NON SPECIFICATO IN PERIZIA'
    ) {
      return '';
    }
    return text;
  };

  const getDettagliStatusValue = (value) => {
    if (value === null || value === undefined) return '';
    if (typeof value === 'object' && !Array.isArray(value)) {
      return normalizeDettagliValue(
        pickFirstNonEmpty(
          value?.status_it,
          value?.status,
          value?.value_it,
          value?.value,
          value?.label_it,
          value?.label,
          value?.description_it,
          value?.description
        )
      );
    }
    return normalizeDettagliValue(value);
  };

  const normalizeDeclarationKey = (value) => safeRender(value, '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]/g, '');

  const extractDeclarationValueFromSources = (sources, aliases) => {
    for (const source of sources) {
      if (!source || typeof source !== 'object') continue;
      for (const [key, raw] of Object.entries(source)) {
        const normalized = normalizeDeclarationKey(key);
        if (!aliases.some((alias) => normalized === alias || normalized.includes(alias))) continue;
        const rendered = getDettagliStatusValue(raw);
        if (rendered) return rendered;
      }
    }
    return '';
  };

  const extractDeclarationEvidenceFromSources = (sources, aliases) => {
    const lists = [];
    sources.forEach((source) => {
      if (!source || typeof source !== 'object') return;
      Object.entries(source).forEach(([key, raw]) => {
        const normalized = normalizeDeclarationKey(key);
        const match = aliases.some((alias) => normalized === alias || normalized.includes(alias));
        if (!match) return;
        if (Array.isArray(raw)) {
          lists.push(raw);
          return;
        }
        if (raw && typeof raw === 'object') {
          const directEvidence = getEvidence(raw);
          if (directEvidence.length > 0) {
            lists.push(directEvidence);
          }
          Object.entries(raw).forEach(([nestedKey, nestedRaw]) => {
            const nestedNormalized = normalizeDeclarationKey(nestedKey);
            if (!aliases.some((alias) => nestedNormalized === alias || nestedNormalized.includes(alias))) return;
            if (Array.isArray(nestedRaw)) lists.push(nestedRaw);
            if (nestedRaw && typeof nestedRaw === 'object') {
              const nestedEvidence = getEvidence(nestedRaw);
              if (nestedEvidence.length > 0) lists.push(nestedEvidence);
            }
          });
        }
      });
    });
    return mergeEvidence(...lists);
  };

  const selectedLotBeni = Array.isArray(selectedLot?.beni) ? selectedLot.beni : [];
  const fallbackLotBeni = Array.isArray(lots?.[selectedLotIndex]?.beni)
    ? lots[selectedLotIndex].beni
    : (Array.isArray(lots?.[0]?.beni) ? lots[0].beni : []);
  const sourceBeniList = selectedLotBeni.length > 0
    ? selectedLotBeni
    : (fallbackLotBeni.length > 0 ? fallbackLotBeni : (Array.isArray(result?.beni) ? result.beni : []));

  const sourceBeneByNumber = new Map();
  sourceBeniList.forEach((bene, idx) => {
    const number = parseNumericEuro(pickFirstNonEmpty(bene?.bene_number, bene?.numero_bene, bene?.numero, idx + 1));
    if (number !== null) sourceBeneByNumber.set(number, bene);
  });
  const contractBeneByNumber = new Map();
  contractLotComposition.forEach((bene, idx) => {
    const number = parseNumericEuro(pickFirstNonEmpty(bene?.bene_number, idx + 1));
    if (number !== null) contractBeneByNumber.set(number, bene);
  });
  const beneNumbers = [...new Set([...contractBeneByNumber.keys(), ...sourceBeneByNumber.keys()])].sort((a, b) => a - b);

  const detailsBeneCards = beneNumbers.map((beneNumber, idx) => {
    const contractBene = contractBeneByNumber.get(beneNumber) || {};
    const sourceBene = sourceBeneByNumber.get(beneNumber) || {};
    const sourceEvidenceObj = sourceBene?.evidence && typeof sourceBene.evidence === 'object' ? sourceBene.evidence : {};
    const contractEvidenceObj = contractBene?.evidence && typeof contractBene.evidence === 'object' ? contractBene.evidence : {};

    const tipologia = safeRender(pickFirstNonEmpty(contractBene?.tipologia, sourceBene?.tipologia), '').trim();
    const shortLocation = safeRender(pickFirstNonEmpty(contractBene?.short_location, sourceBene?.short_location, sourceBene?.ubicazione, sourceBene?.indirizzo), '').trim();
    const piano = safeRender(pickFirstNonEmpty(contractBene?.piano, sourceBene?.piano), '').trim();
    const superficieRaw = pickFirstNonEmpty(
      sourceBene?.superficie_convenzionale_mq,
      sourceBene?.superficie_convenzionale,
      contractBene?.superficie_convenzionale_mq,
      contractBene?.superficie_convenzionale,
      sourceBene?.superficie_mq,
      contractBene?.superficie_mq,
      sourceBene?.superficie,
      beneNumbers.length === 1 ? dati?.superficie?.value : null,
      beneNumbers.length === 1 ? getFieldState('superficie')?.value?.value : null,
      beneNumbers.length === 1 ? getFieldState('superficie')?.value : null
    );
    const superficieDisplay = formatSurfaceDisplay(superficieRaw);
    const valoreNum = parseNumericEuro(pickFirstNonEmpty(contractBene?.valore_stima_eur, sourceBene?.valore_stima_eur, sourceBene?.valore_stima_bene, sourceBene?.valore_di_stima_bene, sourceBene?.valore_stima));

    const catastoValue = formatCatastoCompact(pickFirstNonEmpty(sourceBene?.catasto, contractBene?.catasto)) || parseCatastoFromEvidence(sourceEvidenceObj?.catasto);
    const quotaValue = getRichFieldDisplayValue(
      'quota',
      null,
      sourceBene?.quota,
      contractBene?.quota,
      dati?.quota?.value,
      dati?.quota
    );
    const dirittoRealeValue = splitQuotaFromDiritto(
      getRichFieldDisplayValue(
        'diritto_reale',
        null,
        sourceBene?.diritto_reale,
        sourceBene?.diritto,
        contractBene?.diritto_reale,
        dati?.diritto_reale?.value,
        dati?.diritto_reale
      ),
      quotaValue
    );
    const statoOccupativoValue = safeRender(pickFirstNonEmpty(sourceBene?.occupancy_status, sourceBene?.stato_occupativo, sourceBene?.occupazione_status, sourceBene?.occupazione, occupativo?.status_it, occupativo?.status), '').trim();
    const urbanisticaValue = getRichFieldDisplayValue(
      'regolarita_urbanistica',
      abusi?.conformita_urbanistica,
      sourceBene?.urbanistica,
      sourceBene?.regolarita_urbanistica,
      sourceBene?.conformita_urbanistica
    );
    const agibilitaValue = safeRender(pickFirstNonEmpty(sourceBene?.agibilita, sourceBene?.abitabilita, abusi?.agibilita?.status), '').trim();
    const statoConservativoValue = getDettagliStatusValue(
      pickFirstNonEmpty(sourceBene?.stato_conservativo, contractBene?.stato_conservativo)
    );
    const declarationSources = [
      sourceBene?.dichiarazioni_impianti,
      sourceBene?.dichiarazioni,
      contractBene?.dichiarazioni_impianti,
      contractBene?.dichiarazioni
    ];
    const declarationEvidenceSources = [
      sourceEvidenceObj?.dichiarazioni_impianti,
      sourceEvidenceObj?.dichiarazioni,
      contractEvidenceObj?.dichiarazioni_impianti,
      contractEvidenceObj?.dichiarazioni
    ];
    const apeValue = getRichFieldDisplayValue('ape', abusi?.ape, sourceBene?.ape, contractBene?.ape);
    const elettricoValue = extractDeclarationValueFromSources(declarationSources, ['impiantoelettrico', 'dichiarazioneimpiantoelettrico', 'elettrico', 'conformitaelettrico']);
    const termicoValue = extractDeclarationValueFromSources(declarationSources, ['impiantotermico', 'dichiarazioneimpiantotermico', 'termico', 'conformitatermico']);
    const idricoValue = extractDeclarationValueFromSources(declarationSources, ['impiantoidrico', 'dichiarazioneimpiantoidrico', 'idrico', 'conformitaidrico']);
    const gasValue = getRichFieldDisplayValue(
      'dichiarazione_impianto_gas',
      abusi?.impianti?.gas,
      extractDeclarationValueFromSources(declarationSources, ['impiantogas', 'dichiarazioneimpiantogas', 'gas', 'conformitagas'])
    );
    const apeEvidence = mergeEvidence(
      sourceEvidenceObj?.ape,
      contractEvidenceObj?.ape,
      sourceEvidenceObj?.dichiarazioni_impianti?.ape,
      sourceEvidenceObj?.dichiarazioni?.ape,
      contractEvidenceObj?.dichiarazioni_impianti?.ape,
      contractEvidenceObj?.dichiarazioni?.ape,
      extractDeclarationEvidenceFromSources(declarationEvidenceSources, ['ape', 'attestatodiprestazioneenergetica', 'certificazioneenergetica']),
      getFieldEvidence('ape', abusi?.ape)
    );
    const elettricoEvidence = mergeEvidence(
      sourceEvidenceObj?.dichiarazioni_impianti?.elettrico,
      sourceEvidenceObj?.dichiarazioni?.dichiarazione_impianto_elettrico,
      contractEvidenceObj?.dichiarazioni_impianti?.elettrico,
      contractEvidenceObj?.dichiarazioni?.dichiarazione_impianto_elettrico,
      extractDeclarationEvidenceFromSources(declarationEvidenceSources, ['impiantoelettrico', 'dichiarazioneimpiantoelettrico', 'elettrico', 'conformitaelettrico'])
    );
    const termicoEvidence = mergeEvidence(
      sourceEvidenceObj?.dichiarazioni_impianti?.termico,
      sourceEvidenceObj?.dichiarazioni?.dichiarazione_impianto_termico,
      contractEvidenceObj?.dichiarazioni_impianti?.termico,
      contractEvidenceObj?.dichiarazioni?.dichiarazione_impianto_termico,
      extractDeclarationEvidenceFromSources(declarationEvidenceSources, ['impiantotermico', 'dichiarazioneimpiantotermico', 'termico', 'conformitatermico'])
    );
    const idricoEvidence = mergeEvidence(
      sourceEvidenceObj?.dichiarazioni_impianti?.idrico,
      sourceEvidenceObj?.dichiarazioni?.dichiarazione_impianto_idrico,
      contractEvidenceObj?.dichiarazioni_impianti?.idrico,
      contractEvidenceObj?.dichiarazioni?.dichiarazione_impianto_idrico,
      extractDeclarationEvidenceFromSources(declarationEvidenceSources, ['impiantoidrico', 'dichiarazioneimpiantoidrico', 'idrico', 'conformitaidrico'])
    );
    const gasEvidence = mergeEvidence(
      sourceEvidenceObj?.dichiarazioni_impianti?.gas,
      sourceEvidenceObj?.dichiarazioni?.dichiarazione_impianto_gas,
      contractEvidenceObj?.dichiarazioni_impianti?.gas,
      contractEvidenceObj?.dichiarazioni?.dichiarazione_impianto_gas,
      extractDeclarationEvidenceFromSources(declarationEvidenceSources, ['impiantogas', 'dichiarazioneimpiantogas', 'gas', 'conformitagas']),
      getFieldEvidence('dichiarazione_impianto_gas', abusi?.impianti?.gas)
    );
    const statoConservativoEvidence = mergeEvidence(
      sourceBene?.stato_conservativo?.evidence,
      contractBene?.stato_conservativo?.evidence,
      sourceEvidenceObj?.stato_conservativo,
      contractEvidenceObj?.stato_conservativo
    );
    const impiantiValueObj = pickFirstNonEmpty(sourceBene?.impianti, contractBene?.impianti);
    const impiantiEvidenceObj = mergeEvidence(
      sourceEvidenceObj?.impianti?.elettrico,
      sourceEvidenceObj?.impianti?.idrico,
      sourceEvidenceObj?.impianti?.termico,
      contractEvidenceObj?.impianti?.elettrico,
      contractEvidenceObj?.impianti?.idrico,
      contractEvidenceObj?.impianti?.termico
    );
    const impiantiRows = [
      {
        key: 'impianto_elettrico',
        label: 'Elettrico',
        value: getDettagliStatusValue(impiantiValueObj?.elettrico),
        evidence: mergeEvidence(
          sourceBene?.impianti?.elettrico?.evidence,
          contractBene?.impianti?.elettrico?.evidence,
          sourceEvidenceObj?.impianti?.elettrico,
          contractEvidenceObj?.impianti?.elettrico
        )
      },
      {
        key: 'impianto_idrico',
        label: 'Idrico',
        value: getDettagliStatusValue(impiantiValueObj?.idrico),
        evidence: mergeEvidence(
          sourceBene?.impianti?.idrico?.evidence,
          contractBene?.impianti?.idrico?.evidence,
          sourceEvidenceObj?.impianti?.idrico,
          contractEvidenceObj?.impianti?.idrico
        )
      },
      {
        key: 'impianto_termico',
        label: 'Termico',
        value: getDettagliStatusValue(impiantiValueObj?.termico),
        evidence: mergeEvidence(
          sourceBene?.impianti?.termico?.evidence,
          contractBene?.impianti?.termico?.evidence,
          sourceEvidenceObj?.impianti?.termico,
          contractEvidenceObj?.impianti?.termico
        )
      }
    ];

    return {
      key: `dettagli-bene-${beneNumber || idx + 1}`,
      beneNumber: beneNumber || idx + 1,
      tipologia,
      shortLocation,
      piano,
      superficie: superficieDisplay || '',
      valoreStima: valoreNum !== null ? `€${valoreNum.toLocaleString()}` : '',
      topEvidence: mergeEvidence(
        contractEvidenceObj?.tipologia,
        contractEvidenceObj?.location_piano,
        contractEvidenceObj?.superficie_mq,
        contractEvidenceObj?.valore_stima_eur,
        sourceEvidenceObj?.tipologia,
        sourceEvidenceObj?.location_piano,
        sourceEvidenceObj?.superficie,
        sourceEvidenceObj?.valore_stima
      ),
      detailRows: [
        {
          key: 'diritto_reale',
          label: 'Diritto reale',
          value: dirittoRealeValue,
          evidence: mergeEvidence(sourceEvidenceObj?.diritto_reale, dati?.diritto_reale?.evidence, getFieldEvidence('diritto_reale', dati?.diritto_reale))
        },
        {
          key: 'quota',
          label: 'Quota',
          value: quotaValue,
          evidence: mergeEvidence(sourceEvidenceObj?.quota, contractEvidenceObj?.quota, dati?.quota?.evidence, sourceEvidenceObj?.diritto_reale)
        },
        {
          key: 'stato_occupativo',
          label: 'Stato occupativo',
          value: statoOccupativoValue,
          evidence: mergeEvidence(sourceEvidenceObj?.occupancy_status, sourceEvidenceObj?.occupazione, getFieldEvidence('stato_occupativo', occupativo))
        },
        {
          key: 'catasto',
          label: 'Catasto',
          value: catastoValue,
          evidence: mergeEvidence(sourceEvidenceObj?.catasto)
        },
        {
          key: 'urbanistica',
          label: 'Urbanistica',
          value: urbanisticaValue,
          evidence: mergeEvidence(sourceEvidenceObj?.urbanistica, getFieldEvidence('regolarita_urbanistica', abusi?.conformita_urbanistica))
        },
        {
          key: 'agibilita',
          label: 'Agibilità / Abitabilità',
          value: agibilitaValue,
          evidence: mergeEvidence(sourceEvidenceObj?.agibilita, sourceEvidenceObj?.note, getFieldEvidence('agibilita', abusi?.agibilita))
        },
        {
          key: 'stato_conservativo',
          label: 'Stato conservativo',
          value: statoConservativoValue,
          evidence: statoConservativoEvidence
        },
      ],
      impiantiRows,
      impiantiTopEvidence: impiantiEvidenceObj,
      declarationRows: [
        {
          key: 'ape',
          label: 'APE',
          value: apeValue,
          evidence: apeEvidence
        },
        {
          key: 'dichiarazione_impianto_elettrico',
          label: 'Dichiarazione impianto elettrico',
          value: elettricoValue,
          evidence: elettricoEvidence
        },
        {
          key: 'dichiarazione_impianto_termico',
          label: 'Dichiarazione impianto termico',
          value: termicoValue,
          evidence: termicoEvidence
        },
        {
          key: 'dichiarazione_impianto_idrico',
          label: 'Dichiarazione impianto idrico',
          value: idricoValue,
          evidence: idricoEvidence
        },
        {
          key: 'dichiarazione_impianto_gas',
          label: 'Dichiarazione impianto gas',
          value: gasValue,
          evidence: gasEvidence
        }
      ]
    };
  });

  const buildSharedRightsNote = (lot) => {
    if (!lot || typeof lot !== 'object') return '';
    const directNote = safeRender(
      pickFirstNonEmpty(
        lot.shared_rights_note,
        lot.shared_rights,
        lot.quota_note,
        lot.note_diritto,
        lot.notes_diritto
      ),
      ''
    ).trim();
    if (directNote) return directNote;
    const notes = Array.isArray(lot.risk_notes) ? lot.risk_notes : [];
    const match = notes.find((note) => {
      const text = safeRender(note, '').toLowerCase();
      return text.includes('stradella') || text.includes('strada privata') || text.includes('quota 1/4') || text.includes('corte comune');
    });
    return safeRender(match, '').trim();
  };

  const detailsLotCards = isLotFirstDetailScope
    ? lots.map((lot, idx) => {
        const lotEvidence = lot?.evidence && typeof lot.evidence === 'object' ? lot.evidence : {};
        const lotRiskNotes = Array.isArray(lot?.risk_notes)
          ? lot.risk_notes.map((note) => safeRender(note, '')).filter(Boolean)
          : [];
        const subordinateBeni = Array.isArray(lot?.beni)
          ? lot.beni.map((bene, beneIdx) => ({
              key: `${lot?.lot_number || idx + 1}-${bene?.bene_number || beneIdx + 1}`,
              title: bene?.bene_number ? `Bene ${bene.bene_number}` : `Bene ${beneIdx + 1}`,
              value: [
                safeRender(bene?.tipologia, ''),
                safeRender(pickFirstNonEmpty(bene?.ubicazione, bene?.indirizzo, bene?.short_location), ''),
                formatSurfaceDisplay(bene?.superficie_convenzionale_mq, bene?.superficie_convenzionale, bene?.superficie_mq)
                  ? formatSurfaceDisplay(bene?.superficie_convenzionale_mq, bene?.superficie_convenzionale, bene?.superficie_mq)
                  : ''
              ].filter(Boolean).join(' | ')
            })).filter((bene) => bene.value)
          : [];
        const sharedRightsNote = buildSharedRightsNote(lot);
        return {
          key: `lot-detail-${lot?.lot_number || idx + 1}`,
          lotNumber: lot?.lot_number || idx + 1,
          tipologia: safeRender(lot?.tipologia, ''),
          shortLocation: safeRender(lot?.ubicazione, ''),
          superficie: formatSurfaceDisplay(lot?.superficie_convenzionale_mq, lot?.superficie_convenzionale, lot?.superficie_mq) || '',
          valoreStima: lot?.valore_stima_eur ? formatMoney(lot.valore_stima_eur) : '',
          prezzoBase: lot?.prezzo_base_eur || '',
          topEvidence: mergeEvidence(
            lotEvidence?.ubicazione,
            lotEvidence?.tipologia,
            lotEvidence?.superficie,
            lotEvidence?.valore_stima,
            lotEvidence?.prezzo_base
          ),
          detailRows: [
            {
              key: 'diritto_reale',
              label: 'Diritto reale',
              value: splitQuotaFromDiritto(getRichFieldDisplayValue('diritto_reale', null, lot?.diritto_reale), getRichFieldDisplayValue('quota', null, lot?.quota)),
              evidence: mergeEvidence(lotEvidence?.diritto_reale)
            },
            {
              key: 'quota',
              label: 'Quota',
              value: getRichFieldDisplayValue('quota', null, lot?.quota),
              evidence: mergeEvidence(lotEvidence?.quota, lotEvidence?.diritto_reale)
            },
            {
              key: 'shared_rights',
              label: 'Diritti condivisi',
              value: sharedRightsNote,
              evidence: mergeEvidence(lotEvidence?.note)
            },
            {
              key: 'stato_occupativo',
              label: 'Stato occupativo',
              value: normalizeDettagliValue(lot?.occupancy_status || lot?.stato_occupativo),
              evidence: mergeEvidence(lotEvidence?.occupancy_status)
            },
            {
              key: 'catasto',
              label: 'Catasto',
              value: formatCatastoCompact(lot?.catasto) || normalizeDettagliValue(safeRender(lot?.catasto, '')),
              evidence: mergeEvidence(lotEvidence?.catasto)
            },
            {
              key: 'stato_conservativo',
              label: 'Stato conservativo',
              value: normalizeDettagliValue(lot?.stato_conservativo),
              evidence: mergeEvidence(lotEvidence?.stato_conservativo)
            },
            {
              key: 'note_rischio',
              label: 'Note / rischi principali',
              value: lotRiskNotes.slice(0, 3).join(' | '),
              evidence: mergeEvidence(lotEvidence?.note)
            }
          ].filter((row) => row.value),
          subordinateBeni
        };
      })
    : [];

  const contractWaterfall = panoramicaContract?.valuation_waterfall && typeof panoramicaContract.valuation_waterfall === 'object'
    ? panoramicaContract.valuation_waterfall
    : null;
  const waterfallStimaValue = parseNumericEuro(contractWaterfall?.valore_stima_eur);
  const waterfallDeprezzamentiValue = parseNumericEuro(contractWaterfall?.deprezzamenti_eur);
  const waterfallFinaleValue = parseNumericEuro(contractWaterfall?.valore_finale_eur);
  const waterfallPrezzoBaseValue = parseNumericEuro(contractWaterfall?.prezzo_base_eur);
  const canRenderValuationWaterfall = Boolean(contractWaterfall) && [
    waterfallStimaValue,
    waterfallDeprezzamentiValue,
    waterfallFinaleValue,
    waterfallPrezzoBaseValue
  ].every((value) => typeof value === 'number' && Number.isFinite(value));
  const waterfallEvidence = contractWaterfall?.evidence && typeof contractWaterfall.evidence === 'object'
    ? contractWaterfall.evidence
    : {};
  const waterfallStimaEvidence = Array.isArray(waterfallEvidence.valore_stima_eur) ? waterfallEvidence.valore_stima_eur : [];
  const waterfallDeprezzamentiEvidence = Array.isArray(waterfallEvidence.deprezzamenti_eur) ? waterfallEvidence.deprezzamenti_eur : [];
  const waterfallFinaleEvidence = Array.isArray(waterfallEvidence.valore_finale_eur) ? waterfallEvidence.valore_finale_eur : [];
  const waterfallPrezzoBaseEvidence = Array.isArray(waterfallEvidence.prezzo_base_eur) ? waterfallEvidence.prezzo_base_eur : [];

  const overviewTribunale = safeRender(contractLotSummary?.tribunale, '').trim()
    ? contractLotSummary.tribunale
    : formatFieldStateDisplay('tribunale', caseHeader.tribunale?.value || caseHeader.tribunale);
  const overviewProcedura = safeRender(contractLotSummary?.procedura, '').trim()
    ? contractLotSummary.procedura
    : formatFieldStateDisplay('procedura', caseHeader.procedure?.value || caseHeader.procedure_id);
  const overviewLotto = safeRender(contractLotSummary?.lotto_label, '').trim()
    ? contractLotSummary.lotto_label
    : formatFieldStateDisplay('lotto', caseHeader.lotto?.value || caseHeader.lotto);
  const overviewPrezzoBaseNum = parseNumericEuro(contractLotSummary?.prezzo_base_eur);
  const overviewPrezzoBaseValue = overviewPrezzoBaseNum !== null
    ? `€${overviewPrezzoBaseNum.toLocaleString()}`
    : normalizeOverviewValue(dati.prezzo_base_asta?.formatted || dati.prezzo_base_asta?.value || dati.prezzo_base_asta);
  const overviewTribunaleEvidence = Array.isArray(contractLotSummaryEvidence?.tribunale) && contractLotSummaryEvidence.tribunale.length > 0
    ? contractLotSummaryEvidence.tribunale
    : getFieldEvidence('tribunale', caseHeader.tribunale);
  const overviewProceduraEvidence = Array.isArray(contractLotSummaryEvidence?.procedura) && contractLotSummaryEvidence.procedura.length > 0
    ? contractLotSummaryEvidence.procedura
    : getFieldEvidence('procedura', caseHeader.procedure || caseHeader.procedure_id);
  const overviewLottoEvidence = Array.isArray(contractLotSummaryEvidence?.lotto_label) && contractLotSummaryEvidence.lotto_label.length > 0
    ? contractLotSummaryEvidence.lotto_label
    : getFieldEvidence('lotto', caseHeader.lotto);
  const overviewPrezzoBaseEvidence = Array.isArray(contractLotSummaryEvidence?.prezzo_base_eur) && contractLotSummaryEvidence.prezzo_base_eur.length > 0
    ? contractLotSummaryEvidence.prezzo_base_eur
    : getEvidence(dati.prezzo_base_asta);

  const getHeadlineStatus = (fieldKey) => fieldStates?.[fieldKey]?.status;
  const isNeedsVerification = (status) => status === 'LOW_CONFIDENCE' || status === 'NOT_FOUND';

  const getSearchedInPatterns = (fieldKey) => {
    const state = getFieldState(fieldKey);
    const searchedIn = Array.isArray(state?.searched_in) ? state.searched_in : [];
    const patterns = searchedIn.flatMap((entry) => {
      if (typeof entry === 'string') return [entry];
      if (!entry || typeof entry !== 'object') return [];
      if (Array.isArray(entry.patterns)) return entry.patterns.filter(Boolean);
      if (entry.pattern) return [entry.pattern];
      if (entry.query) return [entry.query];
      if (entry.quote) return [getShortText(entry.quote, 55)];
      return [];
    }).filter(Boolean);
    return [...new Set(patterns)].slice(0, 4);
  };

  const getHeadlineSourceValue = (fieldKey) => {
    switch (fieldKey) {
      case 'procedura':
        return caseHeader.procedure?.value || caseHeader.procedure_id;
      case 'lotto':
        return caseHeader.lotto?.value || caseHeader.lotto;
      case 'tribunale':
        return caseHeader.tribunale?.value || caseHeader.tribunale;
      case 'address':
        return caseHeader.address?.value || caseHeader.address?.full || caseHeader.address;
      default:
        return null;
    }
  };

  const getHeadlineDisplayValue = (fieldKey, fallbackValue) => {
    const status = getHeadlineStatus(fieldKey);
    if (status === 'LOW_CONFIDENCE') return 'DA VERIFICARE';
    if (status === 'NOT_FOUND') return 'Non specificato in perizia';
    return safeRender(fallbackValue, 'Non specificato in perizia');
  };

  const MissingStateRationale = ({ fieldKey, forceMissing = false }) => {
    const state = getFieldState(fieldKey);
    const missing = forceMissing || state?.status === 'NOT_FOUND' || state?.status === 'LOW_CONFIDENCE';
    if (!missing) return null;
    const patterns = getSearchedInPatterns(fieldKey);
    if (patterns.length > 0) {
      return (
        <div className="mt-2">
          <p className="text-xs text-zinc-500">Cercato nel documento: {patterns.join(', ')}</p>
          <p className="text-[11px] text-zinc-600">Searched in document: {patterns.join(', ')}</p>
        </div>
      );
    }
    return (
      <div className="mt-2">
        <p className="text-xs text-zinc-500">Nessuna evidenza trovata nel documento.</p>
        <p className="text-[11px] text-zinc-600">No evidence found in the document.</p>
      </div>
    );
  };

  const getEstrattoItemDisplayValue = (item) => {
    const explicit = safeRender(item?.value_it ?? item?.__value, '').trim();
    if (explicit) return explicit;
    const amount = parseNumericEuro(item?.amount_eur);
    if (amount !== null) return `€${amount.toLocaleString()}`;
    const dateValue = safeRender(item?.date || item?.data || item?.date_it, '').trim();
    const timeValue = safeRender(item?.time || item?.ora || item?.time_it, '').trim();
    if (dateValue || timeValue) return `${dateValue}${dateValue && timeValue ? ' ' : ''}${timeValue}`.trim();
    return '';
  };

  const activeHeadlineKey = headlineModal.fieldKey;
  const activeHeadlineState = activeHeadlineKey ? fieldStates?.[activeHeadlineKey] : null;
  const activeHeadlineDisplay = activeHeadlineKey
    ? getHeadlineDisplayValue(activeHeadlineKey, getHeadlineSourceValue(activeHeadlineKey))
    : '';
  // Get money box items - render all items from backend without caps
  const rawMoneyBoxItems = Array.isArray(moneyBox.items) ? moneyBox.items : [];
  const moneyBoxItems = rawMoneyBoxItems
    .sort((a, b) => {
      const aVal = parseNumericEuro(a?.stima_euro) ?? a?.market_range_eur?.max ?? 0;
      const bVal = parseNumericEuro(b?.stima_euro) ?? b?.market_range_eur?.max ?? 0;
      return bVal - aVal;
    });
  const moneyBoxItemA = moneyBoxItems.find((item) => item.code === 'A' || item.voce === 'A' || item.label_it?.toLowerCase().includes('regolarizzazione'));
  
  // Get money box total - support both old and new format, handle TBD
  const moneyBoxTotal = moneyBox.totale_extra_budget || moneyBox.total_extra_costs;
  const moneyBoxTotalRange = moneyBox.total_extra_costs_range;
  const moneyBoxTotalMin = moneyBoxTotal?.min;
  const moneyBoxTotalMax = moneyBoxTotal?.max;
  const moneyBoxTotalLegacyRangeMin = moneyBoxTotal?.range?.min;
  const moneyBoxTotalLegacyRangeMax = moneyBoxTotal?.range?.max;
  const moneyPolicy = safeRender(moneyBox.policy, '').toUpperCase();
  const isConservativeCostMode = moneyPolicy === 'CONSERVATIVE' || moneyPolicy === 'LOT_CONSERVATIVE';
  const isTotalTBD = [
    moneyBoxTotalMin,
    moneyBoxTotalMax,
    moneyBoxTotalLegacyRangeMin,
    moneyBoxTotalLegacyRangeMax
  ].some((value) => value === 'TBD' || value === 'NON_QUANTIFICATO_IN_PERIZIA');
  const moneyBoxNumericTotal = typeof moneyBoxTotalMin === 'number' && typeof moneyBoxTotalMax === 'number'
    ? { min: moneyBoxTotalMin, max: moneyBoxTotalMax }
    : typeof moneyBoxTotalLegacyRangeMin === 'number' && typeof moneyBoxTotalLegacyRangeMax === 'number'
      ? { min: moneyBoxTotalLegacyRangeMin, max: moneyBoxTotalLegacyRangeMax }
      : null;
  const hasMoneyBoxTotalRange = typeof moneyBoxTotalRange?.min_eur === 'number' && typeof moneyBoxTotalRange?.max_eur === 'number';
  const prezzoBaseValue =
    parseNumericEuro(selectedLot?.prezzo_base_value) ??
    parseNumericEuro(dati?.prezzo_base_asta?.value) ??
    parseNumericEuro(dati?.prezzo_base_asta?.formatted) ??
    parseNumericEuro(dati?.prezzo_base_asta);
  const canonicalCostCodes = new Set(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']);
  const valuationWaterfall = panoramicaContract?.valuation_waterfall && typeof panoramicaContract.valuation_waterfall === 'object'
    ? panoramicaContract.valuation_waterfall
    : null;
  const valuationDeprezzamentiValue = parseNumericEuro(valuationWaterfall?.deprezzamenti_eur);
  const valuationDeprezzamentiMeta = valuationWaterfall?.deprezzamenti_meta && typeof valuationWaterfall.deprezzamenti_meta === 'object'
    ? valuationWaterfall.deprezzamenti_meta
    : null;
  const valuationDeprezzamentiEvidence = Array.isArray(valuationWaterfall?.evidence?.deprezzamenti_eur)
    ? valuationWaterfall.evidence.deprezzamenti_eur
    : [];
  const valuationDeprezzamentiIsComputed = valuationDeprezzamentiMeta?.mode === 'COMPUTED';
  const valuationDeprezzamentiGrossValue = parseNumericEuro(valuationDeprezzamentiMeta?.gross_value_eur);
  const valuationDeprezzamentiFinalValue = parseNumericEuro(valuationDeprezzamentiMeta?.final_value_eur);
  const valuationDeprezzamentiComputedValue = parseNumericEuro(valuationDeprezzamentiMeta?.computed_difference_eur);
  const valuationDeprezzamentiGrossEvidence = Array.isArray(valuationDeprezzamentiMeta?.gross_evidence)
    ? valuationDeprezzamentiMeta.gross_evidence
    : [];
  const valuationDeprezzamentiFinalEvidence = Array.isArray(valuationDeprezzamentiMeta?.final_evidence)
    ? valuationDeprezzamentiMeta.final_evidence
    : [];

  const isJunkOrValuationSummaryCost = (item) => {
    const code = safeRender(item?.code || item?.voce, '').toUpperCase();
    const value = parseNumericEuro(item?.stima_euro);
    const textBlob = [
      safeRender(item?.label_it || item?.label, ''),
      safeRender(item?.stima_nota, ''),
      ...getItemEvidence(item).map((ev) => `${safeRender(ev?.quote, '')} ${safeRender(ev?.search_hint, '')}`)
    ].join(' ').toLowerCase();

    if (code === 'S3C08') return true;
    if (value !== null && value <= 0) return true;
    if (value !== null && prezzoBaseValue !== null && Math.round(value) === Math.round(prezzoBaseValue)) return true;
    if (textBlob.includes('già conteggiate')) return true;

    const valuationSummaryMarkers = [
      'valore di stima',
      'valore finale di stima',
      'prezzo base',
      'deprezzamento',
      'rischio assunto',
      'oneri di regolarizzazione urbanistica'
    ];
    return valuationSummaryMarkers.some((marker) => textBlob.includes(marker));
  };

  const explicitCostMentions = (() => {
    const candidates = moneyBoxItems.filter((item) => {
      const value = parseNumericEuro(item?.stima_euro);
      if (value === null || value <= 0) return false;
      if (isJunkOrValuationSummaryCost(item)) return false;
      const code = safeRender(item?.code || item?.voce, '').toUpperCase();
      if (!code.startsWith('S3C')) return false;
      return true;
    });

    const seen = new Set();
    return candidates
      .filter((item) => {
        const value = parseNumericEuro(item?.stima_euro);
        const evidence = getItemEvidence(item);
        const firstQuote = safeRender(evidence?.[0]?.quote, '').toLowerCase();
        const dedupeKey = `${Math.round(value || 0)}|${firstQuote.replace(/\s+/g, ' ').slice(0, 120)}`;
        if (seen.has(dedupeKey)) return false;
        seen.add(dedupeKey);
        return true;
      })
      .sort((a, b) => (parseNumericEuro(b?.stima_euro) || 0) - (parseNumericEuro(a?.stima_euro) || 0));
  })();

  const canonicalMoneyBoxItems = moneyBoxItems.filter((item) => {
    const code = safeRender(item?.code || item?.voce, '').toUpperCase();
    return canonicalCostCodes.has(code);
  });

  const explicitAmountKeys = new Set(
    explicitCostMentions
      .map((item) => parseNumericEuro(item?.stima_euro))
      .filter((value) => typeof value === 'number' && Number.isFinite(value) && value > 0)
      .map((value) => Math.round(value))
  );
  const nexodifyEstimateItems = [...canonicalMoneyBoxItems]
    .filter((item) => {
      const { isDocumentBacked, isEstimated } = classifyMoneyBoxItem(item);
      if (!isEstimated) return false;

      const numericValue = parseNumericEuro(item?.stima_euro);
      const roundedValue = typeof numericValue === 'number' && Number.isFinite(numericValue)
        ? Math.round(numericValue)
        : null;

      // Keep Nexodify bucket semantically pure: drop estimate rows that duplicate
      // a deterministic explicit cost already shown in the document-backed bucket.
      if (isDocumentBacked && roundedValue !== null && explicitAmountKeys.has(roundedValue)) {
        return false;
      }
      return true;
    })
    .sort((a, b) => {
      const aCode = safeRender(a?.code || a?.voce, '').toUpperCase();
      const bCode = safeRender(b?.code || b?.voce, '').toUpperCase();
      return aCode.localeCompare(bCode);
    });
  const qualitativeBurdens = (() => {
    const sourceItems = Array.isArray(moneyBox.qualitative_burdens) && moneyBox.qualitative_burdens.length > 0
      ? moneyBox.qualitative_burdens
      : moneyBoxItems.filter((item) => safeRender(item?.type, '').toUpperCase() === 'QUALITATIVE');
    const seen = new Set();
    return sourceItems.filter((item) => {
      const label = safeRender(item?.label_it || item?.label || item?.voce, '').trim();
      if (!label) return false;
      const key = label.toLowerCase();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  })();
  const moneyBoxBreakdown = canonicalMoneyBoxItems.reduce((acc, item) => {
    const euroValue = parseNumericEuro(item?.stima_euro);
    const marketRange = item?.market_range_eur && typeof item.market_range_eur === 'object'
      ? item.market_range_eur
      : null;
    const hasMarketRange = typeof marketRange?.min === 'number' && typeof marketRange?.max === 'number';
    const { isDocumentBacked, isEstimated } = classifyMoneyBoxItem(item);
    const addToBucket = (bucket, min, max) => {
      if (!Number.isFinite(min) || !Number.isFinite(max)) return;
      acc[bucket].min += min;
      acc[bucket].max += max;
    };

    if (euroValue !== null) {
      if (isEstimated) addToBucket('estimated', euroValue, euroValue);
      else if (isDocumentBacked) addToBucket('documentBacked', euroValue, euroValue);
      return acc;
    }

    if (hasMarketRange) {
      if (isEstimated) addToBucket('estimated', marketRange.min, marketRange.max);
      else if (isDocumentBacked) addToBucket('documentBacked', marketRange.min, marketRange.max);
    }

    return acc;
  }, {
    documentBacked: { min: 0, max: 0 },
    estimated: { min: 0, max: 0 }
  });
  const documentBackedMin = moneyBoxBreakdown.documentBacked.min;
  const documentBackedMax = moneyBoxBreakdown.documentBacked.max;
  const estimatedMin = moneyBoxBreakdown.estimated.min;
  const estimatedMax = moneyBoxBreakdown.estimated.max;
  const allInMin = hasMoneyBoxTotalRange && prezzoBaseValue !== null
    ? prezzoBaseValue + moneyBoxTotalRange.min_eur
    : null;
  const allInMax = hasMoneyBoxTotalRange && prezzoBaseValue !== null
    ? prezzoBaseValue + moneyBoxTotalRange.max_eur
    : null;
  const canRenderAllIn = allInMin !== null && allInMax !== null;

  const isTocLikeLegalEvidence = (quoteRaw, searchHintRaw = '') => {
    const quote = safeRender(quoteRaw, '').toLowerCase();
    const searchHint = safeRender(searchHintRaw, '').toLowerCase();
    const text = `${quote} ${searchHint}`.trim();
    const compact = text.replace(/\s+/g, ' ');
    if (!text) return true;
    if (/\.\.{4,}/.test(text)) return true;
    if (/[·•]\s*\d+\s*$/.test(text)) return true;
    if (/servit[ùu][\s,;:\-]+censo[\s,;:\-]+livell[\s,;:\-]+usi civici/.test(compact)) return true;
    if (/bene\s*n[°o]?|benen°|beni oggetto/.test(compact) && /servit[ùu]|usi civici/.test(compact)) return true;
    if (/sommario|indice/.test(text)) return true;
    if (text.length < 45 && /(servitù|usi civici|vincolo)/.test(text)) return true;
    return false;
  };

  const isSubstantiveServituEvidence = (quoteRaw, searchHintRaw = '') => {
    const quote = safeRender(quoteRaw, '').toLowerCase();
    const searchHint = safeRender(searchHintRaw, '').toLowerCase();
    const text = `${quote} ${searchHint}`.replace(/\s+/g, ' ').trim();
    const normalized = text
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-z0-9]+/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
    if (!text) return false;
    if (isTocLikeLegalEvidence(quoteRaw, searchHintRaw)) return false;
    // Hard-stop heading/index phrases even when OCR introduces spacing noise (es. "LIVE LLO").
    if (/servitu\s+censo\s+livell\w*\s+usi\s+civici/.test(normalized)) return false;
    // Absence statements are not blockers and must never be top-promoted.
    if (/(non\s+sono\s+presenti|non\s+risultan\w*|assenza)\s+.*(servitu|usi\s+civici)/.test(normalized)) return false;
    // Reject short/generic snippets without an operative finding.
    if (normalized.length < 140) return false;
    if (!/(servitu|usi\s+civici)/.test(normalized)) return false;
    // Strict requirement: only promote when a concrete operative servitù/usi-civici finding is present.
    return /(servitu\s+di\s+passaggio|fondo\s+dominante|fondo\s+servente|atto\s+di\s+servitu|servitu\s+costituit\w*|grava\w*\s+da\s+servitu|usi\s+civici\s+presenti|diritti\s+demaniali\s+presenti)/.test(normalized);
  };

  const getFirstEvidence = (entry) => {
    if (!entry || typeof entry !== 'object') return null;
    const ev = getEvidence(entry);
    if (Array.isArray(ev) && ev.length > 0) return ev[0];
    if (Array.isArray(entry.__evidence) && entry.__evidence.length > 0) return entry.__evidence[0];
    return null;
  };

  const pickLegalCategory = (entry) => {
    const firstEv = getFirstEvidence(entry);
    const text = [
      safeRender(entry?.killer, ''),
      safeRender(entry?.label_it || entry?.__label, ''),
      safeRender(entry?.label_en, ''),
      safeRender(entry?.status_it || entry?.status, ''),
      safeRender(entry?.reason_it || entry?.action_required_it || entry?.action, ''),
      safeRender(entry?.value_it || entry?.__value, ''),
      safeRender(firstEv?.quote, ''),
      safeRender(firstEv?.search_hint, ''),
    ].join(' ').toLowerCase();

    if (/(occupat|debitore|coniuge)/.test(text)) return 'occupazione';
    if (/(pignorament|esecuzione immobiliare)/.test(text)) return 'pignoramento_esecuzione';
    if (/(ipotec|formalit)/.test(text)) return 'ipoteca_formalita';
    if (/(difform|catast|urbanistic|regolarit)/.test(text)) return 'difformita_urb_cat';
    if (/(agibil|abitabil|ape|impiant|documentaz)/.test(text)) return 'agibilita_docs';
    if (/(accesso|mappal|atto\s+di\s+vincolo|vincoli?\s+ancora\s+vigenti|a\s+carico\s+del\s+proprietario)/.test(text)) return 'accesso_vincolo';
    if (/(servitù|servitu|usi civici|censo|livello)/.test(text)) return 'servitu_usi_civici';
    return null;
  };

  const categoryLabelMap = {
    occupazione: 'Occupazione',
    pignoramento_esecuzione: 'Pignoramento / Esecuzione',
    ipoteca_formalita: 'Ipoteca / Formalità pregiudizievoli',
    difformita_urb_cat: 'Difformità urbanistico-catastali',
    agibilita_docs: 'Agibilità / documentazione tecnica critica',
    accesso_vincolo: 'Accesso / vincolo da verificare',
    servitu_usi_civici: 'Servitù / usi civici'
  };

  const baseLegalItems = Array.isArray(legalKillers?.items) ? legalKillers.items : [];
  const detailLegalItems = Array.isArray(estrattoLegalSection?.__items) ? estrattoLegalSection.__items : [];
  const difformitaKeywordRegex = /(difform|incongruenz|non\s+sussiste\s+corrispondenza\s+catastal|planimetri[a|e].*(difform|non\s+corrispond)|mancata\s+corrispondenza\s+catastal|irregolarit[àa]\s+urbanistic|abusi?\s+ediliz)/i;
  const difformitaFieldCandidates = ['regolarita_urbanistica', 'conformita_catastale']
    .map((key) => fieldStates?.[key])
    .filter((state) => state && typeof state === 'object')
    .map((state) => {
      const ev = Array.isArray(state?.evidence) ? state.evidence : [];
      const first = ev[0] || null;
      const signalText = [
        safeRender(state?.value, ''),
        safeRender(state?.status, ''),
        safeRender(state?.status_it, ''),
        safeRender(first?.quote, ''),
        safeRender(first?.search_hint, '')
      ].join(' ');
      if (!difformitaKeywordRegex.test(signalText)) return null;
      return {
        killer: 'Difformità urbanistico-catastali',
        status: 'ROSSO',
        status_it: 'CRITICO',
        reason_it: safeRender(state?.status_it, 'Difformità rilevata'),
        evidence: ev
      };
    })
    .filter(Boolean);

  const allLegalCandidates = [...baseLegalItems, ...detailLegalItems, ...difformitaFieldCandidates].filter((entry) => entry && typeof entry === 'object');

  const legalCategoryCandidates = Object.keys(categoryLabelMap).reduce((acc, key) => {
    acc[key] = [];
    return acc;
  }, {});

  allLegalCandidates.forEach((entry) => {
    const category = pickLegalCategory(entry);
    if (!category) return;
    const firstEv = getFirstEvidence(entry);
    const quote = safeRender(firstEv?.quote, '');
    const searchHint = safeRender(firstEv?.search_hint, '');
    const page = firstEv?.page;
    const isSubstantive = Boolean(quote) && !isTocLikeLegalEvidence(quote, searchHint);
    const fromSection9 = baseLegalItems.includes(entry);
    legalCategoryCandidates[category].push({
      entry,
      isSubstantive,
      fromSection9,
      page: typeof page === 'number' ? page : 999,
      quoteLen: quote.length,
      quote,
      searchHint
    });
  });

  const severityByCategory = {
    occupazione: 'GIALLO',
    pignoramento_esecuzione: 'GIALLO',
    ipoteca_formalita: 'ROSSO',
    difformita_urb_cat: 'ROSSO',
    agibilita_docs: 'GIALLO',
    accesso_vincolo: 'GIALLO',
    servitu_usi_civici: 'GIALLO'
  };
  const legalKindByCategory = {
    occupazione: 'material_blocker',
    pignoramento_esecuzione: 'execution_context',
    ipoteca_formalita: 'material_blocker',
    difformita_urb_cat: 'material_blocker',
    agibilita_docs: 'caution_watch',
    accesso_vincolo: 'caution_watch',
    servitu_usi_civici: 'background_note'
  };
  const legalKindLabelMap = {
    execution_context: 'Contesto esecutivo',
    material_blocker: 'Blocco materiale',
    caution_watch: 'Cautela / verifica',
    background_note: 'Nota di sfondo'
  };
  const occupancyDisplayTruth = normalizeComparableText(getRichFieldDisplayValue('stato_occupativo', occupativo, occupativo?.status_it, occupativo?.status));
  const agibilitaDisplayTruth = normalizeComparableText(pickFirstNonEmpty(fieldStates?.agibilita?.value, fieldStates?.agibilita?.status_it, abusi?.agibilita?.status, abusi?.agibilita));
  const hasPositiveOccupancyTruth = /(libero|non occupato|disponibile)/.test(occupancyDisplayTruth)
    && !/(occupato da terzi|locato|opponibil|detent|debitore occupa)/.test(occupancyDisplayTruth);
  const hasPositiveAgibilitaTruth = /(presente|rilasciat|agibil|abitabil)/.test(agibilitaDisplayTruth)
    && !/(non|assen|manc|irregolar)/.test(agibilitaDisplayTruth);

  const normalizeLegalSeverity = (category, sourceStatus) => {
    const baseSeverity = normalizeUiSeverity(sourceStatus || severityByCategory[category], severityByCategory[category]);
    const kind = legalKindByCategory[category];
    if (kind === 'execution_context' && baseSeverity === 'RED') return 'AMBER';
    if (kind === 'background_note' && baseSeverity === 'RED') return 'AMBER';
    return baseSeverity;
  };

  const buildTopLegalChecklist = () => {
    const ordered = [
      'pignoramento_esecuzione',
      'ipoteca_formalita',
      'difformita_urb_cat',
      'agibilita_docs',
      'occupazione',
      'accesso_vincolo',
      'servitu_usi_civici'
    ];
    const out = [];
    ordered.forEach((category) => {
      const candidates = legalCategoryCandidates[category] || [];
      const preserved = candidates.filter((c) => c.fromSection9);
      let substantive = candidates.filter((c) => c.isSubstantive);
      if (category === 'servitu_usi_civici') {
        substantive = candidates.filter((c) => c.fromSection9 || isSubstantiveServituEvidence(c.quote, c.searchHint));
      }
      if (category === 'accesso_vincolo') {
        substantive = candidates.filter((c) => c.fromSection9 || (
          c.isSubstantive && (() => {
          const txt = `${safeRender(c.quote, '')} ${safeRender(c.searchHint, '')}`.toLowerCase();
          return /(accesso|mappal|atto\s+di\s+vincolo|vincoli?\s+ancora\s+vigenti|proprietario\s+debitore)/.test(txt)
            && !isTocLikeLegalEvidence(c.quote, c.searchHint);
          })()
        ));
      }
      const eligible = preserved.length > 0 ? preserved : substantive;
      if (eligible.length === 0) return;
      eligible.sort((a, b) => {
        if (a.fromSection9 !== b.fromSection9) return a.fromSection9 ? -1 : 1;
        if (a.isSubstantive !== b.isSubstantive) return a.isSubstantive ? -1 : 1;
        if (a.page !== b.page) return a.page - b.page;
        return b.quoteLen - a.quoteLen;
      });
      const best = eligible[0];
      const source = best.entry || {};
      const sourceText = normalizeComparableText([
        source?.killer,
        source?.label_it,
        source?.status_it,
        source?.reason_it,
        source?.action_required_it,
        source?.value_it
      ].join(' '));
      if (category === 'occupazione' && hasPositiveOccupancyTruth) return;
      if (category === 'agibilita_docs' && hasPositiveAgibilitaTruth && !/(non|assen|manc|irregolar)/.test(sourceText)) return;
      const kind = legalKindByCategory[category];
      out.push({
        killer: categoryLabelMap[category],
        status: normalizeLegalSeverity(category, source?.status),
        status_it: safeRender(source?.status_it, ''),
        action: safeRender(source?.reason_it || source?.action_required_it || source?.action, ''),
        evidence: getEvidence(source),
        kind,
        contextLabel: legalKindLabelMap[kind]
      });
    });
    return out;
  };

  const topLegalChecklistItems = buildTopLegalChecklist();
  const legalKillersObj = topLegalChecklistItems.reduce((acc, item, idx) => {
    acc[item.killer || `killer_${idx + 1}`] = item;
    return acc;
  }, {});

  const filteredLegalDetailItems = (() => {
    if (!Array.isArray(detailLegalItems)) return [];
    const seen = new Set();
    let genericVincoloSeen = 0;
    const cleaned = [];
    detailLegalItems.forEach((item) => {
      if (!item || typeof item !== 'object') return;
      const label = safeRender(item?.label_it || item?.__label, '').toLowerCase();
      const firstEv = getFirstEvidence(item);
      const quote = safeRender(firstEv?.quote, '');
      const searchHint = safeRender(firstEv?.search_hint, '');
      const isTocLike = isTocLikeLegalEvidence(quote, searchHint);
      const isGenericVincolo = label.includes('vincolo legale: vincolo');
      if (isGenericVincolo) {
        genericVincoloSeen += 1;
        if (genericVincoloSeen > 1) return;
      }
      if (isTocLike && !/pignorament|ipotec|difform|occupat|agibil|servitù|servitu|usi civici/i.test(`${label} ${quote}`)) {
        return;
      }
      const key = `${label}|${quote.replace(/\s+/g, ' ').slice(0, 160)}`;
      if (seen.has(key)) return;
      seen.add(key);
      cleaned.push(item);
    });
    return cleaned;
  })();

  const groupedLegalDetailSections = (() => {
    const groups = {
      execution_context: [],
      material_blocker: [],
      caution_watch: [],
      background_note: []
    };
    const seen = new Set();
    const clusterIndex = new Map();

    const mapToGroup = (category) => {
      return legalKindByCategory[category] || 'background_note';
    };

    filteredLegalDetailItems.forEach((item) => {
      const firstEv = getFirstEvidence(item);
      const quote = safeRender(firstEv?.quote, '');
      const searchHint = safeRender(firstEv?.search_hint, '');
      const page = firstEv?.page ?? '';
      const category = pickLegalCategory(item);
      if (category === 'servitu_usi_civici' && !isSubstantiveServituEvidence(quote, searchHint)) return;
      const bucket = mapToGroup(category);
      const labelIt = safeRender(item?.label_it || item?.__label, 'Voce');
      const labelEn = safeRender(item?.label_en, '');
      const displayValue = getEstrattoItemDisplayValue(item);
      const normalizedQuote = quote
        .toLowerCase()
        .replace(/\s+/g, ' ')
        .replace(/[^a-z0-9 ]/g, '')
        .trim()
        .slice(0, 170);
      const semanticClusterKey = `${bucket}|${normalizeComparableText(labelIt)}|${normalizeComparableText(displayValue)}`;
      const dedupeKey = `${bucket}|${page}|${normalizedQuote || `${labelIt}|${displayValue}`.toLowerCase().slice(0, 170)}`;
      if (seen.has(dedupeKey)) return;
      seen.add(dedupeKey);
      const nextItem = {
        key: dedupeKey,
        labelIt,
        labelEn,
        displayValue,
        evidence: Array.isArray(item?.__evidence) ? item.__evidence : []
      };
      const shouldClusterBySemanticKey = bucket === 'execution_context' || bucket === 'material_blocker';
      if (shouldClusterBySemanticKey && clusterIndex.has(semanticClusterKey)) {
        const target = clusterIndex.get(semanticClusterKey);
        target.evidence = mergeEvidence(target.evidence, nextItem.evidence);
        if (!target.displayValue && nextItem.displayValue) target.displayValue = nextItem.displayValue;
        return;
      }
      groups[bucket].push(nextItem);
      if (shouldClusterBySemanticKey) clusterIndex.set(semanticClusterKey, nextItem);
    });

    return [
      { key: 'execution_context', title: 'Contesto esecutivo', items: groups.execution_context },
      { key: 'material_blocker', title: 'Blocchi materiali', items: groups.material_blocker },
      { key: 'caution_watch', title: 'Cautele / verifiche', items: groups.caution_watch },
      { key: 'background_note', title: 'Note di sfondo', items: groups.background_note }
    ].filter((group) => Array.isArray(group.items) && group.items.length > 0);
  })();

  const redFlagGroups = {
    legal: [],
    technical: [],
    occupancy: [],
    missingData: [],
    costUncertainty: []
  };

  const addRedFlag = (groupKey, item) => {
    if (!redFlagGroups[groupKey]) return;
    if (!item || typeof item !== 'object') return;
    const key = safeRender(item.key || item.label, '').toLowerCase();
    if (!key) return;
    const exists = redFlagGroups[groupKey].some((entry) => safeRender(entry.key || entry.label, '').toLowerCase() === key);
    if (exists) return;
    redFlagGroups[groupKey].push(item);
  };

  const redFlagKindLabels = {
    confirmed_risk: 'Rischio confermato',
    unresolved_conflict: 'Conflitto irrisolto',
    coverage_gap: 'Copertura documento',
    cost_uncertainty: 'Incertezza costi'
  };
  const classifyStoredRedFlag = (title, detail) => {
    const text = `${title} ${detail}`.toLowerCase();
    if (/(risolt|resolved|gia verificat|positivo|regolare)/.test(text)) {
      return { group: 'missingData', kind: 'coverage_gap', severity: 'INFO' };
    }
    if (/(cost|costi|stima|assunzion|quantificat)/.test(text)) {
      return { group: 'costUncertainty', kind: 'cost_uncertainty', severity: 'AMBER' };
    }
    if (/(occupaz|liberaz|opponibil)/.test(text)) {
      return { group: 'occupancy', kind: 'unresolved_conflict', severity: 'AMBER' };
    }
    if (/(urbanistic|catast|agibil|abitabil|ape|impiant)/.test(text)) {
      return /(non specificat|mancant|copertura|manual review|verificar)/.test(text)
        ? { group: 'missingData', kind: 'coverage_gap', severity: 'INFO' }
        : { group: 'technical', kind: 'confirmed_risk', severity: 'AMBER' };
    }
    if (/(servit|usi civici|censo|livello)/.test(text)) {
      return /(passaggio|fondo dominante|fondo servente|atto di servitu|diritti demaniali presenti)/.test(text)
        ? { group: 'legal', kind: 'confirmed_risk', severity: 'AMBER' }
        : { group: 'missingData', kind: 'coverage_gap', severity: 'INFO' };
    }
    if (/(pignorament|ipotec|formalit|servit|usi civici|vincol)/.test(text)) {
      return /(pignorament|esecuzione immobiliare)/.test(text)
        ? { group: 'legal', kind: 'coverage_gap', severity: 'INFO' }
        : { group: 'legal', kind: 'confirmed_risk', severity: 'AMBER' };
    }
    return { group: 'missingData', kind: 'coverage_gap', severity: 'INFO' };
  };

  redFlags.forEach((flag, idx) => {
    if (typeof flag === 'string') return;
    const title = safeRender(flag?.flag_it || flag?.label || flag?.title_it || flag?.title, '').trim();
    const detail = safeRender(flag?.action_it || flag?.explanation || flag?.detail || flag?.reason_it, '').trim();
    if (!title) return;
    const classified = classifyStoredRedFlag(title, detail);
    addRedFlag(classified.group, {
      key: `stored_flag_${idx}`,
      label: title,
      explanation: detail || 'Verificare in perizia originale.',
      severity: normalizeUiSeverity(flag?.severity || flag?.status, classified.severity),
      evidence: Array.isArray(flag?.evidence) ? flag.evidence : [],
      kindLabel: redFlagKindLabels[classified.kind]
    });
  });

  topLegalChecklistItems.forEach((item, idx) => {
    if (!item || item.kind === 'execution_context' || item.kind === 'background_note') return;
    addRedFlag('legal', {
      key: `legal_check_${idx}`,
      label: safeRender(item?.killer, 'Segnalazione legale'),
      explanation: safeRender(item?.action || item?.status_it || item?.contextLabel, ''),
      severity: normalizeUiSeverity(item?.status, item?.kind === 'background_note' ? 'INFO' : 'AMBER'),
      evidence: getEvidence(item),
      kindLabel: item.kind === 'material_blocker'
        ? redFlagKindLabels.confirmed_risk
        : item.kind === 'caution_watch'
          ? redFlagKindLabels.unresolved_conflict
          : redFlagKindLabels.coverage_gap
    });
  });

  // Dedupe across groups: keep occupancy only in Occupazione group.
  redFlagGroups.legal = redFlagGroups.legal.filter((item) => {
    const label = safeRender(item?.label, '').toLowerCase();
    return !label.includes('occupazione');
  });
  if (hasPositiveOccupancyTruth) {
    redFlagGroups.occupancy = redFlagGroups.occupancy.filter((item) => normalizeUiSeverity(item?.severity, 'AMBER') === 'RED');
  }
  if (hasPositiveAgibilitaTruth) {
    redFlagGroups.technical = redFlagGroups.technical.filter((item) => {
      const label = normalizeComparableText(item?.label);
      if (!/(agibil|abitabil)/.test(label)) return true;
      return normalizeUiSeverity(item?.severity, 'AMBER') === 'RED';
    });
  }

  // Dedupe broad legal difformita when technical split flags are present.
  const hasTechUrbanisticaFlag = redFlagGroups.technical.some((item) => item?.key === 'tech_urbanistica');
  const hasTechCatastoFlag = redFlagGroups.technical.some((item) => item?.key === 'tech_catasto');
  const hasTechAgibilitaFlag = redFlagGroups.technical.some((item) => item?.key === 'tech_agibilita');
  if (hasTechUrbanisticaFlag && hasTechCatastoFlag) {
    redFlagGroups.legal = redFlagGroups.legal.filter((item) => {
      const key = safeRender(item?.key, '').toLowerCase();
      const label = safeRender(item?.label, '').toLowerCase();
      if (key.includes('difformita_urb_cat')) return false;
      if (label.includes('difformità urbanistico-catastali')) return false;
      return true;
    });
  }

  if (hasTechAgibilitaFlag) {
    redFlagGroups.legal = redFlagGroups.legal.filter((item) => {
      const key = safeRender(item?.key, '').toLowerCase();
      const label = safeRender(item?.label, '').toLowerCase();
      if (key.includes('agibilita_docs')) return false;
      if (label.includes('agibilità')) return false;
      if (label.includes('abitabilità')) return false;
      return true;
    });
  }

  const groupedRedFlags = [
    { key: 'legal', title: 'Legale / Formalità', items: redFlagGroups.legal },
    { key: 'technical', title: 'Tecnico / Compliance', items: redFlagGroups.technical },
    { key: 'occupancy', title: 'Occupazione', items: redFlagGroups.occupancy },
    { key: 'missingData', title: 'Copertura documento / Dati mancanti', items: redFlagGroups.missingData },
    { key: 'costUncertainty', title: 'Incertezza costi / Assunzioni', items: redFlagGroups.costUncertainty }
  ];
  const hasGroupedRedFlags = groupedRedFlags.some((group) => group.items.length > 0);

  const scoreSummarySignal = (textRaw) => {
    const text = safeRender(textRaw, '').toLowerCase();
    if (!text) return 0;
    let score = 0;
    if (/(difform|abus|non conform|irregolar|occupat|opponibil|blocc|criticit|agibilit assente)/.test(text)) score += 50;
    if (/(libero|non emergono abusi|conforme|ape presente|agibilit present)/.test(text)) score += 35;
    if (/(pignorament|esecuzione immobiliare|trascrizion|ipotec)/.test(text)) score += 10;
    if (/(servit|vincolo|contesto esecutivo|procedura esecutiva|asta)/.test(text)) score -= 5;
    return score;
  };

  const weightedDecisionBullets = decisionBulletsIt
    .map((bullet, idx) => ({ bullet, bulletEn: decisionBulletsEn[idx] || '', idx, score: scoreSummarySignal(bullet) }))
    .sort((a, b) => b.score - a.score || a.idx - b.idx);
  const hasStrongDecisionSignals = weightedDecisionBullets.some((item) => item.score >= 30);
  const displayedDecisionBullets = weightedDecisionBullets
    .filter((item) => !hasStrongDecisionSignals || item.score >= 10)
    .slice(0, 5);
  const weightedDrivers = Array.isArray(decision.driver_rosso)
    ? decision.driver_rosso
        .map((driver, idx) => ({
          driver,
          idx,
          score: scoreSummarySignal(`${safeRender(driver?.headline_it, '')} ${safeRender(driver?.reason_it || driver?.headline_en, '')}`)
        }))
        .sort((a, b) => b.score - a.score || a.idx - b.idx)
    : [];
  const hasStrongDrivers = weightedDrivers.some((item) => item.score >= 30);
  const displayedDrivers = weightedDrivers
    .filter((item) => !hasStrongDrivers || item.score >= 15)
    .map((item) => item.driver)
    .slice(0, 4);
  const topMaterialLegalItem = topLegalChecklistItems.find((item) => item?.kind === 'material_blocker');
  const summaryHasWeakServituBias = /servit|usi civici/.test(normalizeComparableText(decisionIt));
  const displayDecisionIt = summaryHasWeakServituBias && topMaterialLegalItem
    ? safeRender(topMaterialLegalItem.killer, decisionIt)
    : scoreSummarySignal(decisionIt) < 10 && displayedDecisionBullets[0]?.score >= 30
      ? displayedDecisionBullets[0].bullet
      : decisionIt;
  const displayDecisionEn = summaryHasWeakServituBias
    ? decisionEn
    : scoreSummarySignal(decisionEn) < 10 && displayedDecisionBullets[0]?.bulletEn
      ? displayedDecisionBullets[0].bulletEn
      : decisionEn;

  // Debug logging for troubleshooting
  if (process.env.NODE_ENV === 'development') {
    console.log('Lots:', lots.length, lots);
    console.log('Selected Lot:', selectedLotIndex, selectedLot);
    console.log('MoneyBox items:', moneyBoxItems.length, moneyBoxItems);
    console.log('MoneyBox total:', moneyBoxTotal, 'isTBD:', isTotalTBD);
    console.log('LegalKillers:', Object.keys(legalKillersObj).length, legalKillersObj);
    console.log('Dati certi:', dati);
    console.log('Semaforo:', semaforo);
  }

  const uiDisplayedFields = {
    tribunale: formatFieldStateDisplay('tribunale', caseHeader.tribunale?.value || caseHeader.tribunale),
    procedure_id: formatFieldStateDisplay('procedura', caseHeader.procedure?.value || caseHeader.procedure_id),
    occupancy: formatFieldStateDisplay('stato_occupativo', occupativo.status_it || occupativo.status),
    beni_count: Array.isArray(beni) ? beni.length : 0,
    beni_summary: Array.isArray(beni) ? beni.map((b, i) => ({
      idx: i + 1,
      bene_number: b?.bene_number ?? null,
      tipologia: safeRender(b?.tipologia, ''),
      note: safeRender(b?.note, '')
    })) : [],
    ape_status: getSurfaceDisplayValue('ape', abusi.ape, result.ape),
    spese_condominiali_arretrate: formatFieldStateDisplay('spese_condominiali_arretrate', result.spese_condominiali_arretrate || result.spese_condominiali),
    sanatoria_estimate: moneyBoxItemA?.stima_euro ?? result?.sanatoria_estimate ?? null,
    prezzo_base: safeRender(dati.prezzo_base_asta?.formatted || dati.prezzo_base_asta?.value || dati.prezzo_base_asta, 'Non specificato in perizia'),
    dati_asta: safeRender(datiAsta?.data || datiAsta?.value || datiAsta, 'Non specificato in perizia'),
    decisione_rapida_it: decisionIt,
    decisione_rapida_en: decisionEn,
    decision_source: decisionSourceLabel,
    semaforo_status: safeRender(semaforo.status, 'AMBER'),
    semaforo_blockers: Array.isArray(decision.driver_rosso)
      ? decision.driver_rosso.map((d) => safeRender(d?.headline_it, '')).filter(Boolean)
      : []
  };

  const params = new URLSearchParams(location.search);
  const isDebugMode = params.get('debug') === '1';
  if (isDebugMode && typeof window !== 'undefined') {
    window.__UI_SNAPSHOT__ = {
      analysis_id: analysisId,
      result_path_used: resultPathUsed,
      raw_api_shape_keys: {
        analysis_top_keys: analysis ? Object.keys(analysis) : [],
        result_top_keys: result ? Object.keys(result) : []
      },
      displayed_fields: uiDisplayedFields,
      raw_keys_used: {
        case_header_keys: caseHeader ? Object.keys(caseHeader) : [],
        field_state_keys: fieldStates ? Object.keys(fieldStates) : [],
        money_box_item_codes: moneyBoxItems.map((i) => i?.code || i?.voce || i?.label_it || 'NON_SPECIFICATO_IN_PERIZIA')
      },
      user_messages: {
        count: userMessages.length,
        codes: userMessages.map((m) => safeRender(m?.code, '')).filter(Boolean)
      },
    };
  }

  const HeadlineInlineField = ({ label, fieldKey, value, evidence, className = '' }) => {
    const status = getHeadlineStatus(fieldKey);
    const displayValue = getHeadlineDisplayValue(fieldKey, value);
    const needsVerification = isNeedsVerification(status);
    const isConfirmed = status === 'USER_PROVIDED';
    const shouldRender = value !== undefined && value !== null && value !== '' || status;

    if (!shouldRender) return null;

    return (
      <p className={`text-sm flex flex-wrap items-center gap-2 ${className}`}>
        {label && <span>{label}:</span>}
        <span>{displayValue}</span>
        <EvidenceBadge evidence={evidence} />
        {needsVerification && (
          <span className="px-2 py-0.5 rounded-full text-[10px] font-mono bg-amber-500/20 text-amber-300">
            DA VERIFICARE
          </span>
        )}
        {isConfirmed && (
          <span className="px-2 py-0.5 rounded-full text-[10px] font-mono bg-emerald-500/20 text-emerald-300">
            Confermato
          </span>
        )}
        {needsVerification && (
          <button
            type="button"
            onClick={() => openHeadlineModal(fieldKey)}
            className="text-xs text-gold hover:underline"
          >
            Correggi
          </button>
        )}
      </p>
    );
  };

  const HeadlineFieldCard = ({ label, fieldKey, value, evidence }) => {
    const status = getHeadlineStatus(fieldKey);
    const displayValue = getHeadlineDisplayValue(fieldKey, value);
    const hasEvidence = evidence && Array.isArray(evidence) && evidence.length > 0;
    const pages = hasEvidence ? [...new Set(evidence.map(e => e.page).filter(Boolean))].sort((a, b) => a - b) : [];
    const needsVerification = isNeedsVerification(status);
    const isConfirmed = status === 'USER_PROVIDED';

    return (
      <div className="p-4 bg-zinc-950 rounded-lg">
        <div className="flex items-center justify-between mb-1">
          <p className="text-xs font-mono text-zinc-500">{label}</p>
          {hasEvidence && (
            <span className="text-xs font-mono text-gold flex items-center gap-1">
              <FileText className="w-3 h-3" />
              p. {pages.join(', ')}
            </span>
          )}
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <p className="font-medium text-zinc-100">{displayValue}</p>
          {needsVerification && (
            <span className="px-2 py-0.5 rounded-full text-[10px] font-mono bg-amber-500/20 text-amber-300">
              DA VERIFICARE
            </span>
          )}
          {isConfirmed && (
            <span className="px-2 py-0.5 rounded-full text-[10px] font-mono bg-emerald-500/20 text-emerald-300">
              Confermato
            </span>
          )}
          {needsVerification && (
            <button
              type="button"
              onClick={() => openHeadlineModal(fieldKey)}
              className="text-xs text-gold hover:underline"
            >
              Correggi
            </button>
          )}
        </div>
        {hasEvidence && evidence[0]?.quote && (
          <p className="text-xs text-zinc-500 mt-2 italic border-l-2 border-gold/30 pl-2">
            "{evidence[0].quote.substring(0, 150)}{evidence[0].quote.length > 150 ? '...' : ''}"
          </p>
        )}
        <MissingStateRationale fieldKey={fieldKey} forceMissing={!hasEvidence && needsVerification} />
      </div>
    );
  };

  return (
    <div className="min-h-screen bg-[#09090b]">
      <Sidebar user={user} logout={logout} />
      
      <main className="px-4 pb-8 pt-24 sm:px-6 lg:ml-64 lg:px-8 lg:pt-8">
        {isDebugMode && (
          <pre className="mb-4 text-[10px] leading-4 text-zinc-400 bg-zinc-950 border border-zinc-800 rounded p-2 overflow-x-auto">
{JSON.stringify({
  has_estratto_quality: !!result.estratto_quality,
  estratto_sections_count: estrattoSections.length,
  money_box_items_count: moneyBoxItems.length
}, null, 2)}
          </pre>
        )}

        {/* Back Button & Actions */}
        <div className="mb-6 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <Link to="/history" className="inline-flex items-center gap-2 text-zinc-400 hover:text-zinc-100 transition-colors">
            <ArrowLeft className="w-4 h-4" />
            Torna allo storico
          </Link>
          <div className="flex items-center gap-3 self-start sm:self-auto">
            <Button
              onClick={() => setShowDeleteModal(true)}
              variant="outline"
              data-testid="delete-analysis-btn"
              className="border-red-500/30 text-red-400 hover:bg-red-500/10 hover:border-red-500/50"
            >
              <Trash2 className="w-4 h-4 mr-2" />
              Elimina
            </Button>
          </div>
        </div>

        {/* Delete Confirmation Modal */}
        {showDeleteModal && (
          <div className="fixed inset-0 z-50 flex items-center justify-center">
            <div className="absolute inset-0 bg-black/70" onClick={() => setShowDeleteModal(false)} />
            <div className="relative bg-zinc-900 border border-zinc-800 rounded-xl p-6 max-w-md w-full mx-4 shadow-xl">
              <button 
                onClick={() => setShowDeleteModal(false)}
                className="absolute top-4 right-4 text-zinc-500 hover:text-zinc-300"
              >
                <X className="w-5 h-5" />
              </button>
              
              <div className="flex items-center gap-3 mb-4">
                <div className="p-2 bg-red-500/20 rounded-lg">
                  <AlertTriangle className="w-6 h-6 text-red-400" />
                </div>
                <h3 className="text-lg font-semibold text-zinc-100">Elimina analisi</h3>
              </div>
              
              <p className="text-zinc-400 text-sm mb-6">
                Sei sicuro di voler eliminare questa analisi? L'azione non può essere annullata.
              </p>
              
              <div className="flex flex-col-reverse gap-3 sm:flex-row sm:justify-end">
                <Button 
                  variant="outline" 
                  onClick={() => setShowDeleteModal(false)}
                  disabled={isDeleting}
                  className="border-zinc-700 text-zinc-300 hover:bg-zinc-800"
                >
                  Annulla
                </Button>
                <Button 
                  onClick={handleDelete}
                  disabled={isDeleting}
                  className="bg-red-600 hover:bg-red-700 text-white"
                >
                  {isDeleting ? (
                    <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin mr-2" />
                  ) : (
                    <Trash2 className="w-4 h-4 mr-2" />
                  )}
                  Elimina
                </Button>
              </div>
            </div>
          </div>
        )}

        <HeadlineVerifyModal
          open={headlineModal.open}
          onClose={closeHeadlineModal}
          analysisId={analysisId}
          fieldKey={headlineModal.fieldKey}
          fieldState={activeHeadlineState}
          currentDisplayValue={activeHeadlineDisplay}
          onSaved={fetchAnalysis}
        />
        
        {/* Multi-Lot Selector - show if multiple lots */}
        {isMultiLot && (
          <MultiLotSelector 
            lots={lots} 
            selectedLot={selectedLotIndex} 
            onSelectLot={setSelectedLotIndex} 
          />
        )}

        {/* Header with Semaforo */}
        <div className="mb-8 rounded-xl border border-zinc-800 bg-zinc-900 p-5 sm:p-6">
          <div className="flex flex-col gap-6 xl:flex-row xl:items-start xl:justify-between">
            <div className="min-w-0">
              <h1 className="mb-2 text-2xl font-serif font-bold text-zinc-100 text-wrap-safe">
                {safeRender(analysis.case_title || analysis.file_name, 'Analisi Perizia')}
              </h1>
              <div className="mb-3 flex flex-wrap items-center gap-2">
                <span className="text-[10px] px-2 py-1 rounded border border-zinc-700 text-zinc-300 uppercase tracking-wide">
                  {semaforoLabels.it}
                </span>
                <span className="text-[10px] text-zinc-500 uppercase tracking-wide">{semaforoLabels.en}</span>
                {docTextOkMessage && (
                  <span className="text-[10px] px-2 py-1 rounded border border-emerald-500/40 text-emerald-300 bg-emerald-500/10">
                    Documento leggibile / Document readable
                  </span>
                )}
              </div>
              <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-sm text-zinc-500">
                <span className="font-mono">Case: {safeRender(analysis.case_id)}</span>
                <span>•</span>
                <span>{analysis.pages_count || '?'} pagine</span>
                <span>•</span>
                <span>{new Date(analysis.created_at).toLocaleString('it-IT')}</span>
                {isMultiLot && (
                  <>
                    <span>•</span>
                    <span className="text-gold font-semibold">{lots.length} Lotti</span>
                  </>
                )}
              </div>
              
              {/* Case Header with Evidence - support both formats */}
              <div className="mt-4 space-y-1">
                <HeadlineInlineField
                  label="Procedura"
                  fieldKey="procedura"
                  value={caseHeader.procedure?.value || caseHeader.procedure_id}
                  evidence={getEvidence(caseHeader.procedure || caseHeader.procedure_id)}
                  className="text-gold"
                />
                <HeadlineInlineField
                  fieldKey="tribunale"
                  value={normalizeSpacedOCR(caseHeader.tribunale?.value || caseHeader.tribunale)}
                  evidence={getEvidence(caseHeader.tribunale)}
                  className="text-zinc-400"
                />
                <HeadlineInlineField
                  fieldKey="address"
                  value={caseHeader.address?.value || caseHeader.address?.full || caseHeader.address}
                  evidence={getEvidence(caseHeader.address)}
                  className="text-zinc-400"
                />
              </div>
            </div>
            <div className="space-y-2 xl:max-w-sm xl:text-right">
              <div className="flex flex-col gap-2 sm:flex-row xl:justify-end">
                <Link
                  to={`/analysis/${analysisId}/print`}
                  target="_blank"
                  rel="noreferrer"
                  className="inline-flex items-center gap-2 rounded-md border border-zinc-700 px-3 py-2 text-sm text-zinc-200 transition-colors hover:border-zinc-500 hover:bg-zinc-800"
                >
                  <FileText className="w-4 h-4" />
                  Vista stampa
                </Link>
                <Button
                  disabled
                  data-testid="download-pdf-btn"
                  title="Temporaneamente non disponibile"
                  className="bg-gold text-zinc-950 hover:bg-gold-dim disabled:cursor-not-allowed disabled:opacity-60"
                >
                  <FileDown className="w-4 h-4 mr-2" />
                  Scarica Report
                </Button>
              </div>
              <div className="flex xl:justify-end">
                <SemaforoBadge status={semaforoStatus} />
              </div>
              <p className="ml-auto max-w-xs text-xs text-zinc-400">
                Sintesi operativa: priorità alle verifiche indicate.
              </p>
              <p className="ml-auto max-w-xs text-[11px] text-zinc-500">
                Operational summary: prioritize the checks listed.
              </p>
            </div>
          </div>
          {activeTab === 'overview' && (
            <>
              <p className="text-sm text-zinc-400 mt-3">
                {safeRender(semaforo.status_label || semaforo.reason_it || semaforo.status_it, '')}
              </p>
              <p className="text-xs text-zinc-500 mt-1">
                {safeRender(semaforo.reason_en || semaforo.status_en, '')}
              </p>
              <div className="xl:text-right">
                {/* Show driver/reason for semaforo */}
                {semaforo.driver?.value && (
                  <p className="text-xs text-amber-400 mt-1">
                    Driver: {semaforo.driver.value}
                  </p>
                )}
                {/* Show evidence pages */}
                {(getEvidence(semaforo.semaforo_complessivo || semaforo).length > 0) && (
                  <p className="mt-1 flex items-center gap-1 text-xs text-gold xl:justify-end">
                    <FileText className="w-3 h-3" />
                    Basato su pag. {[...new Set(getEvidence(semaforo.semaforo_complessivo || semaforo).map(e => e.page))].join(', ')}
                  </p>
                )}
              </div>

              {/* Quick Decision with Evidence */}
              <div className="mt-6 p-4 bg-zinc-950 rounded-lg border border-zinc-800">
                <div className="mb-2 flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                  <p className="text-xs font-mono uppercase text-zinc-500">Decisione Rapida</p>
                  <span className="text-[10px] px-2 py-1 rounded border border-zinc-700 text-zinc-400 uppercase tracking-wide">
                    {decisionSourceLabel}
                  </span>
                </div>
                <p className="text-lg font-semibold text-zinc-100">{displayDecisionIt}</p>
                <p className="text-sm text-zinc-500 mt-1">{displayDecisionEn}</p>
                {displayedDecisionBullets.length > 0 && (
                  <ul className="mt-3 space-y-1 text-sm text-zinc-300 list-disc pl-5">
                    {displayedDecisionBullets.map((item) => (
                      <li key={`it-bullet-${item.idx}`}>{safeRender(item.bullet, '')}</li>
                    ))}
                  </ul>
                )}
                {displayedDecisionBullets.some((item) => item.bulletEn) && (
                  <ul className="mt-2 space-y-1 text-xs text-zinc-500 list-disc pl-5">
                    {displayedDecisionBullets.filter((item) => item.bulletEn).map((item) => (
                      <li key={`en-bullet-${item.idx}`}>{safeRender(item.bulletEn, '')}</li>
                    ))}
                  </ul>
                )}
                
                {/* Mutuabilità if available */}
                {semaforo.mutuabilita_stimata && (
                  <div className="mt-3 p-2 bg-zinc-900 rounded">
                    <span className="text-xs text-zinc-500">Mutuabilità stimata: </span>
                    <span className="text-sm font-medium text-gold">{semaforo.mutuabilita_stimata.value}</span>
                    {semaforo.mutuabilita_stimata.reason && (
                      <p className="text-xs text-zinc-400 mt-1">{semaforo.mutuabilita_stimata.reason}</p>
                    )}
                  </div>
                )}
                
                {/* Driver Rosso with Evidence - old format */}
                {displayedDrivers.length > 0 && (
                  <div className="mt-4 space-y-2">
                    <p className="text-xs font-mono text-red-400 uppercase">Criticità Rilevate:</p>
                    {displayedDrivers.map((driver, idx) => (
                      <div key={idx} className="p-2 bg-red-500/10 rounded border border-red-500/20">
                        <div className="flex flex-wrap items-center gap-2">
                          <AlertTriangle className="w-4 h-4 text-red-400" />
                          <span className="text-sm text-red-400">{safeRender(driver.headline_it)}</span>
                          {getEvidence(driver).length > 0 && (
                            <span className="text-xs font-mono text-gold">
                              p. {[...new Set(getEvidence(driver).map(e => e.page))].join(', ')}
                            </span>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </>
          )}
        </div>
        
        {/* Tabs */}
        <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
          <TabsList className="mb-6 h-auto w-full justify-start overflow-x-auto border border-zinc-800 bg-zinc-900 p-1">
            <TabsTrigger value="overview" data-testid="tab-overview">Panoramica</TabsTrigger>
            <TabsTrigger value="costs" data-testid="tab-costs">Costi</TabsTrigger>
            <TabsTrigger value="legal" data-testid="tab-legal">Legal Killers</TabsTrigger>
            <TabsTrigger value="details" data-testid="tab-details">Dettagli</TabsTrigger>
            <TabsTrigger value="flags" data-testid="tab-flags">Red Flags</TabsTrigger>
          </TabsList>
          
          {/* Overview Tab */}
          <TabsContent value="overview" className="space-y-6">
            {/* Summary for Client - Section 12 Style */}
            <div className="relative rounded-xl border border-zinc-800 bg-zinc-900 p-5 sm:p-6">
              {/* QA Badge - Small Corner */}
              <div className={`mb-4 inline-flex items-center gap-1 rounded px-2 py-1 text-xs font-mono sm:absolute sm:right-4 sm:top-4 ${
                safeRender(qa.status) === 'PASS' ? 'bg-emerald-500/20 text-emerald-400' :
                safeRender(qa.status) === 'FAIL' ? 'bg-red-500/20 text-red-400' :
                'bg-amber-500/20 text-amber-400'
              }`}>
                {safeRender(qa.status) === 'PASS' ? (
                  <CheckCircle className="w-3 h-3" />
                ) : safeRender(qa.status) === 'FAIL' ? (
                  <XCircle className="w-3 h-3" />
                ) : (
                  <AlertTriangle className="w-3 h-3" />
                )}
                <span>QA: {safeRender(qa.status, 'PENDING')}</span>
              </div>
              
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">
                📋 Summary for Client
              </h2>
              
              {/* Recommendation - Main message */}
              {(summary.raccomandazione || summary.summary_it) && (
                <div className="mb-4 space-y-3">
                  {summary.raccomandazione && (
                    <div className="p-4 bg-amber-500/10 border-l-4 border-amber-500 rounded-r-lg">
                      <p className="text-zinc-200 font-medium">
                        ⚠️ {safeRender(summary.raccomandazione)}
                      </p>
                    </div>
                  )}
                  
                  <div className="p-4 bg-zinc-950 rounded-lg">
                    <p className="text-zinc-300 leading-relaxed">{safeRender(summary.summary_it, 'Analisi documento completata.')}</p>
                  </div>
                  
                  {summary.summary_en && (
                    <div className="p-4 bg-zinc-950/50 rounded-lg border-l-2 border-gold/30">
                      <p className="text-zinc-400 text-sm italic">{safeRender(summary.summary_en)}</p>
                    </div>
                  )}
                </div>
              )}
              
              {/* Disclaimer */}
              <div className="text-xs text-zinc-600 mt-4 pt-4 border-t border-zinc-800">
                <p>📌 {safeRender(summary.disclaimer_it, 'Documento informativo. Non costituisce consulenza legale.')}</p>
              </div>
            </div>

            {/* Case Summary - Principal Facts */}
            <div className="rounded-xl border border-zinc-800 bg-zinc-900 p-5 sm:p-6">
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Case Summary</h2>
              <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
                <PanoramicaDataValueCard
                  label="Tribunale"
                  value={overviewTribunale}
                  evidence={overviewTribunaleEvidence}
                />
                <PanoramicaDataValueCard
                  label="Procedura"
                  value={overviewProcedura}
                  evidence={overviewProceduraEvidence}
                />
                <PanoramicaDataValueCard
                  label="Lotto"
                  value={overviewLotto}
                  evidence={overviewLottoEvidence}
                />
                <PanoramicaDataValueCard
                  label="Stato Occupativo"
                  value={formatFieldStateDisplay('stato_occupativo', occupativo.status_it || occupativo.status)}
                  evidence={getFieldEvidence('stato_occupativo', occupativo)}
                />
                <PanoramicaDataValueCard
                  label="Spese Condominiali Arretrate"
                  value={formatFieldStateDisplay('spese_condominiali_arretrate', result.spese_condominiali_arretrate || result.spese_condominiali)}
                  evidence={getFieldEvidence('spese_condominiali_arretrate', result.spese_condominiali_arretrate || result.spese_condominiali)}
                />
                <PanoramicaDataValueCard
                  label="APE"
                  value={getSurfaceDisplayValue('ape', abusi.ape, result.ape)}
                  evidence={getFieldEvidence('ape', abusi.ape || result.ape)}
                />
                {datiAsta && (
                  <PanoramicaDataValueCard
                    label="Dati Asta"
                    value={safeRender(datiAsta?.data || datiAsta?.value || datiAsta)}
                    evidence={getEvidence(datiAsta)}
                  />
                )}
              </div>
            </div>

            {/* Composizione Lotto / Lot Composition */}
            {lotCompositionItems.length > 0 && (
              <div className="rounded-xl border border-zinc-800 bg-zinc-900 p-5 sm:p-6">
                <h2 className="text-lg font-serif font-bold text-zinc-100 mb-3">Composizione Lotto / Lot Composition</h2>
                <p className="text-xs text-zinc-500 mb-4">
                  {lotCompositionItems.length > 1
                    ? `Composizione rilevata del lotto selezionato (${lotCompositionItems.length} beni).`
                    : 'Composizione rilevata del lotto selezionato (1 bene).'}
                </p>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  {lotCompositionItems.map((item) => (
                    <div key={item.key} className="p-4 bg-zinc-950 rounded-lg border border-zinc-800">
                      <div className="flex items-center justify-between gap-2">
                        <p className="text-sm font-semibold text-zinc-100">{item.title}</p>
                        {item.evidence.length > 0 && <EvidenceBadge evidence={item.evidence} />}
                      </div>
                      <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 mt-3">
                        {item.tipologia && (
                          <div>
                            <p className="text-[11px] uppercase tracking-wide text-zinc-500">Tipologia</p>
                            <p className="text-sm text-zinc-200">{item.tipologia}</p>
                          </div>
                        )}
                        {item.location && (
                          <div>
                            <p className="text-[11px] uppercase tracking-wide text-zinc-500">Ubicazione</p>
                            <p className="text-sm text-zinc-200">{item.location}</p>
                          </div>
                        )}
                        {item.piano && (
                          <div>
                            <p className="text-[11px] uppercase tracking-wide text-zinc-500">Piano</p>
                            <p className="text-sm text-zinc-200">{item.piano}</p>
                          </div>
                        )}
                        {item.superficie && item.superficie !== 'Non specificato in perizia' && (
                          <div>
                            <p className="text-[11px] uppercase tracking-wide text-zinc-500">Superficie</p>
                            <p className="text-sm text-zinc-200">{item.superficie}</p>
                          </div>
                        )}
                        {item.valoreStima && item.valoreStima !== 'Non specificato in perizia' && (
                          <div>
                            <p className="text-[11px] uppercase tracking-wide text-zinc-500">Valore stima bene</p>
                            <p className="text-sm font-mono text-gold">{item.valoreStima}</p>
                          </div>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Riepilogo Costi (Stima Compatta) */}
            <div className="rounded-xl border border-zinc-800 bg-zinc-900 p-5 sm:p-6">
              <h2 className="text-lg font-serif font-bold text-zinc-100 mb-3">Sintesi costi extra (stima) / Compact extra-cost estimate</h2>
              {hasMoneyBoxTotalRange ? (
                <div className="space-y-2">
                  <div className="flex flex-col gap-1 text-sm sm:flex-row sm:items-center sm:justify-between sm:gap-3">
                    <span className="text-zinc-400">Da perizia / Document-backed</span>
                    <span className="font-mono text-emerald-300">
                      €{documentBackedMin.toLocaleString()} - €{documentBackedMax.toLocaleString()}
                    </span>
                  </div>
                  <div className="flex flex-col gap-1 text-sm sm:flex-row sm:items-center sm:justify-between sm:gap-3">
                    <span className="text-zinc-400">Stime Nexodify / Estimated missing items</span>
                    <span className="font-mono text-gold">
                      €{estimatedMin.toLocaleString()} - €{estimatedMax.toLocaleString()}
                    </span>
                  </div>
                  <div className="mt-3 flex flex-col gap-1 border-t border-zinc-800 pt-3 sm:flex-row sm:items-center sm:justify-between sm:gap-3">
                    <span className="text-zinc-200 font-medium">Totale costi extra / Total extra costs</span>
                    <span className="font-mono font-bold text-zinc-100">
                      €{moneyBoxTotalRange.min_eur.toLocaleString()} - €{moneyBoxTotalRange.max_eur.toLocaleString()}
                    </span>
                  </div>
                  <p className="text-xs text-zinc-500 mt-2">
                    Le voci rilevate automaticamente dalla perizia non sono incluse in questo totale finché non sono validate.
                  </p>
                  <p className="text-[11px] text-zinc-600">
                    Automatically detected cost mentions from the appraisal are not included in this total until validated.
                  </p>
                  <p className="text-xs text-zinc-500 mt-2">
                    Quadro sintetico indicativo: non sostituisce la tassonomia costi completa del contratto.
                  </p>
                </div>
              ) : (
                <>
                  <p className="text-sm text-zinc-400">Costi extra: non disponibili</p>
                  <p className="text-xs text-zinc-500 mt-1">Extra costs: unavailable</p>
                </>
              )}
            </div>
            
            {/* Key Data Grid with Evidence */}
            <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
              <PanoramicaDataValueCard
                label="Prezzo Base" 
                value={overviewPrezzoBaseValue}
                evidence={overviewPrezzoBaseEvidence}
                valueClassName="text-gold text-xl"
              />
              <PanoramicaDataValueCard
                label="Superficie" 
                value={getSurfaceDisplayValue('superficie', dati?.superficie, dati?.superficie?.value, dati?.superficie, contractLotSummary?.superficie_mq)}
                evidence={getFieldEvidence('superficie', dati?.superficie)}
              />
              <PanoramicaDataValueCard
                label="Stato Occupativo" 
                value={normalizeOverviewValue(occupativo.status_it || occupativo.status)}
                evidence={getEvidence(occupativo)}
              />
              <PanoramicaDataValueCard
                label="Conformità Urbanistica" 
                value={getSurfaceDisplayValue('regolarita_urbanistica', abusi.conformita_urbanistica)}
                evidence={getFieldEvidence('regolarita_urbanistica', abusi.conformita_urbanistica)}
              />
              <PanoramicaDataValueCard
                label="Conformità Catastale" 
                value={normalizeOverviewValue(abusi.conformita_catastale?.status)}
                evidence={getEvidence(abusi.conformita_catastale)}
              />
              <PanoramicaDataValueCard
                label="Agibilità/Abitabilità" 
                value={normalizeOverviewValue(abusi.agibilita?.status)}
                evidence={getEvidence(abusi.agibilita)}
              />
              <PanoramicaDataValueCard
                label="APE (Certificato Energetico)" 
                value={getSurfaceDisplayValue('ape', abusi.ape)}
                evidence={getFieldEvidence('ape', abusi.ape)}
              />
              <PanoramicaDataValueCard
                label="Diritto Reale" 
                value={splitQuotaFromDiritto(getRichFieldDisplayValue('diritto_reale', null, dati.diritto_reale?.value || dati.diritto_reale), getRichFieldDisplayValue('quota', null, dati?.quota?.value || dati?.quota))}
                evidence={getEvidence(dati.diritto_reale)}
              />
              <PanoramicaDataValueCard
                label="Quota"
                value={getSurfaceDisplayValue('quota', dati?.quota, dati?.quota?.value, dati?.quota)}
                evidence={getEvidence(dati?.quota)}
              />
            </div>

            {/* Waterfall Valutativo / Valuation Waterfall */}
            {canRenderValuationWaterfall && (
              <div className="rounded-xl border border-zinc-800 bg-zinc-900 p-5 sm:p-6">
                <h2 className="text-lg font-serif font-bold text-zinc-100 mb-3">Waterfall Valutativo / Valuation Waterfall</h2>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
                  <div className="p-3 bg-zinc-950 rounded-lg border border-zinc-800">
                    <div className="flex items-center justify-between gap-2">
                      <p className="text-xs text-zinc-500">Valore di stima</p>
                      {waterfallStimaEvidence.length > 0 && <EvidenceBadge evidence={waterfallStimaEvidence} />}
                    </div>
                    <p className="mt-2 text-lg font-mono font-semibold text-zinc-100">€{waterfallStimaValue.toLocaleString()}</p>
                  </div>
                  <div className="p-3 bg-zinc-950 rounded-lg border border-zinc-800">
                    <div className="flex items-center justify-between gap-2">
                      <p className="text-xs text-zinc-500">Deprezzamenti</p>
                      {waterfallDeprezzamentiEvidence.length > 0 && <EvidenceBadge evidence={waterfallDeprezzamentiEvidence} />}
                    </div>
                    <p className="mt-2 text-lg font-mono font-semibold text-amber-300">- €{waterfallDeprezzamentiValue.toLocaleString()}</p>
                  </div>
                  <div className="p-3 bg-zinc-950 rounded-lg border border-zinc-800">
                    <div className="flex items-center justify-between gap-2">
                      <p className="text-xs text-zinc-500">Valore finale</p>
                      {waterfallFinaleEvidence.length > 0 && <EvidenceBadge evidence={waterfallFinaleEvidence} />}
                    </div>
                    <p className="mt-2 text-lg font-mono font-semibold text-gold">€{waterfallFinaleValue.toLocaleString()}</p>
                  </div>
                  <div className="p-3 bg-zinc-950 rounded-lg border border-zinc-800">
                    <div className="flex items-center justify-between gap-2">
                      <p className="text-xs text-zinc-500">Prezzo base d'asta</p>
                      {waterfallPrezzoBaseEvidence.length > 0 && <EvidenceBadge evidence={waterfallPrezzoBaseEvidence} />}
                    </div>
                    <p className="mt-2 text-lg font-mono font-semibold text-emerald-300">€{waterfallPrezzoBaseValue.toLocaleString()}</p>
                  </div>
                </div>
              </div>
            )}
            
            {/* Impianti Section */}
            {abusi.impianti && (
              <div className="rounded-xl border border-zinc-800 bg-zinc-900 p-4">
                <h3 className="text-sm font-semibold text-zinc-100 mb-3">Conformità Impianti</h3>
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
                  <div className={`text-center p-3 rounded-lg ${
                    abusi.impianti.elettrico?.conformita === 'SI' ? 'bg-emerald-500/10 border border-emerald-500/30' :
                    abusi.impianti.elettrico?.conformita === 'NO' ? 'bg-red-500/10 border border-red-500/30' :
                    'bg-amber-500/10 border border-amber-500/30'
                  }`}>
                    <p className="text-xs text-zinc-400 mb-1">⚡ Elettrico</p>
                    <p className={`text-sm font-bold ${
                      abusi.impianti.elettrico?.conformita === 'SI' ? 'text-emerald-400' :
                      abusi.impianti.elettrico?.conformita === 'NO' ? 'text-red-400' :
                      'text-amber-400'
                    }`}>{abusi.impianti.elettrico?.conformita || 'NON RISULTA'}</p>
                  </div>
                  <div className={`text-center p-3 rounded-lg ${
                    abusi.impianti.termico?.conformita === 'SI' ? 'bg-emerald-500/10 border border-emerald-500/30' :
                    abusi.impianti.termico?.conformita === 'NO' ? 'bg-red-500/10 border border-red-500/30' :
                    'bg-amber-500/10 border border-amber-500/30'
                  }`}>
                    <p className="text-xs text-zinc-400 mb-1">🔥 Termico</p>
                    <p className={`text-sm font-bold ${
                      abusi.impianti.termico?.conformita === 'SI' ? 'text-emerald-400' :
                      abusi.impianti.termico?.conformita === 'NO' ? 'text-red-400' :
                      'text-amber-400'
                    }`}>{abusi.impianti.termico?.conformita || 'NON RISULTA'}</p>
                  </div>
                  <div className={`text-center p-3 rounded-lg ${
                    abusi.impianti.idrico?.conformita === 'SI' ? 'bg-emerald-500/10 border border-emerald-500/30' :
                    abusi.impianti.idrico?.conformita === 'NO' ? 'bg-red-500/10 border border-red-500/30' :
                    'bg-amber-500/10 border border-amber-500/30'
                  }`}>
                    <p className="text-xs text-zinc-400 mb-1">💧 Idrico</p>
                    <p className={`text-sm font-bold ${
                      abusi.impianti.idrico?.conformita === 'SI' ? 'text-emerald-400' :
                      abusi.impianti.idrico?.conformita === 'NO' ? 'text-red-400' :
                      'text-amber-400'
                    }`}>{abusi.impianti.idrico?.conformita || 'NON RISULTA'}</p>
                  </div>
                </div>
              </div>
            )}
            
            {/* Scenario Indicativo */}
            {canRenderAllIn && (
              <div className="rounded-xl border border-gold/30 bg-gold/10 p-5 sm:p-6">
                <h3 className="text-lg font-semibold text-zinc-100 mb-2">Scenario indicativo (stima)</h3>
                <p className="text-zinc-300 mb-4 text-sm">{safeRender(indice.lettura_secca_it || indice.dry_read_it, 'Calcolo basato su prezzo base + extra budget stimato')}</p>
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
                  <div className="text-center p-3 bg-zinc-950 rounded-lg">
                    <p className="text-xs text-zinc-500 mb-1">PREZZO BASE</p>
                    <p className="text-lg font-mono font-bold text-zinc-300">€{prezzoBaseValue.toLocaleString()}</p>
                  </div>
                  <div className="text-center p-3 bg-zinc-950 rounded-lg">
                    <p className="text-xs text-zinc-500 mb-1">ALL-IN MIN</p>
                    <p className="text-lg font-mono font-bold text-gold">€{allInMin.toLocaleString()}</p>
                  </div>
                  <div className="text-center p-3 bg-zinc-950 rounded-lg">
                    <p className="text-xs text-zinc-500 mb-1">ALL-IN MAX</p>
                    <p className="text-lg font-mono font-bold text-gold">€{allInMax.toLocaleString()}</p>
                  </div>
                </div>
                {hasMoneyBoxTotalRange && (
                  <p className="text-xs text-zinc-500 mt-3 text-center">
                    Extra budget: €{moneyBoxTotalRange.min_eur.toLocaleString()} - €{moneyBoxTotalRange.max_eur.toLocaleString()}
                  </p>
                )}
                <p className="text-xs text-zinc-500 mt-2 text-center">
                  Basato su extra stimati: non sostituisce verifica tecnica e legale.
                </p>
              </div>
            )}
          </TabsContent>
          
          {/* Costs Tab */}
          <TabsContent value="costs" className="space-y-6">
            <div className="money-box-card p-6">
              <div className="flex items-center gap-3 mb-2">
                <DollarSign className="w-6 h-6 text-gold" />
                <h2 className="text-xl font-serif font-bold text-zinc-100">Costi / Costs</h2>
              </div>
              <p className="text-xs text-zinc-500 mb-6">
                Sezioni separate per evitare mix fuorviante tra deprezzamenti, costi espliciti e stime.
              </p>

              <div className="p-4 rounded-lg border border-zinc-800 bg-zinc-950/60 mb-5">
                <h3 className="text-sm font-semibold text-zinc-100 mb-2">Deprezzamenti da perizia / Perizia valuation adjustments</h3>
                {valuationDeprezzamentiValue !== null ? (
                  <>
                    <p className="text-lg font-mono font-semibold text-amber-300">- €{valuationDeprezzamentiValue.toLocaleString()}</p>
                    {valuationDeprezzamentiIsComputed ? (
                      <div className="mt-3 rounded-md border border-amber-500/20 bg-zinc-950/70 p-3">
                        <p className="text-xs font-medium uppercase tracking-wide text-amber-300">
                          {safeRender(valuationDeprezzamentiMeta?.label_it, 'Deprezzamento totale calcolato da valori in perizia')}
                        </p>
                        <div className="mt-2 space-y-2 text-sm text-zinc-300">
                          {valuationDeprezzamentiGrossValue !== null && (
                            <div>
                              <p className="flex items-center gap-1">
                                <span>{safeRender(valuationDeprezzamentiMeta?.gross_label_it, 'Valore di stima lordo')}: €{valuationDeprezzamentiGrossValue.toLocaleString()}</span>
                                {valuationDeprezzamentiGrossEvidence.length > 0 && <EvidenceBadge evidence={valuationDeprezzamentiGrossEvidence} />}
                              </p>
                            </div>
                          )}
                          {valuationDeprezzamentiFinalValue !== null && (
                            <div>
                              <p className="flex items-center gap-1">
                                <span>{safeRender(valuationDeprezzamentiMeta?.final_label_it, 'Valore finale / prezzo base')}: €{valuationDeprezzamentiFinalValue.toLocaleString()}</span>
                                {valuationDeprezzamentiFinalEvidence.length > 0 && <EvidenceBadge evidence={valuationDeprezzamentiFinalEvidence} />}
                              </p>
                            </div>
                          )}
                          {valuationDeprezzamentiGrossValue !== null && valuationDeprezzamentiFinalValue !== null && valuationDeprezzamentiComputedValue !== null && (
                            <p className="font-mono text-xs text-zinc-400">
                              Calcolo: €{valuationDeprezzamentiGrossValue.toLocaleString()} - €{valuationDeprezzamentiFinalValue.toLocaleString()} = €{valuationDeprezzamentiComputedValue.toLocaleString()}
                            </p>
                          )}
                        </div>
                        {valuationDeprezzamentiEvidence.length > 0 && (
                          <div className="mt-3">
                            <EvidenceDetail evidence={valuationDeprezzamentiEvidence} />
                          </div>
                        )}
                      </div>
                    ) : valuationDeprezzamentiEvidence.length > 0 && (
                      <div className="mt-2">
                        <EvidenceDetail evidence={valuationDeprezzamentiEvidence} />
                      </div>
                    )}
                    <p className="text-xs text-zinc-500 mt-2">
                      Voce di deprezzamento della valutazione in perizia: non equivale automaticamente a cassa extra lato acquirente.
                    </p>
                  </>
                ) : (
                  <p className="text-sm text-zinc-500">Non specificato in perizia</p>
                )}
              </div>

              {moneyBoxTotalRange && typeof moneyBoxTotalRange.min_eur === 'number' && typeof moneyBoxTotalRange.max_eur === 'number' && (
                <div className="mb-5 p-4 bg-zinc-950 rounded-lg border border-gold/30">
                  <p className="text-lg font-semibold text-zinc-100">
                    Scenario stima extra-costi (escl. deprezzamenti): €{moneyBoxTotalRange.min_eur.toLocaleString()} - €{moneyBoxTotalRange.max_eur.toLocaleString()}
                  </p>
                  <p className="text-xs text-zinc-500 mt-1">
                    Range indicativo di extra-costi; le stime Nexodify sono assunzioni e non frasi dirette della perizia.
                  </p>
                </div>
              )}

              <div className="p-4 rounded-lg border border-zinc-800 bg-zinc-950/40 mb-5">
                <h3 className="text-sm font-semibold text-zinc-100 mb-3">Costi espliciti citati nel testo / Explicit cost mentions from text</h3>
                {explicitCostMentions.length > 0 ? (
                  <div className="space-y-3">
                    {explicitCostMentions.map((item, index) => (
                      <MoneyBoxItem key={`explicit_${index}`} item={item} />
                    ))}
                  </div>
                ) : (
                  <p className="text-zinc-500 text-sm">Nessun costo esplicito affidabile disponibile.</p>
                )}
              </div>

              <div className="p-4 rounded-lg border border-zinc-800 bg-zinc-950/40">
                <h3 className="text-sm font-semibold text-zinc-100 mb-3">Stime Nexodify / Nexodify estimates</h3>
                <p className="text-xs text-zinc-500 mb-3">
                  Voci canoniche A-H mostrate come stime/assunzioni operative, non come dichiarazioni dirette della perizia.
                </p>
                {isConservativeCostMode ? (
                  qualitativeBurdens.length > 0 ? (
                    <div className="space-y-2">
                      {qualitativeBurdens.map((item, index) => (
                        <div key={`burden_${index}`} className="rounded-lg border border-zinc-800 bg-zinc-950/60 px-4 py-3">
                          <p className="text-sm font-medium text-zinc-100">
                            {safeRender(item?.label_it || item?.label || item?.voce, 'Onere qualitativo da verificare')}
                          </p>
                          <p className="text-xs text-zinc-500 mt-1">
                            Onere grounded lato acquirente, non quantificato in modo difendibile dalla perizia.
                          </p>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="text-zinc-500 text-sm">Nessun onere qualitativo grounded disponibile.</p>
                  )
                ) : nexodifyEstimateItems.length > 0 ? (
                  <div className="space-y-3">
                    {nexodifyEstimateItems.map((item, index) => (
                      <MoneyBoxItem key={`nexo_${index}`} item={item} />
                    ))}
                  </div>
                ) : (
                  <p className="text-zinc-500 text-sm">Nessuna stima Nexodify disponibile.</p>
                )}
              </div>

              {/* Total - support TBD and numeric totals */}
              {(moneyBoxTotal || moneyBox.total_extra_costs) && !moneyBoxTotalRange && (
                <div className="mt-6 p-4 bg-gold/10 border border-gold/30 rounded-lg">
                  <div className="flex items-center justify-between">
                    <span className="text-lg font-semibold text-zinc-100">Totale stima extra-costi (escl. deprezzamenti)</span>
                    <span className={`text-2xl font-mono font-bold ${(isConservativeCostMode || isTotalTBD || !moneyBoxNumericTotal) ? 'text-amber-400' : 'text-gold'}`}>
                      {isConservativeCostMode || isTotalTBD || !moneyBoxNumericTotal ? (
                        'NON QUANTIFICATO IN PERIZIA'
                      ) : (
                        `€${moneyBoxNumericTotal.min.toLocaleString()} - €${moneyBoxNumericTotal.max.toLocaleString()}`
                      )}
                      {!isConservativeCostMode && !isTotalTBD && moneyBoxNumericTotal && (moneyBoxTotal?.nota?.includes('+') || moneyBox.total_extra_costs?.max_is_open) && '+'}
                    </span>
                  </div>
                  {moneyBoxTotal?.nota && (
                    <p className="text-xs text-zinc-400 mt-2">{moneyBoxTotal.nota}</p>
                  )}
                  {(isConservativeCostMode || isTotalTBD || !moneyBoxNumericTotal) && (
                    <p className="text-xs text-amber-400 mt-2">
                      Costi non quantificati in perizia. Verifica tecnico/legale obbligatoria.
                    </p>
                  )}
                </div>
              )}
            </div>
          </TabsContent>
          
          {/* Legal Killers Tab */}
          <TabsContent value="legal" className="space-y-6">
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <div className="flex items-center gap-3 mb-2">
                <Scale className="w-6 h-6 text-red-400" />
                <h2 className="text-xl font-serif font-bold text-zinc-100">Legal Killers Checklist</h2>
              </div>
              <p className="text-xs text-zinc-500 mb-6">
                Verifiche critiche che possono bloccare l'acquisto. I riferimenti alle pagine indicano dove verificare nel documento.
              </p>
              
              {Object.keys(legalKillersObj).length > 0 ? (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {Object.entries(legalKillersObj).map(([key, value]) => (
                    <LegalKillerItem key={key} name={key} data={value} />
                  ))}
                </div>
              ) : (
                <p className="text-zinc-500 text-center py-8">Nessun blocker legale materiale con evidenza sostanziale disponibile</p>
              )}

              {groupedLegalDetailSections.length > 0 && (
                <details className="mt-6 p-4 rounded-lg border border-zinc-800 bg-zinc-950/60">
                  <summary className="cursor-pointer text-sm font-semibold text-zinc-100 select-none">
                    Approfondisci evidenze legali
                  </summary>
                  <div className="space-y-4 mt-3">
                    {groupedLegalDetailSections.map((group) => (
                      <div key={group.key} className="p-3 rounded border border-zinc-800 bg-zinc-900/70">
                        <h3 className="text-xs uppercase tracking-wide text-zinc-400">{group.title}</h3>
                        <div className="space-y-2 mt-2">
                          {group.items.map((item) => (
                            <div key={item.key} className="p-2 rounded border border-zinc-800 bg-zinc-900">
                              <div className="flex items-center justify-between gap-2">
                                <p className="text-sm text-zinc-100">
                                  <span className="font-medium">{item.labelIt}</span>
                                  {item.displayValue ? `: ${item.displayValue}` : ''}
                                </p>
                                {item.evidence.length > 0 && <EvidenceBadge evidence={item.evidence} />}
                              </div>
                              {item.labelEn && <p className="text-xs text-zinc-500 mt-1">{item.labelEn}</p>}
                            </div>
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>
                </details>
              )}
            </div>
          </TabsContent>
          
          {/* Details Tab */}
          <TabsContent value="details" className="space-y-6">
            {/* Case Header */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Dati Procedura</h2>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <HeadlineFieldCard
                  label="Procedura"
                  fieldKey="procedura"
                  value={caseHeader.procedure?.value || caseHeader.procedure_id}
                  evidence={getEvidence(caseHeader.procedure || caseHeader.procedure_id)}
                />
                <HeadlineFieldCard
                  label="Lotto"
                  fieldKey="lotto"
                  value={caseHeader.lotto?.value || caseHeader.lotto}
                  evidence={getEvidence(caseHeader.lotto)}
                />
                <HeadlineFieldCard
                  label="Tribunale"
                  fieldKey="tribunale"
                  value={normalizeSpacedOCR(caseHeader.tribunale?.value || caseHeader.tribunale)}
                  evidence={getEvidence(caseHeader.tribunale)}
                />
                <HeadlineFieldCard
                  label="Indirizzo"
                  fieldKey="address"
                  value={caseHeader.address?.value || caseHeader.address?.full || caseHeader.address}
                  evidence={getEvidence(caseHeader.address)}
                />
              </div>
            </div>

            {/* Per-bene detailed cards (contract-first, deterministic only) */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">
                {isLotFirstDetailScope ? 'Dettagli per lotto' : 'Dettagli per bene'}
              </h2>
              {(isLotFirstDetailScope ? detailsLotCards.length > 0 : detailsBeneCards.length > 0) ? (
                <div className="space-y-4">
                  {(isLotFirstDetailScope ? detailsLotCards : detailsBeneCards).map((card) => (
                    <div key={card.key} className="p-4 bg-zinc-950 rounded-lg border border-zinc-800">
                      <div className="flex items-start justify-between gap-3">
                        <p className="text-sm font-semibold text-zinc-100">
                          {isLotFirstDetailScope
                            ? `Lotto ${card.lotNumber}${card.tipologia ? ` - ${card.tipologia}` : ''}`
                            : `Bene ${card.beneNumber}${card.tipologia ? ` - ${card.tipologia}` : ''}`}
                        </p>
                        {card.topEvidence.length > 0 && <EvidenceBadge evidence={card.topEvidence} />}
                      </div>

                      <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mt-3">
                        {card.shortLocation && (
                          <div>
                            <p className="text-[11px] uppercase tracking-wide text-zinc-500">
                              {isLotFirstDetailScope ? 'Ubicazione' : 'Short location'}
                            </p>
                            <p className="text-sm text-zinc-200">{card.shortLocation}</p>
                          </div>
                        )}
                        {!isLotFirstDetailScope && card.piano && (
                          <div>
                            <p className="text-[11px] uppercase tracking-wide text-zinc-500">Piano</p>
                            <p className="text-sm text-zinc-200">{card.piano}</p>
                          </div>
                        )}
                        {card.superficie && (
                          <div>
                            <p className="text-[11px] uppercase tracking-wide text-zinc-500">Superficie</p>
                            <p className="text-sm text-zinc-200">{card.superficie}</p>
                          </div>
                        )}
                        {card.valoreStima && (
                          <div>
                            <p className="text-[11px] uppercase tracking-wide text-zinc-500">
                              {isLotFirstDetailScope ? 'Valore di stima lotto' : 'Valore stima bene'}
                            </p>
                            <p className="text-sm font-mono text-gold">{card.valoreStima}</p>
                          </div>
                        )}
                        {isLotFirstDetailScope && card.prezzoBase && (
                          <div>
                            <p className="text-[11px] uppercase tracking-wide text-zinc-500">Prezzo base</p>
                            <p className="text-sm font-mono text-emerald-300">{card.prezzoBase}</p>
                          </div>
                        )}
                      </div>

                      <div className="mt-4 pt-3 border-t border-zinc-800 grid grid-cols-1 md:grid-cols-2 gap-3">
                        {card.detailRows.map((row) => (
                          row.value ? (
                            <div key={`${card.key}_${row.key}`} className="p-3 rounded border border-zinc-800 bg-zinc-900/70">
                              <div className="flex items-center justify-between gap-2">
                                <p className="text-[11px] uppercase tracking-wide text-zinc-500">{row.label}</p>
                                {row.evidence.length > 0 && <EvidenceBadge evidence={row.evidence} />}
                              </div>
                              <p className="text-sm text-zinc-200 mt-1">{row.value}</p>
                            </div>
                          ) : null
                        ))}
                      </div>

                      {isLotFirstDetailScope && Array.isArray(card.subordinateBeni) && card.subordinateBeni.length > 0 && (
                        <div className="mt-4 pt-3 border-t border-zinc-800">
                          <p className="text-[11px] uppercase tracking-wide text-zinc-500 mb-3">Beni subordinati</p>
                          <div className="space-y-2">
                            {card.subordinateBeni.map((bene) => (
                              <div key={`${card.key}_${bene.key}`} className="p-3 rounded border border-zinc-800 bg-zinc-900/70">
                                <p className="text-sm text-zinc-100 font-medium">{bene.title}</p>
                                <p className="text-sm text-zinc-300 mt-1">{bene.value}</p>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}

                      {!isLotFirstDetailScope && Array.isArray(card.declarationRows) && card.declarationRows.some((row) => row?.value) && (
                        <div className="mt-4 pt-3 border-t border-zinc-800">
                          <p className="text-[11px] uppercase tracking-wide text-zinc-500 mb-3">Certificazioni / Dichiarazioni</p>
                          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                            {card.declarationRows.map((row) => (
                              row.value ? (
                                <div key={`${card.key}_${row.key}`} className="p-3 rounded border border-zinc-800 bg-zinc-900/70">
                                  <div className="flex items-center justify-between gap-2">
                                    <p className="text-[11px] uppercase tracking-wide text-zinc-500">{row.label}</p>
                                    {row.evidence.length > 0 && <EvidenceBadge evidence={row.evidence} />}
                                  </div>
                                  <p className="text-sm text-zinc-200 mt-1">{row.value}</p>
                                </div>
                              ) : null
                            ))}
                          </div>
                        </div>
                      )}

                      {!isLotFirstDetailScope && Array.isArray(card.impiantiRows) && card.impiantiRows.some((row) => row?.value) && (
                        <div className="mt-4 pt-3 border-t border-zinc-800">
                          <div className="flex items-center justify-between gap-2 mb-3">
                            <p className="text-[11px] uppercase tracking-wide text-zinc-500">Impianti</p>
                            {Array.isArray(card.impiantiTopEvidence) && card.impiantiTopEvidence.length > 0 && (
                              <EvidenceBadge evidence={card.impiantiTopEvidence} />
                            )}
                          </div>
                          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                            {card.impiantiRows.map((row) => (
                              row.value ? (
                                <div key={`${card.key}_${row.key}`} className="p-3 rounded border border-zinc-800 bg-zinc-900/70">
                                  <div className="flex items-center justify-between gap-2">
                                    <p className="text-[11px] uppercase tracking-wide text-zinc-500">{row.label}</p>
                                    {row.evidence.length > 0 && <EvidenceBadge evidence={row.evidence} />}
                                  </div>
                                  <p className="text-sm text-zinc-200 mt-1">{row.value}</p>
                                </div>
                              ) : null
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-zinc-500">
                  {isLotFirstDetailScope
                    ? 'Lotti non disponibili nell\'estrazione corrente.'
                    : 'Beni non disponibili nell\'estrazione corrente.'}
                </p>
              )}
            </div>

            {/* Compact lot-level details: deterministic values only */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Dettagli lotto (compatti)</h2>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <DataValueWithEvidence
                  label="Conformità Urbanistica"
                  value={getSurfaceDisplayValue('regolarita_urbanistica', abusi.conformita_urbanistica)}
                  evidence={getFieldEvidence('regolarita_urbanistica', abusi.conformita_urbanistica)}
                />
                <DataValueWithEvidence
                  label="Agibilità/Abitabilità"
                  value={safeRender(abusi.agibilita?.status || abusi.agibilita, 'Non specificato in perizia')}
                  evidence={getFieldEvidence('agibilita', abusi.agibilita)}
                />
                <DataValueWithEvidence
                  label="APE (Certificato Energetico)"
                  value={getSurfaceDisplayValue('ape', abusi.ape)}
                  evidence={getFieldEvidence('ape', abusi.ape)}
                />
                <DataValueWithEvidence
                  label="Dati Asta"
                  value={safeRender(datiAsta?.data || datiAsta?.value || datiAsta, 'Non specificato in perizia')}
                  evidence={getEvidence(datiAsta)}
                />
              </div>
            </div>
          </TabsContent>
          
          {/* Red Flags Tab */}
          <TabsContent value="flags" className="space-y-6">
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <div className="flex items-center gap-3 mb-2">
                <AlertTriangle className="w-6 h-6 text-amber-400" />
                <h2 className="text-xl font-serif font-bold text-zinc-100">Red Flags Operativi</h2>
              </div>
              <p className="text-xs text-zinc-500 mb-6">
                Problematiche identificate nel documento. Clicca sul riferimento pagina per verificare nel PDF originale.
              </p>

              {hasGroupedRedFlags ? (
                <div className="space-y-5">
                  {groupedRedFlags.map((group) => (
                    group.items.length > 0 ? (
                      <div key={`rf_group_${group.key}`} className="p-4 rounded-lg border border-zinc-800 bg-zinc-950/60">
                        <h3 className="text-sm font-semibold text-zinc-100 mb-3">{group.title}</h3>
                        <div className="space-y-3">
                          {group.items.map((item) => (
                            <RedFlagMatrixItem key={item.key} item={item} />
                          ))}
                        </div>
                      </div>
                    ) : null
                  ))}
                </div>
              ) : redFlags.length > 0 ? (
                <div className="space-y-4">
                  {redFlags.map((flag, i) => (
                    <RedFlagItem key={i} flag={flag} />
                  ))}
                </div>
              ) : (
                <div className="text-center py-8">
                  <CheckCircle className="w-12 h-12 text-emerald-400 mx-auto mb-4" />
                  <p className="text-zinc-400">Nessun red flag identificato</p>
                </div>
              )}
            </div>
          </TabsContent>
        </Tabs>
        
        {/* Disclaimer Footer */}
        <div className="mt-8 p-4 bg-zinc-900/50 border border-zinc-800 rounded-lg text-center">
          <p className="text-xs text-zinc-500">
            {safeRender(summary.disclaimer_it, 'Documento informativo. Non costituisce consulenza legale. Consultare un professionista qualificato.')}
          </p>
          <p className="text-xs text-zinc-600 mt-1">
            {safeRender(summary.disclaimer_en, 'Informational document. Not legal advice. Consult a qualified professional.')}
          </p>
        </div>
      </main>
    </div>
  );
};

export default AnalysisResult;
