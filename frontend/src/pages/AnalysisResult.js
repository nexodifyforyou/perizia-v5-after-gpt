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

const parseNumericEuro = (value) => {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string') {
    const normalized = value.replace(/[^\d,.-]/g, '').replace(/\./g, '').replace(',', '.');
    const parsed = parseFloat(normalized);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
};

const getShortText = (value, max = 110) => {
  const text = safeRender(value, '').trim();
  if (!text) return '';
  return text.length > max ? `${text.slice(0, max)}...` : text;
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
  const status = safeRender(data?.status, 'Non specificato in perizia');
  const evidence = getEvidence(data);
  const hasEvidence = evidence.length > 0;
  const pages = hasEvidence ? [...new Set(evidence.map(e => e.page).filter(Boolean))] : [];
  
  const getStatusIcon = (status) => {
    const normalizedStatus = status.toUpperCase();
    if (normalizedStatus === 'YES' || normalizedStatus === 'SI') return <XCircle className="w-5 h-5 text-red-400" />;
    if (normalizedStatus === 'NO') return <CheckCircle className="w-5 h-5 text-emerald-400" />;
    return <HelpCircle className="w-5 h-5 text-amber-400" />;
  };

  const getStatusBg = (status) => {
    const normalizedStatus = status.toUpperCase();
    if (normalizedStatus === 'YES' || normalizedStatus === 'SI') return 'bg-red-500/10 border-red-500/30';
    if (normalizedStatus === 'NO') return 'bg-emerald-500/10 border-emerald-500/30';
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

  return (
    <div className={`p-4 rounded-lg border ${getStatusBg(status)}`}>
      <div className="flex items-start gap-3">
        {getStatusIcon(status)}
        <div className="flex-1">
          <div className="flex items-center gap-2">
            <p className="text-sm font-medium text-zinc-100">{displayName}</p>
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

// Multi-Lot Selector Component
const MultiLotSelector = ({ lots, selectedLot, onSelectLot }) => {
  if (!lots || lots.length <= 1) return null;
  
  return (
    <div className="mb-6 p-4 bg-gradient-to-r from-gold/10 to-amber-500/10 rounded-lg border border-gold/30">
      <div className="flex items-center gap-2 mb-3">
        <Home className="w-5 h-5 text-gold" />
        <h3 className="text-lg font-semibold text-zinc-100">Perizia Multi-Lotto ({lots.length} lotti)</h3>
      </div>
      
      {/* Compact Lots Table */}
      <div className="overflow-x-auto mb-4">
        <table className="w-full text-sm">
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
                <td className="py-2 px-3 text-zinc-300">{lot.superficie_mq || 'TBD'}</td>
                <td className="py-2 px-3 text-zinc-300">{(lot.diritto_reale || 'NON SPECIFICATO').substring(0, 20)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      
      {/* Lot Selector Dropdown */}
      <div className="flex items-center gap-3">
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
  const [headlineModal, setHeadlineModal] = useState({ open: false, fieldKey: null });
  const [estrattoShowAll, setEstrattoShowAll] = useState({});

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
        <main className="ml-64 p-8">
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
    diritto_reale: { value: selectedLot.diritto_reale, evidence: selectedLot.evidence?.diritto_reale || [] },
    superficie_catastale: { value: selectedLot.superficie_mq, evidence: selectedLot.evidence?.superficie || [] },
    tipologia: { value: selectedLot.tipologia, evidence: [] }
  } : (section4.prezzo_base_asta ? section4 : (result.dati_certi_del_lotto || {}));
  
  const abusi = section5.conformita_urbanistica ? section5 : (result.abusi_edilizi_conformita || {});
  const occupativo = section6.status ? section6 : (result.stato_occupativo || {});
  const conservativo = section7.condizione_generale ? section7 : (result.stato_conservativo || {});
  const formalita = section8.ipoteche ? section8 : (result.formalita || {});
  const legalKillers = section9.items ? section9 : (result.legal_killers_checklist || {});
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
  const isTotalTBD = moneyBoxTotalMin === 'TBD' || moneyBoxTotalMax === 'TBD';

  // Get legal killers - convert new array format to object for display
  const legalKillersObj = legalKillers.items 
    ? legalKillers.items.reduce((acc, item) => { 
        const key = item.killer || item.key || `item_${Object.keys(acc).length}`;
        acc[key] = item; 
        return acc; 
      }, {}) 
    : (typeof legalKillers === 'object' ? legalKillers : {});

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
    ape_status: safeRender(getFieldValue('ape', abusi.ape?.status || result.ape), 'Non specificato in perizia'),
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
      
      <main className="ml-64 p-8">
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
        <div className="flex items-center justify-between mb-6">
          <Link to="/history" className="inline-flex items-center gap-2 text-zinc-400 hover:text-zinc-100 transition-colors">
            <ArrowLeft className="w-4 h-4" />
            Torna allo storico
          </Link>
          <div className="flex items-center gap-3">
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
              
              <div className="flex gap-3 justify-end">
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
        <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6 mb-8">
          <div className="flex items-start justify-between gap-6">
            <div>
              <h1 className="text-2xl font-serif font-bold text-zinc-100 mb-2">
                {safeRender(analysis.case_title || analysis.file_name, 'Analisi Perizia')}
              </h1>
              <div className="flex items-center gap-2 mb-3">
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
              <div className="flex items-center gap-4 text-sm text-zinc-500">
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
            <div className="text-right space-y-2">
              <Button
                onClick={handleDownloadPDF}
                disabled={downloading}
                data-testid="download-pdf-btn"
                className="bg-gold text-zinc-950 hover:bg-gold-dim"
              >
                <FileDown className="w-4 h-4 mr-2" />
                {downloading ? 'Scaricando...' : 'Scarica Report'}
              </Button>
              <div className="flex justify-end">
                <SemaforoBadge status={semaforoStatus} />
              </div>
              <p className="text-xs text-zinc-400 max-w-xs ml-auto">
                Sintesi operativa: priorità alle verifiche indicate.
              </p>
              <p className="text-[11px] text-zinc-500 max-w-xs ml-auto">
                Operational summary: prioritize the checks listed.
              </p>
            </div>
          </div>
          <p className="text-sm text-zinc-400 mt-3">
            {safeRender(semaforo.status_label || semaforo.reason_it || semaforo.status_it, '')}
          </p>
          <p className="text-xs text-zinc-500 mt-1">
            {safeRender(semaforo.reason_en || semaforo.status_en, '')}
          </p>
          <div className="text-right">
              {/* Show driver/reason for semaforo */}
              {semaforo.driver?.value && (
                <p className="text-xs text-amber-400 mt-1">
                  Driver: {semaforo.driver.value}
                </p>
              )}
              {/* Show evidence pages */}
              {(getEvidence(semaforo.semaforo_complessivo || semaforo).length > 0) && (
                <p className="text-xs text-gold mt-1 flex items-center justify-end gap-1">
                  <FileText className="w-3 h-3" />
                  Basato su pag. {[...new Set(getEvidence(semaforo.semaforo_complessivo || semaforo).map(e => e.page))].join(', ')}
                </p>
              )}
            </div>
          
          {/* Quick Decision with Evidence */}
          <div className="mt-6 p-4 bg-zinc-950 rounded-lg border border-zinc-800">
            <div className="flex items-center justify-between gap-3 mb-2">
              <p className="text-xs font-mono uppercase text-zinc-500">Decisione Rapida</p>
              <span className="text-[10px] px-2 py-1 rounded border border-zinc-700 text-zinc-400 uppercase tracking-wide">
                {decisionSourceLabel}
              </span>
            </div>
            <p className="text-lg font-semibold text-zinc-100">{decisionIt}</p>
            <p className="text-sm text-zinc-500 mt-1">{decisionEn}</p>
            {decisionBulletsIt.length > 0 && (
              <ul className="mt-3 space-y-1 text-sm text-zinc-300 list-disc pl-5">
                {decisionBulletsIt.slice(0, 5).map((item, idx) => (
                  <li key={`it-bullet-${idx}`}>{safeRender(item, '')}</li>
                ))}
              </ul>
            )}
            {decisionBulletsEn.length > 0 && (
              <ul className="mt-2 space-y-1 text-xs text-zinc-500 list-disc pl-5">
                {decisionBulletsEn.slice(0, 5).map((item, idx) => (
                  <li key={`en-bullet-${idx}`}>{safeRender(item, '')}</li>
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
            {decision.driver_rosso && decision.driver_rosso.length > 0 && (
              <div className="mt-4 space-y-2">
                <p className="text-xs font-mono text-red-400 uppercase">Criticità Rilevate:</p>
                {decision.driver_rosso.map((driver, idx) => (
                  <div key={idx} className="p-2 bg-red-500/10 rounded border border-red-500/20">
                    <div className="flex items-center gap-2">
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
        </div>
        
        {/* Tabs */}
        <Tabs defaultValue="overview" className="w-full">
          <TabsList className="w-full justify-start bg-zinc-900 border border-zinc-800 p-1 mb-6 overflow-x-auto">
            <TabsTrigger value="overview" data-testid="tab-overview">Panoramica</TabsTrigger>
            <TabsTrigger value="costs" data-testid="tab-costs">Costi</TabsTrigger>
            <TabsTrigger value="legal" data-testid="tab-legal">Legal Killers</TabsTrigger>
            <TabsTrigger value="details" data-testid="tab-details">Dettagli</TabsTrigger>
            <TabsTrigger value="flags" data-testid="tab-flags">Red Flags</TabsTrigger>
          </TabsList>
          
          {/* Overview Tab */}
          <TabsContent value="overview" className="space-y-6">
            {/* Summary for Client - Section 12 Style */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6 relative">
              {/* QA Badge - Small Corner */}
              <div className={`absolute top-4 right-4 px-2 py-1 rounded text-xs font-mono flex items-center gap-1 ${
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
                <div className="space-y-3 mb-4">
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
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Case Summary</h2>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                <DataValueWithEvidence
                  label="Tribunale"
                  value={formatFieldStateDisplay('tribunale', caseHeader.tribunale?.value || caseHeader.tribunale)}
                  evidence={getFieldEvidence('tribunale', caseHeader.tribunale)}
                />
                <DataValueWithEvidence
                  label="Procedura"
                  value={formatFieldStateDisplay('procedura', caseHeader.procedure?.value || caseHeader.procedure_id)}
                  evidence={getFieldEvidence('procedura', caseHeader.procedure || caseHeader.procedure_id)}
                />
                <DataValueWithEvidence
                  label="Lotto"
                  value={formatFieldStateDisplay('lotto', caseHeader.lotto?.value || caseHeader.lotto)}
                  evidence={getFieldEvidence('lotto', caseHeader.lotto)}
                />
                <DataValueWithEvidence
                  label="Stato Occupativo"
                  value={formatFieldStateDisplay('stato_occupativo', occupativo.status_it || occupativo.status)}
                  evidence={getFieldEvidence('stato_occupativo', occupativo)}
                />
                <DataValueWithEvidence
                  label="Spese Condominiali Arretrate"
                  value={formatFieldStateDisplay('spese_condominiali_arretrate', result.spese_condominiali_arretrate || result.spese_condominiali)}
                  evidence={getFieldEvidence('spese_condominiali_arretrate', result.spese_condominiali_arretrate || result.spese_condominiali)}
                />
                <DataValueWithEvidence
                  label="APE"
                  value={safeRender(getFieldValue('ape', abusi.ape?.status || result.ape), 'Non specificato in perizia')}
                  evidence={getFieldEvidence('ape', abusi.ape || result.ape)}
                />
                {datiAsta && (
                  <DataValueWithEvidence
                    label="Dati Asta"
                    value={safeRender(datiAsta?.data || datiAsta?.value || datiAsta)}
                    evidence={getEvidence(datiAsta)}
                  />
                )}
              </div>
            </div>

            {/* Riepilogo Costi / Cost Summary */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <h2 className="text-lg font-serif font-bold text-zinc-100 mb-3">Riepilogo Costi / Cost Summary</h2>
              {moneyBoxTotalRange && typeof moneyBoxTotalRange.min_eur === 'number' && typeof moneyBoxTotalRange.max_eur === 'number' ? (
                <>
                  <p className="text-base font-semibold text-zinc-100">
                    Costi extra stimati: € {moneyBoxTotalRange.min_eur.toLocaleString()} - € {moneyBoxTotalRange.max_eur.toLocaleString()}
                  </p>
                  <p className="text-sm text-zinc-400 mt-1">
                    Estimated extra costs: € {moneyBoxTotalRange.min_eur.toLocaleString()} - € {moneyBoxTotalRange.max_eur.toLocaleString()}
                  </p>
                  {moneyBoxTotalRange.includes_market_estimates && (
                    <>
                      <p className="text-xs text-zinc-500 mt-2">Include stime Nexodify per voci non presenti in perizia.</p>
                      <p className="text-[11px] text-zinc-600">Includes Nexodify estimates for items not present in the appraisal.</p>
                    </>
                  )}
                </>
              ) : (
                <>
                  <p className="text-sm text-zinc-400">Costi extra: non disponibili</p>
                  <p className="text-xs text-zinc-500 mt-1">Extra costs: unavailable</p>
                </>
              )}
            </div>
            
            {/* Key Data Grid with Evidence */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              <DataValueWithEvidence 
                label="Prezzo Base" 
                value={dati.prezzo_base_asta?.formatted || dati.prezzo_base_asta?.value || dati.prezzo_base_asta}
                evidence={getEvidence(dati.prezzo_base_asta)}
                valueClassName="text-gold text-xl"
              />
              <DataValueWithEvidence 
                label="Superficie" 
                value={dati.superficie_catastale?.value || dati.superficie_catastale}
                evidence={getEvidence(dati.superficie_catastale)}
              />
              <DataValueWithEvidence 
                label="Stato Occupativo" 
                value={occupativo.status_it || occupativo.status}
                evidence={getEvidence(occupativo)}
              />
              <DataValueWithEvidence 
                label="Conformità Urbanistica" 
                value={abusi.conformita_urbanistica?.status}
                evidence={getEvidence(abusi.conformita_urbanistica)}
              />
              <DataValueWithEvidence 
                label="Conformità Catastale" 
                value={abusi.conformita_catastale?.status}
                evidence={getEvidence(abusi.conformita_catastale)}
              />
              <DataValueWithEvidence 
                label="Agibilità/Abitabilità" 
                value={abusi.agibilita?.status || 'Non specificato'}
                evidence={getEvidence(abusi.agibilita)}
              />
              <DataValueWithEvidence 
                label="APE (Certificato Energetico)" 
                value={abusi.ape?.status || 'Non specificato'}
                evidence={getEvidence(abusi.ape)}
              />
              <DataValueWithEvidence 
                label="Diritto Reale" 
                value={dati.diritto_reale?.value || dati.diritto_reale}
                evidence={getEvidence(dati.diritto_reale)}
              />
            </div>
            
            {/* Impianti Section */}
            {abusi.impianti && (
              <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
                <h3 className="text-sm font-semibold text-zinc-100 mb-3">Conformità Impianti</h3>
                <div className="grid grid-cols-3 gap-4">
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
            
            {/* Indice di Convenienza */}
            {(indice.all_in_light_min || indice.all_in_light_max || indice.prezzo_base) && (
              <div className="bg-gold/10 border border-gold/30 rounded-xl p-6">
                <h3 className="text-lg font-semibold text-zinc-100 mb-2">Indice di Convenienza (All-In Light)</h3>
                <p className="text-zinc-300 mb-4 text-sm">{safeRender(indice.lettura_secca_it || indice.dry_read_it, 'Calcolo basato su prezzo base + extra budget stimato')}</p>
                <div className="grid grid-cols-3 gap-4">
                  <div className="text-center p-3 bg-zinc-950 rounded-lg">
                    <p className="text-xs text-zinc-500 mb-1">PREZZO BASE</p>
                    <p className="text-lg font-mono font-bold text-zinc-300">€{(indice.prezzo_base || dati.prezzo_base_asta?.value || 0).toLocaleString()}</p>
                  </div>
                  <div className="text-center p-3 bg-zinc-950 rounded-lg">
                    <p className="text-xs text-zinc-500 mb-1">ALL-IN MIN</p>
                    <p className="text-lg font-mono font-bold text-gold">€{(indice.all_in_light_min || 0).toLocaleString()}</p>
                  </div>
                  <div className="text-center p-3 bg-zinc-950 rounded-lg">
                    <p className="text-xs text-zinc-500 mb-1">ALL-IN MAX</p>
                    <p className="text-lg font-mono font-bold text-gold">€{(indice.all_in_light_max || 0).toLocaleString()}</p>
                  </div>
                </div>
                {(indice.extra_budget_min || indice.extra_budget_max) && (
                  <p className="text-xs text-zinc-500 mt-3 text-center">
                    Extra budget: €{(indice.extra_budget_min || 0).toLocaleString()} - €{(indice.extra_budget_max || 0).toLocaleString()}
                  </p>
                )}
              </div>
            )}
          </TabsContent>
          
          {/* Costs Tab */}
          <TabsContent value="costs" className="space-y-6">
            <div className="money-box-card p-6">
              <div className="flex items-center gap-3 mb-2">
                <DollarSign className="w-6 h-6 text-gold" />
                <h2 className="text-xl font-serif font-bold text-zinc-100">Portafoglio Costi (Money Box)</h2>
              </div>
              <p className="text-xs text-zinc-500 mb-6">
                <span className="text-emerald-400">Verde</span> = dal documento | 
                <span className="text-gold ml-2">Oro</span> = stima Nexodify
              </p>
              {moneyBoxTotalRange && typeof moneyBoxTotalRange.min_eur === 'number' && typeof moneyBoxTotalRange.max_eur === 'number' && (
                <div className="mb-5 p-4 bg-zinc-950 rounded-lg border border-gold/30">
                  <p className="text-lg font-semibold text-zinc-100">
                    Totale costi extra stimati: €{moneyBoxTotalRange.min_eur.toLocaleString()} - €{moneyBoxTotalRange.max_eur.toLocaleString()}
                  </p>
                  {moneyBoxTotalRange.includes_market_estimates ? (
                    <p className="text-xs text-zinc-500 mt-1">Include stime di mercato per voci non presenti in perizia.</p>
                  ) : (
                    <p className="text-xs text-zinc-500 mt-1">Calcolato solo su importi rilevati dalla perizia.</p>
                  )}
                </div>
              )}
              
              {moneyBoxItems.length > 0 ? (
                <div className="space-y-3">
                  {moneyBoxItems.map((item, index) => (
                    <MoneyBoxItem key={index} item={item} />
                  ))}
                </div>
              ) : (
                <p className="text-zinc-500 text-center py-8">Nessun dato sui costi disponibile</p>
              )}
              
              {/* Total - support TBD and numeric totals */}
              {(moneyBoxTotal || moneyBox.total_extra_costs) && !moneyBoxTotalRange && (
                <div className="mt-6 p-4 bg-gold/10 border border-gold/30 rounded-lg">
                  <div className="flex items-center justify-between">
                    <span className="text-lg font-semibold text-zinc-100">Totale Costi Extra Stimati</span>
                    <span className={`text-2xl font-mono font-bold ${isTotalTBD ? 'text-amber-400' : 'text-gold'}`}>
                      {isTotalTBD ? (
                        'Non disponibile'
                      ) : moneyBoxTotal?.min !== undefined ? (
                        `€${(typeof moneyBoxTotal.min === 'number' ? moneyBoxTotal.min : 0).toLocaleString()} - €${(typeof moneyBoxTotal.max === 'number' ? moneyBoxTotal.max : 0).toLocaleString()}`
                      ) : (
                        `€${(moneyBox.total_extra_costs?.range?.min || 0).toLocaleString()} - €${(moneyBox.total_extra_costs?.range?.max || 0).toLocaleString()}`
                      )}
                      {!isTotalTBD && (moneyBoxTotal?.nota?.includes('+') || moneyBox.total_extra_costs?.max_is_open) && '+'}
                    </span>
                  </div>
                  {moneyBoxTotal?.nota && (
                    <p className="text-xs text-zinc-400 mt-2">{moneyBoxTotal.nota}</p>
                  )}
                  {isTotalTBD && (
                    <p className="text-xs text-amber-400 mt-2">
                      ⚠️ Costi non quantificati in perizia — Verifica tecnico/legale obbligatoria
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
                <p className="text-zinc-500 text-center py-8">Checklist legal killers non disponibile</p>
              )}

              {estrattoLegalSection && Array.isArray(estrattoLegalSection.__items) && estrattoLegalSection.__items.length > 0 && (
                <div className="mt-6 p-4 rounded-lg border border-zinc-800 bg-zinc-950/60">
                  <h3 className="text-sm font-semibold text-zinc-100">Dettagli dal documento / Document details</h3>
                  <div className="space-y-3 mt-3">
                    {(!!estrattoShowAll.legal_doc ? estrattoLegalSection.__items : estrattoLegalSection.__items.slice(0, 12)).map((item, idx) => {
                      const labelIt = safeRender(item?.label_it || item?.__label, 'Voce');
                      const labelEn = safeRender(item?.label_en, '');
                      const displayValue = getEstrattoItemDisplayValue(item);
                      const evidence = Array.isArray(item?.__evidence) ? item.__evidence : [];
                      return (
                        <div key={`legal_doc_${idx}`} className="p-3 rounded border border-zinc-800 bg-zinc-900">
                          <p className="text-sm text-zinc-100">
                            <span className="font-medium">{labelIt}</span>
                            {displayValue ? `: ${displayValue}` : ''}
                          </p>
                          {labelEn && <p className="text-xs text-zinc-500 mt-1">{labelEn}</p>}
                          {evidence.length > 0 && (
                            <div className="mt-2">
                              <EvidenceDetail evidence={evidence} />
                            </div>
                          )}
                        </div>
                      );
                    })}
                    {estrattoLegalSection.__items.length > 12 && (
                      <button
                        type="button"
                        onClick={() => setEstrattoShowAll((prev) => ({ ...prev, legal_doc: !prev.legal_doc }))}
                        className="text-xs text-gold hover:underline"
                      >
                        {estrattoShowAll.legal_doc ? 'Mostra meno / Show less' : `Mostra altri / Show more (${estrattoLegalSection.__items.length - 12})`}
                      </button>
                    )}
                  </div>
                </div>
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

            {/* Beni Section */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Beni del Lotto</h2>
              {Array.isArray(beni) && beni.length > 0 ? (
                <div className="space-y-3">
                  {beni.map((bene, idx) => (
                    <div key={bene.bene_number || idx} className="p-4 bg-zinc-950 rounded-lg border border-zinc-800">
                      <div className="flex items-start justify-between gap-4">
                        <div className="space-y-1">
                          <p className="text-sm font-semibold text-zinc-100">
                            {bene.bene_number ? `Bene ${bene.bene_number}` : `Bene ${idx + 1}`} - {safeRender(bene.tipologia, 'Tipologia non specificata')}
                          </p>
                          {bene.note && (
                            <p className="text-xs text-zinc-500">{safeRender(bene.note, '')}</p>
                          )}
                          {bene.catasto && (
                            <p className="text-xs text-zinc-400">
                              Catasto: {safeRender(bene.catasto)}
                            </p>
                          )}
                        </div>
                        {getEvidence(bene).length > 0 && (
                          <EvidenceBadge evidence={getEvidence(bene)} />
                        )}
                      </div>
                      {getEvidence(bene).length > 0 && (
                        <EvidenceDetail evidence={getEvidence(bene)} />
                      )}
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-zinc-500">Beni non disponibili nell'estrazione corrente.</p>
              )}
            </div>

            {/* Estratto sections mapped to Dettagli */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Dettagli dal documento</h2>
              <div className="space-y-4">
                {estrattoSectionDefs.map((def) => {
                  const section = estrattoSectionMap[def.key];
                  const sectionItems = Array.isArray(section?.__items) ? section.__items : [];
                  const showAll = !!estrattoShowAll[`details_${def.key}`];
                  const visibleItems = showAll ? sectionItems : sectionItems.slice(0, 10);
                  const headingIt = safeRender(section?.heading_it || section?.__title, def.headingIt);
                  const headingEn = safeRender(section?.heading_en, def.headingEn);

                  return (
                    <div key={`details_section_${def.key}`} className="p-4 rounded-lg border border-zinc-800 bg-zinc-950/60">
                      <h3 className="text-sm font-semibold text-zinc-100">{headingIt}</h3>
                      <p className="text-xs text-zinc-500 mb-3">{headingEn}</p>
                      {visibleItems.length > 0 ? (
                        <div className="space-y-3">
                          {visibleItems.map((item, idx) => {
                            const labelIt = safeRender(item?.label_it || item?.__label, 'Voce');
                            const labelEn = safeRender(item?.label_en, '');
                            const displayValue = getEstrattoItemDisplayValue(item);
                            const evidence = Array.isArray(item?.__evidence) ? item.__evidence : [];
                            return (
                              <div key={`details_${def.key}_${idx}`} className="p-3 rounded border border-zinc-800 bg-zinc-900">
                                <p className="text-sm text-zinc-100">
                                  <span className="font-medium">{labelIt}</span>
                                  {displayValue ? `: ${displayValue}` : ''}
                                </p>
                                {labelEn && <p className="text-xs text-zinc-500 mt-1">{labelEn}</p>}
                                {evidence.length > 0 && (
                                  <div className="mt-2">
                                    <EvidenceDetail evidence={evidence} />
                                  </div>
                                )}
                              </div>
                            );
                          })}
                          {sectionItems.length > 10 && (
                            <button
                              type="button"
                              onClick={() => setEstrattoShowAll((prev) => ({ ...prev, [`details_${def.key}`]: !showAll }))}
                              className="text-xs text-gold hover:underline"
                            >
                              {showAll ? 'Mostra meno / Show less' : `Mostra altri / Show more (${sectionItems.length - 10})`}
                            </button>
                          )}
                        </div>
                      ) : (
                        <p className="text-xs text-zinc-500">Nessuna voce estratta per questa sezione.</p>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
            
            {/* Abusi Edilizi / Conformità - Section 5 */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Abusi Edilizi / Conformità</h2>
              {(userMessagesByCode.ACTION_VERIFY_CATASTO || []).map((msg, idx) => (
                <MessageInjectionCard key={`catasto_msg_${idx}`} msg={msg} />
              ))}
              {(userMessagesByCode.RISK_NON_AGIBILE || []).map((msg, idx) => (
                <MessageInjectionCard key={`non_agibile_msg_${idx}`} msg={msg} />
              ))}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <DataValueWithEvidence 
                  label="Conformità Urbanistica" 
                  value={abusi.conformita_urbanistica?.status}
                  evidence={getEvidence(abusi.conformita_urbanistica)}
                />
                {abusi.conformita_urbanistica?.detail && (
                  <div className="p-3 bg-zinc-950 rounded-lg md:col-span-2">
                    <p className="text-xs text-zinc-500 mb-1">Dettaglio Conformità</p>
                    <p className="text-sm text-zinc-300">{abusi.conformita_urbanistica.detail}</p>
                  </div>
                )}
                <DataValueWithEvidence 
                  label="Conformità Catastale" 
                  value={abusi.conformita_catastale?.status}
                  evidence={getEvidence(abusi.conformita_catastale)}
                />
                <DataValueWithEvidence 
                  label="Condono Presente" 
                  value={safeRender(abusi.condono?.presente || abusi.condono?.present, 'Non specificato in perizia')}
                  evidence={getEvidence(abusi.condono)}
                />
                {(abusi.condono?.anno || abusi.condono?.pratica) && (
                  <DataValueWithEvidence 
                    label="Pratica Condono" 
                    value={`${abusi.condono.pratica || ''} (${abusi.condono.anno || ''}) - ${abusi.condono.stato || ''}`}
                    evidence={getEvidence(abusi.condono)}
                  />
                )}
                <DataValueWithEvidence 
                  label="Agibilità/Abitabilità" 
                  value={safeRender(abusi.agibilita?.status || abusi.agibilita, 'Non specificato in perizia')}
                  evidence={getEvidence(abusi.agibilita)}
                />
                <DataValueWithEvidence 
                  label="APE (Certificato Energetico)" 
                  value={abusi.ape?.status || abusi.ape}
                  evidence={getEvidence(abusi.ape)}
                />
              </div>
              
              {/* Impianti Section */}
              {abusi.impianti && (
                <div className="mt-4 p-4 bg-zinc-950 rounded-lg">
                  <p className="text-xs text-zinc-500 mb-2">Conformità Impianti</p>
                  <div className="grid grid-cols-3 gap-4">
                    <div className="text-center">
                      <p className="text-xs text-zinc-400">Elettrico</p>
                      <p className={`text-sm font-medium ${
                        abusi.impianti.elettrico?.conformita === 'SI' ? 'text-emerald-400' : 
                        abusi.impianti.elettrico?.conformita === 'NO' ? 'text-red-400' : 'text-amber-400'
                      }`}>{abusi.impianti.elettrico?.conformita || 'NON RISULTA'}</p>
                    </div>
                    <div className="text-center">
                      <p className="text-xs text-zinc-400">Termico</p>
                      <p className={`text-sm font-medium ${
                        abusi.impianti.termico?.conformita === 'SI' ? 'text-emerald-400' : 
                        abusi.impianti.termico?.conformita === 'NO' ? 'text-red-400' : 'text-amber-400'
                      }`}>{abusi.impianti.termico?.conformita || 'NON RISULTA'}</p>
                    </div>
                    <div className="text-center">
                      <p className="text-xs text-zinc-400">Idrico</p>
                      <p className={`text-sm font-medium ${
                        abusi.impianti.idrico?.conformita === 'SI' ? 'text-emerald-400' : 
                        abusi.impianti.idrico?.conformita === 'NO' ? 'text-red-400' : 'text-amber-400'
                      }`}>{abusi.impianti.idrico?.conformita || 'NON RISULTA'}</p>
                    </div>
                  </div>
                </div>
              )}
            </div>
            
            {/* Stato Conservativo - Section 7 */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              {(userMessagesByCode.INFO_IMPIANTI_PRESENT || []).map((msg, idx) => (
                <MessageInjectionCard key={`impianti_msg_${idx}`} msg={msg} />
              ))}
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Stato Conservativo / Impianti</h2>
              <p className="text-zinc-300">{safeRender(conservativo.condizione_generale || conservativo.general_condition_it, 'Nessuna nota disponibile')}</p>

              {estrattoSectionMap.impianti && Array.isArray(estrattoSectionMap.impianti.__items) && estrattoSectionMap.impianti.__items.length > 0 ? (
                <div className="mt-4 p-4 bg-zinc-950 rounded-lg border border-zinc-800">
                  <p className="text-xs text-zinc-500 mb-3">Impianti estratti dal documento</p>
                  <ul className="space-y-3">
                    {estrattoSectionMap.impianti.__items.map((item, idx) => (
                      <li key={`estratto_impianti_${idx}`} className="text-sm text-zinc-300">
                        <span className="text-zinc-100">• {safeRender(item.__label, 'Impianti')}</span>: {safeRender(item.__value, 'Non specificato in perizia')}
                        {Array.isArray(item.__evidence) && item.__evidence.length > 0 && (
                          <div className="mt-1">
                            <EvidenceDetail evidence={item.__evidence} />
                          </div>
                        )}
                      </li>
                    ))}
                  </ul>
                </div>
              ) : (
                <MissingStateRationale fieldKey="impianti" forceMissing={true} />
              )}
              
              {/* Carenze */}
              {conservativo.carenze && (
                <div className="mt-3 p-3 bg-amber-500/10 rounded border border-amber-500/30">
                  <p className="text-xs text-amber-400 mb-1">Carenze riscontrate:</p>
                  <p className="text-sm text-zinc-300">{conservativo.carenze}</p>
                </div>
              )}
              
              {/* Dettagli array */}
              {Array.isArray(conservativo.dettagli) && conservativo.dettagli.length > 0 && (
                <div className="mt-4 space-y-2">
                  {conservativo.dettagli.map((det, i) => (
                    <div key={i} className="flex items-start gap-2 p-2 bg-zinc-950 rounded">
                      <Home className="w-4 h-4 text-zinc-500 mt-0.5" />
                      <div>
                        <span className="text-zinc-300 text-sm">{det.area}: {det.stato}</span>
                        {getEvidence(det).length > 0 && (
                          <span className="text-xs font-mono text-gold ml-2">
                            p. {[...new Set(getEvidence(det).map(e => e.page))].join(', ')}
                          </span>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              )}
              
              {/* Old format issues_found */}
              {Array.isArray(conservativo.issues_found) && conservativo.issues_found.length > 0 && (
                <div className="mt-4 space-y-2">
                  {conservativo.issues_found.map((issue, i) => (
                    <div key={i} className="flex items-start gap-2 p-2 bg-amber-500/10 rounded">
                      <AlertTriangle className="w-4 h-4 text-amber-400 mt-0.5" />
                      <div>
                        <span className="text-amber-400 text-sm">{safeRender(issue.issue_it || issue)}</span>
                        {getEvidence(issue).length > 0 && (
                          <span className="text-xs font-mono text-gold ml-2">
                            p. {[...new Set(getEvidence(issue).map(e => e.page))].join(', ')}
                          </span>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
            
            {/* Formalità - Section 8 */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Formalità</h2>
              
              {/* Ipoteche */}
              {Array.isArray(formalita.ipoteche) && formalita.ipoteche.length > 0 ? (
                <div className="mb-4">
                  <p className="text-xs text-zinc-500 mb-2">Ipoteche registrate:</p>
                  <div className="space-y-2">
                    {formalita.ipoteche.map((ip, i) => (
                      <div key={i} className="p-3 bg-zinc-950 rounded flex items-center justify-between">
                        <div>
                          <span className="text-zinc-300 text-sm">{ip.tipo || 'Ipoteca'}</span>
                          {ip.data && <span className="text-xs text-zinc-500 ml-2">({ip.data})</span>}
                        </div>
                        <span className="font-mono text-gold">€{(ip.importo || 0).toLocaleString()}</span>
                      </div>
                    ))}
                  </div>
                </div>
              ) : (
                <DataValueWithEvidence 
                  label="Ipoteca" 
                  value={safeRender(formalita.ipoteca?.status, 'Non specificato in perizia')}
                  evidence={getEvidence(formalita.ipoteca)}
                />
              )}
              
              {/* Pignoramenti */}
              {Array.isArray(formalita.pignoramenti) && formalita.pignoramenti.length > 0 && (
                <div className="mb-4">
                  <p className="text-xs text-zinc-500 mb-2">Pignoramenti:</p>
                  <div className="space-y-2">
                    {formalita.pignoramenti.map((pig, i) => (
                      <div key={i} className="p-3 bg-zinc-950 rounded">
                        <span className="text-zinc-300 text-sm">Pignoramento {pig.data || ''}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
              
              {/* Cancellazione */}
              {formalita.cancellazione && (
                <div className="p-3 bg-emerald-500/10 rounded border border-emerald-500/30">
                  <p className="text-xs text-emerald-400 mb-1">Cancellazione:</p>
                  <p className="text-sm text-zinc-300">{formalita.cancellazione}</p>
                </div>
              )}
              
              {/* Evidence */}
              {getEvidence(formalita).length > 0 && (
                <p className="text-xs text-gold mt-2 flex items-center gap-1">
                  <FileText className="w-3 h-3" />
                  Riferimento: p. {[...new Set(getEvidence(formalita).map(e => e.page))].join(', ')}
                </p>
              )}
            </div>
            
            {/* Checklist Pre-Offerta */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Dati Asta</h2>
              {(userMessagesByCode.ACTION_VERIFY_DATO_ASTA || []).map((msg, idx) => (
                <MessageInjectionCard key={`dati_asta_msg_${idx}`} msg={msg} />
              ))}
              <div className="mb-4">
                <DataValueWithEvidence
                  label="Dati Asta"
                  value={safeRender(datiAsta?.data || datiAsta?.value || datiAsta, 'Non specificato in perizia')}
                  evidence={getEvidence(datiAsta)}
                />
                <MissingStateRationale fieldKey="dati_asta" forceMissing={!datiAsta} />
              </div>
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Checklist Pre-Offerta</h2>
              <p className="text-xs text-zinc-500 mb-1">Legenda: P0 = obbligatorio prima dell’offerta; P1 = consigliato.</p>
              <p className="text-[11px] text-zinc-600 mb-3">Legend: P0 = must-do before bidding; P1 = recommended.</p>
              {checklist.length > 0 ? (
                <div className="space-y-2">
                  {checklist.map((item, i) => {
                    // Handle both string array (new format) and object array (old format)
                    const itemText = typeof item === 'string' ? item : (item.item_it || item.task_it || item.text || JSON.stringify(item));
                    const priority = typeof item === 'object' ? item.priority : null;
                    const status = typeof item === 'object' ? item.status : null;
                    const priorityLabel = priority === 'P0' ? 'Obbligatorio' : priority === 'P1' ? 'Consigliato' : priority;
                    const priorityTooltip = priority === 'P0'
                      ? 'Obbligatorio prima dell’offerta'
                      : priority === 'P1'
                        ? 'Consigliato prima/dopo l’offerta'
                        : '';
                    
                    return (
                      <div key={i} className="flex items-center gap-3 p-3 bg-zinc-950 rounded-lg">
                        {status === 'DONE' ? (
                          <CheckCircle className="w-5 h-5 text-emerald-400" />
                        ) : (
                          <div className="w-5 h-5 rounded-full border-2 border-gold/50 flex items-center justify-center">
                            <span className="text-xs text-gold font-bold">{i + 1}</span>
                          </div>
                        )}
                        <span className="text-zinc-300 text-sm flex-1">{itemText}</span>
                        {priority && (
                          <span
                            title={priorityTooltip}
                            className={`text-xs px-2 py-1 rounded ${
                            priority === 'P0' ? 'bg-red-500/20 text-red-400' :
                            priority === 'P1' ? 'bg-amber-500/20 text-amber-400' :
                            'bg-zinc-700 text-zinc-400'
                            }`}
                          >
                            {priorityLabel}
                          </span>
                        )}
                      </div>
                    );
                  })}
                </div>
              ) : (
                <p className="text-zinc-500">Nessuna checklist disponibile</p>
              )}
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
              
              {redFlags.length > 0 ? (
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
