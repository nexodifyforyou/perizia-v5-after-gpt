import React, { useState, useEffect } from 'react';
import { useParams, Link, useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { Sidebar, SemaforoBadge } from './Dashboard';
import { Button } from '../components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../components/ui/tabs';
import { EvidenceBadge, EvidenceDetail, DataValueWithEvidence } from '../components/EvidenceDisplay';
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

const API_URL = process.env.REACT_APP_BACKEND_URL;

// Helper function to safely render any value
const safeRender = (value, fallback = 'N/A') => {
  if (value === null || value === undefined) return fallback;
  if (typeof value === 'string') return value || fallback;
  if (typeof value === 'number') return value.toString();
  if (typeof value === 'boolean') return value ? 'Sì' : 'No';
  if (Array.isArray(value)) {
    if (value.length === 0) return fallback;
    return value.map(v => safeRender(v, '')).filter(Boolean).join(', ') || fallback;
  }
  if (typeof value === 'object') {
    if (value.value !== undefined) return safeRender(value.value, fallback);
    if (value.status) return safeRender(value.status, fallback);
    if (value.formatted) return safeRender(value.formatted, fallback);
    if (value.label_it) return safeRender(value.label_it, fallback);
    if (value.full) return safeRender(value.full, fallback);
    return fallback;
  }
  return String(value) || fallback;
};

// Get evidence from an object that might have it
const getEvidence = (obj) => {
  if (!obj) return [];
  if (Array.isArray(obj.evidence)) return obj.evidence;
  if (Array.isArray(obj)) return obj.filter(e => e.page || e.quote);
  return [];
};

// Money Box Item Component with Evidence - supports both old and ROMA STANDARD formats
const MoneyBoxItem = ({ item }) => {
  // Get evidence from either new format (fonte_perizia.evidence) or old format (item.evidence)
  const evidence = getEvidence(item.fonte_perizia || item);
  const hasEvidence = evidence.length > 0;
  const pages = hasEvidence ? [...new Set(evidence.map(e => e.page).filter(Boolean))] : [];

  // Format value - handle new format (stima_euro) and old format (value/range)
  const formatValue = () => {
    // New ROMA STANDARD format
    if (item.stima_euro !== undefined && item.stima_euro !== null) {
      const val = typeof item.stima_euro === 'number' ? item.stima_euro : parseFloat(item.stima_euro);
      return isNaN(val) || val === 0 ? item.stima_nota || 'Da verificare' : `€${val.toLocaleString()}`;
    }
    // Old format
    const type = safeRender(item.type, 'UNKNOWN');
    if ((type === 'NEXODIFY_ESTIMATE' || type === 'RANGE') && item.range) {
      return `€${(item.range.min || 0).toLocaleString()} - €${(item.range.max || 0).toLocaleString()}`;
    }
    if (item.value !== undefined && item.value !== null && item.value !== 0) {
      const val = typeof item.value === 'number' ? item.value : parseFloat(item.value);
      return isNaN(val) ? safeRender(item.value) : `€${val.toLocaleString()}`;
    }
    return type;
  };
  
  // Get label - new format uses 'voce', old format uses 'label_it'
  const label = item.voce || item.label_it || item.label || 'Item';
  const code = item.code || label.charAt(0);
  const source = item.fonte_perizia?.value || item.source || '';

  return (
    <div className="p-4 bg-zinc-950/50 rounded-lg border border-zinc-800">
      <div className="flex items-start justify-between mb-2">
        <div className="flex-1">
          <div className="flex items-center gap-2 mb-1">
            <span className="font-mono text-xs text-gold">{code}</span>
            <span className="text-sm font-medium text-zinc-100">{label}</span>
            {hasEvidence && (
              <span className="text-xs font-mono text-gold flex items-center gap-1">
                <FileText className="w-3 h-3" />
                p. {pages.join(', ')}
              </span>
            )}
          </div>
          {source && <p className="text-xs text-zinc-500">{source}</p>}
        </div>
        <div className="text-right">
          <span className={`font-mono text-sm font-bold ${
            source.toLowerCase().includes('perizia') ? 'text-emerald-400' : 
            item.type === 'NEXODIFY_ESTIMATE' || item.stima_nota ? 'text-gold' : 'text-zinc-400'
          }`}>
            {formatValue()}
          </span>
        </div>
      </div>
      {item.stima_nota && item.stima_euro > 0 && (
        <p className="text-xs text-amber-400 mt-1">{item.stima_nota}</p>
      )}
      {item.action_required_it && (
        <p className="text-xs text-amber-400 mt-1">{safeRender(item.action_required_it)}</p>
      )}
      {hasEvidence && evidence[0]?.quote && (
        <div className="mt-2 p-2 bg-zinc-900 rounded border-l-2 border-gold/30">
          <p className="text-xs text-zinc-400 italic">"{evidence[0].quote}"</p>
        </div>
      )}
    </div>
  );
};

// Legal Killer Item Component with Evidence - supports both old and ROMA STANDARD formats
const LegalKillerItem = ({ name, data }) => {
  const status = safeRender(data?.status, 'NOT_SPECIFIED');
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

const AnalysisResult = () => {
  const { analysisId } = useParams();
  const navigate = useNavigate();
  const { user, logout } = useAuth();
  const [analysis, setAnalysis] = useState(null);
  const [loading, setLoading] = useState(true);
  const [downloading, setDownloading] = useState(false);
  const [showDeleteModal, setShowDeleteModal] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);

  const fetchAnalysis = async () => {
    try {
      const response = await axios.get(`${API_URL}/api/history/perizia/${analysisId}`, {
        withCredentials: true
      });
      setAnalysis(response.data);
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

  const handleDownloadPDF = async () => {
    setDownloading(true);
    try {
      const response = await axios.get(`${API_URL}/api/analysis/perizia/${analysisId}/pdf`, {
        withCredentials: true,
        responseType: 'blob'
      });
      
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', `nexodify_report_${analysisId}.html`);
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
      
      toast.success('Report scaricato!');
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
  
  // Map to display variables - prioritize NEW format, fallback to OLD
  const caseHeader = reportHeader.procedure ? reportHeader : (result.case_header || {});
  const semaforo = section1.status ? section1 : (result.semaforo_generale || {});
  const decision = section2.summary_it ? section2 : (result.decision_rapida_client || {});
  const moneyBox = section3.items ? section3 : (result.money_box || {});
  const dati = section4.prezzo_base_asta ? section4 : (result.dati_certi_del_lotto || {});
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

  // Get money box items - support both old and new format
  const moneyBoxItems = Array.isArray(moneyBox.items) ? moneyBox.items : [];
  
  // Get money box total - support both old and new format
  const moneyBoxTotal = moneyBox.totale_extra_budget || moneyBox.total_extra_costs;

  // Get legal killers - convert new array format to object for display
  const legalKillersObj = legalKillers.items 
    ? legalKillers.items.reduce((acc, item) => { 
        const key = item.killer || item.key || `item_${acc.length}`;
        acc[key] = item; 
        return acc; 
      }, {}) 
    : (typeof legalKillers === 'object' ? legalKillers : {});

  // Debug logging for troubleshooting
  console.log('MoneyBox items:', moneyBoxItems.length, moneyBoxItems);
  console.log('LegalKillers:', Object.keys(legalKillersObj).length, legalKillersObj);
  console.log('Dati certi:', dati);
  console.log('Semaforo:', semaforo);

  return (
    <div className="min-h-screen bg-[#09090b]">
      <Sidebar user={user} logout={logout} />
      
      <main className="ml-64 p-8">
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
            <Button 
              onClick={handleDownloadPDF}
              disabled={downloading}
              data-testid="download-pdf-btn"
              className="bg-gold text-zinc-950 hover:bg-gold-dim"
            >
              <FileDown className="w-4 h-4 mr-2" />
              {downloading ? 'Scaricando...' : 'Scarica Report'}
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
        
        {/* Header with Semaforo */}
        <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6 mb-8">
          <div className="flex items-start justify-between">
            <div>
              <h1 className="text-2xl font-serif font-bold text-zinc-100 mb-2">
                {safeRender(analysis.case_title || analysis.file_name, 'Analisi Perizia')}
              </h1>
              <div className="flex items-center gap-4 text-sm text-zinc-500">
                <span className="font-mono">Case: {safeRender(analysis.case_id)}</span>
                <span>•</span>
                <span>{analysis.pages_count || '?'} pagine</span>
                <span>•</span>
                <span>{new Date(analysis.created_at).toLocaleString('it-IT')}</span>
              </div>
              
              {/* Case Header with Evidence - support both formats */}
              <div className="mt-4 space-y-1">
                {(caseHeader.procedure || caseHeader.procedure_id) && (
                  <p className="text-sm text-gold flex items-center gap-2">
                    Procedura: {safeRender(caseHeader.procedure?.value || caseHeader.procedure_id)}
                    <EvidenceBadge evidence={getEvidence(caseHeader.procedure || caseHeader.procedure_id)} />
                  </p>
                )}
                {(caseHeader.tribunale) && (
                  <p className="text-sm text-zinc-400 flex items-center gap-2">
                    {safeRender(caseHeader.tribunale?.value || caseHeader.tribunale)}
                    <EvidenceBadge evidence={getEvidence(caseHeader.tribunale)} />
                  </p>
                )}
                {(caseHeader.address) && (
                  <p className="text-sm text-zinc-400 flex items-center gap-2">
                    {safeRender(caseHeader.address?.value || caseHeader.address)}
                    <EvidenceBadge evidence={getEvidence(caseHeader.address)} />
                  </p>
                )}
              </div>
            </div>
            <div className="text-right">
              <SemaforoBadge status={safeRender(semaforo.status, 'AMBER')} />
              <p className="text-sm text-zinc-400 mt-2 max-w-xs">
                {safeRender(semaforo.status_label || semaforo.reason_it || semaforo.status_it, '')}
              </p>
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
          </div>
          
          {/* Quick Decision with Evidence */}
          <div className="mt-6 p-4 bg-zinc-950 rounded-lg border border-zinc-800">
            <p className="text-xs font-mono uppercase text-zinc-500 mb-2">Decisione Rapida</p>
            <p className="text-lg font-semibold text-zinc-100">{safeRender(decision.summary_it, 'Analisi completata')}</p>
            <p className="text-sm text-zinc-500 mt-1">{safeRender(decision.summary_en, '')}</p>
            
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
            {/* Summary */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Riepilogo</h2>
              <div className="space-y-4">
                <div className="p-4 bg-zinc-950 rounded-lg">
                  <p className="text-zinc-300">{safeRender(summary.summary_it, 'Analisi documento completata.')}</p>
                </div>
                {summary.summary_en && (
                  <div className="p-4 bg-zinc-950 rounded-lg border-l-2 border-gold">
                    <p className="text-zinc-400 text-sm">{safeRender(summary.summary_en)}</p>
                  </div>
                )}
              </div>
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
                label="Diritto Reale" 
                value={dati.diritto_reale?.value || dati.diritto_reale}
                evidence={getEvidence(dati.diritto_reale)}
              />
            </div>
            
            {/* Indice di Convenienza */}
            {(indice.all_in_light_min || indice.all_in_light_max) && (
              <div className="bg-gold/10 border border-gold/30 rounded-xl p-6">
                <h3 className="text-lg font-semibold text-zinc-100 mb-4">Indice di Convenienza (All-In Light)</h3>
                <p className="text-zinc-300 mb-2">{safeRender(indice.dry_read_it)}</p>
                <div className="flex items-center gap-4 mt-4">
                  <div className="text-center p-4 bg-zinc-950 rounded-lg flex-1">
                    <p className="text-xs text-zinc-500 mb-1">MIN</p>
                    <p className="text-2xl font-mono font-bold text-gold">€{(indice.all_in_light_min || 0).toLocaleString()}</p>
                  </div>
                  <div className="text-center p-4 bg-zinc-950 rounded-lg flex-1">
                    <p className="text-xs text-zinc-500 mb-1">MAX</p>
                    <p className="text-2xl font-mono font-bold text-gold">€{(indice.all_in_light_max || 0).toLocaleString()}</p>
                  </div>
                </div>
              </div>
            )}
            
            {/* QA Status */}
            <div className={`p-4 rounded-xl border ${
              safeRender(qa.status) === 'PASS' ? 'bg-emerald-500/10 border-emerald-500/30' :
              safeRender(qa.status) === 'FAIL' ? 'bg-red-500/10 border-red-500/30' :
              'bg-amber-500/10 border-amber-500/30'
            }`}>
              <div className="flex items-center gap-3">
                {safeRender(qa.status) === 'PASS' ? (
                  <CheckCircle className="w-6 h-6 text-emerald-400" />
                ) : safeRender(qa.status) === 'FAIL' ? (
                  <XCircle className="w-6 h-6 text-red-400" />
                ) : (
                  <AlertTriangle className="w-6 h-6 text-amber-400" />
                )}
                <div>
                  <p className="font-semibold text-zinc-100">Quality Assurance: {safeRender(qa.status, qa.qa_pass || 'PENDING')}</p>
                  {Array.isArray(qa.reasons) && qa.reasons.map((r, i) => (
                    <p key={i} className="text-sm text-zinc-400">{safeRender(r.reason_it, r.detail_it || r.code || '')}</p>
                  ))}
                </div>
              </div>
            </div>
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
              
              {moneyBoxItems.length > 0 ? (
                <div className="space-y-3">
                  {moneyBoxItems.map((item, index) => (
                    <MoneyBoxItem key={index} item={item} />
                  ))}
                </div>
              ) : (
                <p className="text-zinc-500 text-center py-8">Nessun dato sui costi disponibile</p>
              )}
              
              {/* Total */}
              {moneyBox.total_extra_costs && (
                <div className="mt-6 p-4 bg-gold/10 border border-gold/30 rounded-lg">
                  <div className="flex items-center justify-between">
                    <span className="text-lg font-semibold text-zinc-100">Totale Costi Extra Stimati</span>
                    <span className="text-2xl font-mono font-bold text-gold">
                      €{(moneyBox.total_extra_costs?.range?.min || moneyBox.total_extra_costs?.amount?.min || 0).toLocaleString()} - €{(moneyBox.total_extra_costs?.range?.max || moneyBox.total_extra_costs?.amount?.max || 0).toLocaleString()}
                      {moneyBox.total_extra_costs?.max_is_open && '+'}
                    </span>
                  </div>
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
            </div>
          </TabsContent>
          
          {/* Details Tab */}
          <TabsContent value="details" className="space-y-6">
            {/* Case Header */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Dati Procedura</h2>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <DataValueWithEvidence 
                  label="Procedura" 
                  value={caseHeader.procedure_id}
                  evidence={getEvidence(caseHeader.procedure_id)}
                />
                <DataValueWithEvidence 
                  label="Lotto" 
                  value={caseHeader.lotto}
                  evidence={getEvidence(caseHeader.lotto)}
                />
                <DataValueWithEvidence 
                  label="Tribunale" 
                  value={caseHeader.tribunale}
                  evidence={getEvidence(caseHeader.tribunale)}
                />
                <DataValueWithEvidence 
                  label="Indirizzo" 
                  value={caseHeader.address?.value || caseHeader.address?.full || caseHeader.address}
                  evidence={getEvidence(caseHeader.address)}
                />
              </div>
            </div>
            
            {/* Abusi Edilizi */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Abusi Edilizi / Conformità</h2>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <DataValueWithEvidence 
                  label="Condono Presente" 
                  value={abusi.condono?.present}
                  evidence={getEvidence(abusi.condono)}
                />
                <DataValueWithEvidence 
                  label="Status Condono" 
                  value={abusi.condono?.status}
                  evidence={getEvidence(abusi.condono)}
                />
                <DataValueWithEvidence 
                  label="Agibilità" 
                  value={abusi.agibilita?.status || abusi.agibilita}
                  evidence={getEvidence(abusi.agibilita)}
                />
                <DataValueWithEvidence 
                  label="Commerciabilità" 
                  value={abusi.commerciabilita?.status}
                  evidence={getEvidence(abusi.commerciabilita)}
                />
              </div>
            </div>
            
            {/* Stato Conservativo */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Stato Conservativo</h2>
              <p className="text-zinc-300">{safeRender(conservativo.general_condition_it, 'Nessuna nota disponibile')}</p>
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
            
            {/* Formalità */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Formalità</h2>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <DataValueWithEvidence 
                  label="Ipoteca" 
                  value={formalita.ipoteca?.status}
                  evidence={getEvidence(formalita.ipoteca)}
                />
                <DataValueWithEvidence 
                  label="Pignoramento" 
                  value={formalita.pignoramento?.status}
                  evidence={getEvidence(formalita.pignoramento)}
                />
                <DataValueWithEvidence 
                  label="Cancellazione con Decreto" 
                  value={formalita.cancellazione_decreto?.status}
                  evidence={getEvidence(formalita.cancellazione_decreto)}
                />
              </div>
            </div>
            
            {/* Checklist Pre-Offerta */}
            {checklist.length > 0 && (
              <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
                <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Checklist Pre-Offerta</h2>
                <div className="space-y-2">
                  {checklist.map((item, i) => (
                    <div key={i} className="flex items-center gap-3 p-3 bg-zinc-950 rounded-lg">
                      {safeRender(item.status) === 'DONE' ? (
                        <CheckCircle className="w-5 h-5 text-emerald-400" />
                      ) : (
                        <div className="w-5 h-5 rounded-full border-2 border-zinc-600" />
                      )}
                      <span className="text-zinc-300 text-sm flex-1">{safeRender(item.item_it, item.task_it || '')}</span>
                      {item.priority && (
                        <span className={`text-xs px-2 py-1 rounded ${
                          item.priority === 'P0' ? 'bg-red-500/20 text-red-400' :
                          item.priority === 'P1' ? 'bg-amber-500/20 text-amber-400' :
                          'bg-zinc-700 text-zinc-400'
                        }`}>{item.priority}</span>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            )}
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
