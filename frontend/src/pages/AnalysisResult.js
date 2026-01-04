import React, { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { Sidebar, SemaforoBadge } from './Dashboard';
import { Button } from '../components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../components/ui/tabs';
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
  Download,
  FileDown
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
    if (value.status) return safeRender(value.status, fallback);
    if (value.value) return safeRender(value.value, fallback);
    if (value.formatted) return safeRender(value.formatted, fallback);
    if (value.label_it) return safeRender(value.label_it, fallback);
    if (value.text) return safeRender(value.text, fallback);
    if (value.full) return safeRender(value.full, fallback);
    try {
      const str = JSON.stringify(value);
      return str.length > 100 ? str.substring(0, 100) + '...' : str;
    } catch {
      return fallback;
    }
  }
  return String(value) || fallback;
};

// Money Box Item Component
const MoneyBoxItem = ({ item }) => {
  const getTypeColor = (type) => {
    switch (type) {
      case 'NEXODIFY_ESTIMATE': return 'text-gold';
      case 'NOT_SPECIFIED': return 'text-amber-400';
      case 'INFO_ONLY': return 'text-zinc-500';
      case 'FIXED': return 'text-emerald-400';
      case 'RANGE': return 'text-gold';
      default: return 'text-zinc-400';
    }
  };

  const formatValue = () => {
    const type = safeRender(item.type, 'UNKNOWN');
    if ((type === 'NEXODIFY_ESTIMATE' || type === 'RANGE') && item.range) {
      const min = item.range.min;
      const max = item.range.max;
      return `€${min?.toLocaleString() || '?'} - €${max?.toLocaleString() || '?'}`;
    }
    if (item.value !== undefined && item.value !== null && item.value !== 0) {
      const val = typeof item.value === 'number' ? item.value : parseFloat(item.value);
      return isNaN(val) ? safeRender(item.value) : `€${val.toLocaleString()}`;
    }
    return type;
  };

  return (
    <div className="flex items-start justify-between p-4 bg-zinc-950/50 rounded-lg border border-zinc-800">
      <div className="flex-1">
        <div className="flex items-center gap-2 mb-1">
          <span className="font-mono text-xs text-gold">{safeRender(item.code, item.key || '?')}</span>
          <span className="text-sm font-medium text-zinc-100">{safeRender(item.label_it, item.label || 'Item')}</span>
        </div>
        <p className="text-xs text-zinc-500">{safeRender(item.label_en, '')}</p>
        {item.action_required_it && (
          <p className="text-xs text-amber-400 mt-1">{safeRender(item.action_required_it)}</p>
        )}
        {item.note_it && (
          <p className="text-xs text-zinc-400 mt-1">{safeRender(item.note_it)}</p>
        )}
        {item.source && (
          <p className="text-xs text-zinc-600 mt-1">Fonte: {safeRender(item.source)}</p>
        )}
      </div>
      <div className="text-right">
        <span className={`font-mono text-sm font-bold ${getTypeColor(safeRender(item.type))}`}>
          {formatValue()}
        </span>
      </div>
    </div>
  );
};

// Legal Killer Item Component
const LegalKillerItem = ({ name, data }) => {
  const status = safeRender(data?.status, 'NOT_SPECIFIED');
  
  const getStatusIcon = (status) => {
    if (status === 'YES') return <XCircle className="w-5 h-5 text-red-400" />;
    if (status === 'NO') return <CheckCircle className="w-5 h-5 text-emerald-400" />;
    return <HelpCircle className="w-5 h-5 text-amber-400" />;
  };

  const getStatusBg = (status) => {
    if (status === 'YES') return 'bg-red-500/10 border-red-500/30';
    if (status === 'NO') return 'bg-emerald-500/10 border-emerald-500/30';
    return 'bg-amber-500/10 border-amber-500/30';
  };

  const formatName = (name) => {
    return name.replace(/_/g, ' ').split(' ').map(w => 
      w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()
    ).join(' ');
  };

  return (
    <div className={`p-4 rounded-lg border ${getStatusBg(status)}`}>
      <div className="flex items-start gap-3">
        {getStatusIcon(status)}
        <div className="flex-1">
          <p className="text-sm font-medium text-zinc-100">{formatName(name)}</p>
          {data?.action_required_it && (
            <p className="text-xs text-zinc-500 mt-1">{safeRender(data.action_required_it)}</p>
          )}
        </div>
        <span className="font-mono text-xs px-2 py-1 rounded bg-zinc-800">{status}</span>
      </div>
    </div>
  );
};

// Red Flag Item Component
const RedFlagItem = ({ flag }) => {
  const severity = safeRender(flag.severity, 'AMBER');
  
  return (
    <div className={`p-4 rounded-lg border ${
      severity === 'RED' ? 'border-red-500/30 bg-red-500/5' : 'border-amber-500/30 bg-amber-500/5'
    }`}>
      <div className="flex items-start gap-3">
        <AlertTriangle className={`w-5 h-5 flex-shrink-0 ${
          severity === 'RED' ? 'text-red-400' : 'text-amber-400'
        }`} />
        <div>
          <p className="text-sm font-medium text-zinc-100">{safeRender(flag.flag_it, flag.message_it || flag.code || 'Flag')}</p>
          <p className="text-xs text-zinc-500 mt-1">{safeRender(flag.flag_en, flag.message_en || '')}</p>
          {(flag.action_it || flag.action_en) && (
            <p className="text-xs text-zinc-400 mt-2">
              <span className="text-gold">Azione:</span> {safeRender(flag.action_it, flag.action_en || '')}
            </p>
          )}
        </div>
      </div>
    </div>
  );
};

// Data Display Card Component
const DataCard = ({ icon: Icon, label, value, color = 'text-zinc-100' }) => (
  <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
    <div className="flex items-center gap-2 mb-3 text-zinc-500">
      <Icon className="w-4 h-4" />
      <span className="text-xs uppercase font-mono">{label}</span>
    </div>
    <p className={`text-xl font-mono font-bold ${color}`}>
      {safeRender(value, 'N/A')}
    </p>
  </div>
);

const AnalysisResult = () => {
  const { analysisId } = useParams();
  const { user, logout } = useAuth();
  const [analysis, setAnalysis] = useState(null);
  const [loading, setLoading] = useState(true);
  const [downloading, setDownloading] = useState(false);

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

  // Extract result data
  const result = analysis.result || {};
  const caseHeader = result.case_header || {};
  const semaforo = result.semaforo_generale || {};
  const decision = result.decision_rapida_client || {};
  const moneyBox = result.money_box || {};
  const dati = result.dati_certi_del_lotto || result.dati_certidel_lotto || {};
  const abusi = result.abusi_edilizi_conformita || {};
  const occupativo = result.stato_occupativo || {};
  const conservativo = result.stato_conservativo || {};
  const formalita = result.formalita || {};
  const legalKillers = result.legal_killers_checklist || {};
  const indice = result.indice_di_convenienza || {};
  const redFlags = Array.isArray(result.red_flags_operativi) ? result.red_flags_operativi : [];
  const checklist = Array.isArray(result.checklist_pre_offerta) ? result.checklist_pre_offerta : [];
  const summary = result.summary_for_client || {};
  const qa = result.qa || {};

  // Get money box items
  const moneyBoxItems = Array.isArray(moneyBox.items) ? moneyBox.items : 
                        Array.isArray(moneyBox) ? moneyBox : [];

  // Get legal killers as object
  const legalKillersObj = legalKillers.items ? 
    legalKillers.items.reduce((acc, item) => { acc[item.key] = item; return acc; }, {}) :
    legalKillers;

  return (
    <div className="min-h-screen bg-[#09090b]">
      <Sidebar user={user} logout={logout} />
      
      <main className="ml-64 p-8">
        {/* Back Button & Download */}
        <div className="flex items-center justify-between mb-6">
          <Link to="/history" className="inline-flex items-center gap-2 text-zinc-400 hover:text-zinc-100 transition-colors">
            <ArrowLeft className="w-4 h-4" />
            Torna allo storico
          </Link>
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
                <span>{new Date(analysis.created_at).toLocaleString('it-IT')}</span>
              </div>
              {caseHeader.procedure_id && caseHeader.procedure_id !== 'NOT_SPECIFIED_IN_PERIZIA' && (
                <p className="text-sm text-gold mt-2">Procedura: {safeRender(caseHeader.procedure_id)}</p>
              )}
              {caseHeader.tribunale && caseHeader.tribunale !== 'NOT_SPECIFIED_IN_PERIZIA' && (
                <p className="text-sm text-zinc-400">{safeRender(caseHeader.tribunale)}</p>
              )}
            </div>
            <div className="text-right">
              <SemaforoBadge status={safeRender(semaforo.status, 'AMBER')} />
              <p className="text-sm text-zinc-400 mt-2 max-w-xs">{safeRender(semaforo.reason_it, semaforo.status_it || '')}</p>
            </div>
          </div>
          
          {/* Quick Decision */}
          <div className="mt-6 p-4 bg-zinc-950 rounded-lg border border-zinc-800">
            <p className="text-xs font-mono uppercase text-zinc-500 mb-2">Decisione Rapida</p>
            <p className="text-lg font-semibold text-zinc-100">{safeRender(decision.summary_it, 'Analisi completata')}</p>
            <p className="text-sm text-zinc-500 mt-1">{safeRender(decision.summary_en, '')}</p>
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
            
            {/* Key Data Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              <DataCard 
                icon={DollarSign} 
                label="Prezzo Base" 
                value={dati.prezzo_base_asta?.formatted || dati.prezzo_base_asta?.value || dati.prezzo_base_asta}
                color="text-gold"
              />
              <DataCard 
                icon={Home} 
                label="Superficie" 
                value={dati.superficie_catastale?.value || dati.superficie_catastale}
              />
              <DataCard 
                icon={Users} 
                label="Stato Occupativo" 
                value={occupativo.status_it || occupativo.status}
              />
              <DataCard 
                icon={FileCheck} 
                label="Conformità Urbanistica" 
                value={abusi.conformita_urbanistica?.status || abusi.conformita_urbanistica}
              />
              <DataCard 
                icon={FileCheck} 
                label="Conformità Catastale" 
                value={abusi.conformita_catastale?.status || abusi.conformita_catastale}
              />
              <DataCard 
                icon={Scale} 
                label="Diritto Reale" 
                value={dati.diritto_reale?.value || dati.diritto_reale}
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
              <div className="flex items-center gap-3 mb-6">
                <DollarSign className="w-6 h-6 text-gold" />
                <h2 className="text-xl font-serif font-bold text-zinc-100">Portafoglio Costi (Money Box)</h2>
              </div>
              
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
              <div className="flex items-center gap-3 mb-6">
                <Scale className="w-6 h-6 text-red-400" />
                <h2 className="text-xl font-serif font-bold text-zinc-100">Legal Killers Checklist</h2>
              </div>
              
              {Object.keys(legalKillersObj).length > 0 ? (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {Object.entries(legalKillersObj).map(([key, value]) => (
                    <LegalKillerItem key={key} name={key} data={value} />
                  ))}
                </div>
              ) : (
                <p className="text-zinc-500 text-center py-8">Checklist legal killers non disponibile</p>
              )}
              
              {legalKillers.critical_note_it && (
                <div className="mt-6 p-4 bg-red-500/10 border border-red-500/30 rounded-lg">
                  <p className="text-red-400">{safeRender(legalKillers.critical_note_it?.text || legalKillers.critical_note_it)}</p>
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
                <div className="p-4 bg-zinc-950 rounded-lg">
                  <p className="text-xs font-mono text-zinc-500 mb-1">Procedura</p>
                  <p className="text-zinc-100">{safeRender(caseHeader.procedure_id)}</p>
                </div>
                <div className="p-4 bg-zinc-950 rounded-lg">
                  <p className="text-xs font-mono text-zinc-500 mb-1">Lotto</p>
                  <p className="text-zinc-100">{safeRender(caseHeader.lotto)}</p>
                </div>
                <div className="p-4 bg-zinc-950 rounded-lg">
                  <p className="text-xs font-mono text-zinc-500 mb-1">Tribunale</p>
                  <p className="text-zinc-100">{safeRender(caseHeader.tribunale)}</p>
                </div>
                <div className="p-4 bg-zinc-950 rounded-lg">
                  <p className="text-xs font-mono text-zinc-500 mb-1">Indirizzo</p>
                  <p className="text-zinc-100">{safeRender(caseHeader.address?.full || caseHeader.address)}</p>
                </div>
              </div>
            </div>
            
            {/* Abusi Edilizi */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Abusi Edilizi / Conformità</h2>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="p-4 bg-zinc-950 rounded-lg">
                  <p className="text-xs font-mono text-zinc-500 mb-1">Condono</p>
                  <p className="text-zinc-100">Presente: {safeRender(abusi.condono?.present)}</p>
                  <p className="text-zinc-400 text-sm">Status: {safeRender(abusi.condono?.status)}</p>
                </div>
                <div className="p-4 bg-zinc-950 rounded-lg">
                  <p className="text-xs font-mono text-zinc-500 mb-1">Agibilità</p>
                  <p className="text-zinc-100">{safeRender(abusi.agibilita?.status || abusi.agibilita)}</p>
                </div>
                <div className="p-4 bg-zinc-950 rounded-lg">
                  <p className="text-xs font-mono text-zinc-500 mb-1">Commerciabilità</p>
                  <p className="text-zinc-100">{safeRender(abusi.commerciabilita?.status || abusi.commerciabilita)}</p>
                </div>
              </div>
            </div>
            
            {/* Stato Conservativo */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Stato Conservativo</h2>
              <p className="text-zinc-300">{safeRender(conservativo.general_condition_it, conservativo.note_it || 'Nessuna nota disponibile')}</p>
              {Array.isArray(conservativo.issues_found) && conservativo.issues_found.length > 0 && (
                <ul className="mt-4 space-y-2">
                  {conservativo.issues_found.map((issue, i) => (
                    <li key={i} className="flex items-center gap-2 text-amber-400 text-sm">
                      <AlertTriangle className="w-4 h-4" />
                      {safeRender(issue.issue_it || issue)}
                    </li>
                  ))}
                </ul>
              )}
            </div>
            
            {/* Formalità */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Formalità</h2>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div className="p-4 bg-zinc-950 rounded-lg">
                  <p className="text-xs font-mono text-zinc-500 mb-1">Ipoteca</p>
                  <p className="text-zinc-100">{safeRender(formalita.ipoteca?.status || formalita.ipoteca)}</p>
                </div>
                <div className="p-4 bg-zinc-950 rounded-lg">
                  <p className="text-xs font-mono text-zinc-500 mb-1">Pignoramento</p>
                  <p className="text-zinc-100">{safeRender(formalita.pignoramento?.status || formalita.pignoramento)}</p>
                </div>
                <div className="p-4 bg-zinc-950 rounded-lg">
                  <p className="text-xs font-mono text-zinc-500 mb-1">Cancellazione Decreto</p>
                  <p className="text-zinc-100">{safeRender(formalita.cancellazione_decreto?.status || formalita.cancellazione_decreto || formalita.cancellazione)}</p>
                </div>
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
              <div className="flex items-center gap-3 mb-6">
                <AlertTriangle className="w-6 h-6 text-amber-400" />
                <h2 className="text-xl font-serif font-bold text-zinc-100">Red Flags Operativi</h2>
              </div>
              
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
