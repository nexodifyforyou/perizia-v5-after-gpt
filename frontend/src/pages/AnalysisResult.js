import React, { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { Sidebar, SemaforoBadge } from './Dashboard';
import { Button } from '../components/ui/button';
import { ScrollArea } from '../components/ui/scroll-area';
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
  ChevronDown,
  ChevronRight,
  ArrowLeft,
  Download
} from 'lucide-react';
import axios from 'axios';
import { toast } from 'sonner';

const API_URL = process.env.REACT_APP_BACKEND_URL;

// Money Box Item Component
const MoneyBoxItem = ({ item }) => {
  const getTypeColor = (type) => {
    switch (type) {
      case 'NEXODIFY_ESTIMATE': return 'text-gold';
      case 'NOT_SPECIFIED': return 'text-amber-400';
      case 'INFO_ONLY': return 'text-zinc-500';
      case 'FIXED': return 'text-emerald-400';
      default: return 'text-zinc-400';
    }
  };

  return (
    <div className="flex items-start justify-between p-4 bg-zinc-950/50 rounded-lg border border-zinc-800">
      <div className="flex-1">
        <div className="flex items-center gap-2 mb-1">
          <span className="font-mono text-xs text-gold">{item.code}</span>
          <span className="text-sm font-medium text-zinc-100">{item.label_it}</span>
        </div>
        <p className="text-xs text-zinc-500">{item.label_en}</p>
        {item.action_required_it && (
          <p className="text-xs text-amber-400 mt-1">{item.action_required_it}</p>
        )}
      </div>
      <div className="text-right">
        <span className={`font-mono text-sm font-bold ${getTypeColor(item.type)}`}>
          {item.type === 'NEXODIFY_ESTIMATE' && item.range 
            ? `€${item.range.min?.toLocaleString()} - €${item.range.max?.toLocaleString()}`
            : item.value 
              ? `€${item.value.toLocaleString()}`
              : item.type
          }
        </span>
      </div>
    </div>
  );
};

// Legal Killer Item Component
const LegalKillerItem = ({ name, data }) => {
  const getStatusIcon = (status) => {
    switch (status) {
      case 'YES': return <XCircle className="w-5 h-5 text-red-400" />;
      case 'NO': return <CheckCircle className="w-5 h-5 text-emerald-400" />;
      default: return <HelpCircle className="w-5 h-5 text-amber-400" />;
    }
  };

  const getStatusBg = (status) => {
    switch (status) {
      case 'YES': return 'bg-red-500/10 border-red-500/30';
      case 'NO': return 'bg-emerald-500/10 border-emerald-500/30';
      default: return 'bg-amber-500/10 border-amber-500/30';
    }
  };

  return (
    <div className={`p-4 rounded-lg border ${getStatusBg(data?.status)}`}>
      <div className="flex items-start gap-3">
        {getStatusIcon(data?.status)}
        <div className="flex-1">
          <p className="text-sm font-medium text-zinc-100">
            {name.replace(/_/g, ' ').toUpperCase()}
          </p>
          <p className="text-xs text-zinc-500 mt-1">{data?.action_required_it}</p>
        </div>
        <span className="font-mono text-xs px-2 py-1 rounded bg-zinc-800">
          {data?.status || 'UNKNOWN'}
        </span>
      </div>
    </div>
  );
};

// Red Flag Item Component
const RedFlagItem = ({ flag }) => {
  const getSeverityColor = (severity) => {
    switch (severity) {
      case 'RED': return 'border-red-500/30 bg-red-500/5';
      case 'AMBER': return 'border-amber-500/30 bg-amber-500/5';
      default: return 'border-zinc-800 bg-zinc-900/50';
    }
  };

  return (
    <div className={`p-4 rounded-lg border ${getSeverityColor(flag.severity)}`}>
      <div className="flex items-start gap-3">
        <AlertTriangle className={`w-5 h-5 flex-shrink-0 ${
          flag.severity === 'RED' ? 'text-red-400' : 'text-amber-400'
        }`} />
        <div>
          <p className="text-sm font-medium text-zinc-100">{flag.flag_it}</p>
          <p className="text-xs text-zinc-500 mt-1">{flag.flag_en}</p>
          <p className="text-xs text-zinc-400 mt-2">
            <span className="text-gold">Azione:</span> {flag.action_it}
          </p>
        </div>
      </div>
    </div>
  );
};

const AnalysisResult = () => {
  const { analysisId } = useParams();
  const { user, logout } = useAuth();
  const [analysis, setAnalysis] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchAnalysis();
  }, [analysisId]);

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

  const result = analysis.result?.result || analysis.result || {};
  const semaforo = result.semaforo_generale || {};
  const decision = result.decision_rapida_client || {};
  const moneyBox = result.money_box || {};
  const datiCerti = result.dati_certi_del_lotto || {};
  const abusi = result.abusi_edilizi_conformita || {};
  const occupativo = result.stato_occupativo || {};
  const conservativo = result.stato_conservativo || {};
  const formalita = result.formalita || {};
  const legalKillers = result.legal_killers_checklist || {};
  const redFlags = result.red_flags_operativi || [];
  const checklist = result.checklist_pre_offerta || [];
  const summary = result.summary_for_client || {};
  const qa = result.qa || {};

  return (
    <div className="min-h-screen bg-[#09090b]">
      <Sidebar user={user} logout={logout} />
      
      <main className="ml-64 p-8">
        {/* Back Button */}
        <Link to="/history" className="inline-flex items-center gap-2 text-zinc-400 hover:text-zinc-100 mb-6 transition-colors">
          <ArrowLeft className="w-4 h-4" />
          Torna allo storico
        </Link>
        
        {/* Header with Semaforo */}
        <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6 mb-8">
          <div className="flex items-start justify-between">
            <div>
              <h1 className="text-2xl font-serif font-bold text-zinc-100 mb-2">
                {analysis.case_title || analysis.file_name}
              </h1>
              <div className="flex items-center gap-4 text-sm text-zinc-500">
                <span className="font-mono">Case: {analysis.case_id}</span>
                <span>•</span>
                <span>{new Date(analysis.created_at).toLocaleString('it-IT')}</span>
              </div>
            </div>
            <div className="text-right">
              <SemaforoBadge status={semaforo.status || 'AMBER'} />
              <p className="text-sm text-zinc-400 mt-2">{semaforo.reason_it}</p>
            </div>
          </div>
          
          {/* Quick Decision */}
          <div className="mt-6 p-4 bg-zinc-950 rounded-lg border border-zinc-800">
            <p className="text-xs font-mono uppercase text-zinc-500 mb-2">Decisione Rapida</p>
            <p className="text-lg font-semibold text-zinc-100">{decision.summary_it}</p>
            <p className="text-sm text-zinc-500 mt-1">{decision.summary_en}</p>
          </div>
        </div>
        
        {/* Tabs for different sections */}
        <Tabs defaultValue="overview" className="w-full">
          <TabsList className="w-full justify-start bg-zinc-900 border border-zinc-800 p-1 mb-6">
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
                  <p className="text-zinc-300">{summary.summary_it}</p>
                </div>
                <div className="p-4 bg-zinc-950 rounded-lg border-l-2 border-gold">
                  <p className="text-zinc-400 text-sm">{summary.summary_en}</p>
                </div>
              </div>
            </div>
            
            {/* Key Data Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
                <div className="flex items-center gap-2 mb-3 text-zinc-500">
                  <DollarSign className="w-4 h-4" />
                  <span className="text-xs uppercase font-mono">Prezzo Base</span>
                </div>
                <p className="text-xl font-mono font-bold text-gold">
                  {datiCerti.prezzo_base_asta || 'N/A'}
                </p>
              </div>
              
              <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
                <div className="flex items-center gap-2 mb-3 text-zinc-500">
                  <Home className="w-4 h-4" />
                  <span className="text-xs uppercase font-mono">Superficie</span>
                </div>
                <p className="text-xl font-mono font-bold text-zinc-100">
                  {datiCerti.superficie_catastale || 'N/A'}
                </p>
              </div>
              
              <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
                <div className="flex items-center gap-2 mb-3 text-zinc-500">
                  <Users className="w-4 h-4" />
                  <span className="text-xs uppercase font-mono">Stato Occupativo</span>
                </div>
                <p className="text-xl font-mono font-bold text-zinc-100">
                  {occupativo.status || 'N/A'}
                </p>
              </div>
              
              <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
                <div className="flex items-center gap-2 mb-3 text-zinc-500">
                  <FileCheck className="w-4 h-4" />
                  <span className="text-xs uppercase font-mono">Conformità Urbanistica</span>
                </div>
                <p className="text-xl font-mono font-bold text-zinc-100">
                  {abusi.conformita_urbanistica || 'N/A'}
                </p>
              </div>
              
              <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
                <div className="flex items-center gap-2 mb-3 text-zinc-500">
                  <FileCheck className="w-4 h-4" />
                  <span className="text-xs uppercase font-mono">Conformità Catastale</span>
                </div>
                <p className="text-xl font-mono font-bold text-zinc-100">
                  {abusi.conformita_catastale || 'N/A'}
                </p>
              </div>
              
              <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
                <div className="flex items-center gap-2 mb-3 text-zinc-500">
                  <Scale className="w-4 h-4" />
                  <span className="text-xs uppercase font-mono">Diritto Reale</span>
                </div>
                <p className="text-xl font-mono font-bold text-zinc-100">
                  {datiCerti.diritto_reale || 'N/A'}
                </p>
              </div>
            </div>
            
            {/* QA Status */}
            <div className={`p-4 rounded-xl border ${
              qa.status === 'PASS' ? 'bg-emerald-500/10 border-emerald-500/30' :
              qa.status === 'FAIL' ? 'bg-red-500/10 border-red-500/30' :
              'bg-amber-500/10 border-amber-500/30'
            }`}>
              <div className="flex items-center gap-3">
                {qa.status === 'PASS' ? (
                  <CheckCircle className="w-6 h-6 text-emerald-400" />
                ) : qa.status === 'FAIL' ? (
                  <XCircle className="w-6 h-6 text-red-400" />
                ) : (
                  <AlertTriangle className="w-6 h-6 text-amber-400" />
                )}
                <div>
                  <p className="font-semibold text-zinc-100">Quality Assurance: {qa.status}</p>
                  {qa.reasons?.map((r, i) => (
                    <p key={i} className="text-sm text-zinc-400">{r.reason_it}</p>
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
                <h2 className="text-xl font-serif font-bold text-zinc-100">Money Box</h2>
              </div>
              
              <div className="space-y-3">
                {moneyBox.items?.map((item, index) => (
                  <MoneyBoxItem key={index} item={item} />
                ))}
              </div>
              
              {/* Total */}
              <div className="mt-6 p-4 bg-gold/10 border border-gold/30 rounded-lg">
                <div className="flex items-center justify-between">
                  <span className="text-lg font-semibold text-zinc-100">Totale Costi Extra Stimati</span>
                  <span className="text-2xl font-mono font-bold text-gold">
                    €{moneyBox.total_extra_costs?.range?.min?.toLocaleString() || '?'} - €{moneyBox.total_extra_costs?.range?.max?.toLocaleString() || '?'}
                    {moneyBox.total_extra_costs?.max_is_open && '+'}
                  </span>
                </div>
              </div>
            </div>
          </TabsContent>
          
          {/* Legal Killers Tab */}
          <TabsContent value="legal" className="space-y-6">
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <div className="flex items-center gap-3 mb-6">
                <Scale className="w-6 h-6 text-red-400" />
                <h2 className="text-xl font-serif font-bold text-zinc-100">Legal Killers Checklist</h2>
              </div>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {Object.entries(legalKillers).map(([key, value]) => (
                  <LegalKillerItem key={key} name={key} data={value} />
                ))}
              </div>
            </div>
          </TabsContent>
          
          {/* Details Tab */}
          <TabsContent value="details" className="space-y-6">
            {/* Abusi Edilizi */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Abusi Edilizi / Conformità</h2>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="p-4 bg-zinc-950 rounded-lg">
                  <p className="text-xs font-mono text-zinc-500 mb-1">Condono</p>
                  <p className="text-zinc-100">Presente: {abusi.condono?.present || 'N/A'}</p>
                  <p className="text-zinc-400 text-sm">Status: {abusi.condono?.status || 'N/A'}</p>
                </div>
              </div>
            </div>
            
            {/* Stato Conservativo */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Stato Conservativo</h2>
              <p className="text-zinc-300">{conservativo.note_it}</p>
              <p className="text-zinc-500 text-sm mt-2">{conservativo.note_en}</p>
              {conservativo.issues_found?.length > 0 && (
                <ul className="mt-4 space-y-2">
                  {conservativo.issues_found.map((issue, i) => (
                    <li key={i} className="flex items-center gap-2 text-amber-400 text-sm">
                      <AlertTriangle className="w-4 h-4" />
                      {issue}
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
                  <p className="text-zinc-100">{formalita.ipoteca || 'N/A'}</p>
                </div>
                <div className="p-4 bg-zinc-950 rounded-lg">
                  <p className="text-xs font-mono text-zinc-500 mb-1">Pignoramento</p>
                  <p className="text-zinc-100">{formalita.pignoramento || 'N/A'}</p>
                </div>
                <div className="p-4 bg-zinc-950 rounded-lg">
                  <p className="text-xs font-mono text-zinc-500 mb-1">Cancellazione Decreto</p>
                  <p className="text-zinc-100">{formalita.cancellazione_decreto || 'N/A'}</p>
                </div>
              </div>
            </div>
            
            {/* Checklist Pre-Offerta */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <h2 className="text-xl font-serif font-bold text-zinc-100 mb-4">Checklist Pre-Offerta</h2>
              <div className="space-y-2">
                {checklist.map((item, i) => (
                  <div key={i} className="flex items-center gap-3 p-3 bg-zinc-950 rounded-lg">
                    {item.status === 'DONE' ? (
                      <CheckCircle className="w-5 h-5 text-emerald-400" />
                    ) : (
                      <div className="w-5 h-5 rounded-full border-2 border-zinc-600" />
                    )}
                    <span className="text-zinc-300 text-sm">{item.item_it}</span>
                  </div>
                ))}
              </div>
            </div>
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
      </main>
    </div>
  );
};

export default AnalysisResult;
