import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import axios from 'axios';
import { useAuth } from '../context/AuthContext';
import { Sidebar } from './Dashboard';
import { Button } from '../components/ui/button';
import TechnicalFeedbackModal from '../components/TechnicalFeedbackModal';
import {
  Plus, FileText, MessageSquareText, AlertTriangle, ArrowUpRight,
  ArrowDownRight, Flag, ClipboardCheck, ExternalLink,
} from 'lucide-react';

const API_URL = process.env.REACT_APP_BACKEND_URL;

const SEMAFORO_LABELS = {
  GREEN: { label: 'Basso rischio', cls: 'text-emerald-400' },
  AMBER: { label: 'Attenzione', cls: 'text-amber-400' },
  RED: { label: 'Alto rischio', cls: 'text-red-400' },
};

const formatDate = (value) => {
  if (!value) return '—';
  try {
    return new Date(value).toLocaleDateString('it-IT', { day: '2-digit', month: '2-digit', year: 'numeric' });
  } catch (e) {
    return String(value);
  }
};

const MetricCard = ({ icon, label, value, accent = 'text-zinc-100' }) => (
  <div className="rounded-2xl border border-zinc-800 bg-zinc-900/70 p-5">
    <div className="flex items-center justify-between mb-3">
      <span className="text-[11px] font-mono uppercase tracking-[0.18em] text-zinc-500">{label}</span>
      <span className="text-zinc-600">{icon}</span>
    </div>
    <p className={`text-3xl font-mono font-bold ${accent}`}>{value ?? 0}</p>
  </div>
);

const BetaDashboard = () => {
  const { user, logout } = useAuth();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [modalOpen, setModalOpen] = useState(false);
  const [modalContext, setModalContext] = useState({});

  const loadSummary = async () => {
    try {
      const res = await axios.get(`${API_URL}/api/beta/dashboard-summary`, { withCredentials: true });
      setData(res.data);
    } catch (err) {
      // surfaced via UI fallback
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadSummary();
  }, []);

  const openFeedbackFor = (analysis) => {
    setModalContext({
      analysisId: analysis?.analysis_id || null,
      caseId: analysis?.case_id || null,
      fileName: analysis?.file_name || null,
    });
    setModalOpen(true);
  };

  const synthesis = data?.synthesis || {};
  const analyses = data?.analyses || [];

  return (
    <div className="min-h-screen bg-[#09090b]">
      <Sidebar user={user} logout={logout} />

      <main className="px-4 pb-12 pt-24 sm:px-6 lg:ml-64 lg:px-8 lg:pt-8">
        {/* Header */}
        <div className="mb-8">
          <p className="text-[11px] font-mono uppercase tracking-[0.24em] text-gold mb-2">Beta partner</p>
          <h1 className="text-3xl font-serif font-bold text-zinc-100">Dashboard Beta Partner — PeriziaScan</h1>
          <p className="mt-3 max-w-3xl text-sm leading-relaxed text-zinc-400">
            Usi PeriziaScan su una perizia reale o anonimizzata e confronti il risultato con la sua esperienza
            tecnica. Il suo feedback ci aiuter&agrave; a rendere l&rsquo;analisi pi&ugrave; precisa, pratica e
            aderente al lavoro quotidiano di un professionista.
          </p>
        </div>

        {/* A. Nuova analisi */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-5 mb-8">
          <div className="lg:col-span-2 rounded-2xl border border-zinc-800 bg-gradient-to-br from-zinc-900 to-zinc-900/40 p-6">
            <div className="flex items-start gap-4">
              <div className="rounded-xl bg-gold/10 p-3">
                <Plus className="w-6 h-6 text-gold" />
              </div>
              <div className="flex-1">
                <h2 className="text-lg font-semibold text-zinc-100 mb-2">Nuova perizia da analizzare</h2>
                <p className="text-sm leading-relaxed text-zinc-400 mb-5">
                  Pu&ograve; utilizzare una perizia reale o anonimizzata. Il report generato rester&agrave;
                  collegato alle sue osservazioni tecniche, cos&igrave; ogni feedback potr&agrave; essere
                  valutato nel contesto corretto.
                </p>
                <Button asChild className="bg-gold text-zinc-950 hover:bg-gold/90">
                  <Link to="/analysis/new" data-testid="beta-new-analysis">Carica nuova perizia</Link>
                </Button>
              </div>
            </div>
          </div>

          {/* D. Ruolo del beta partner */}
          <div className="rounded-2xl border border-zinc-800 bg-zinc-900/70 p-6">
            <h3 className="text-sm font-semibold text-zinc-200 mb-3 flex items-center gap-2">
              <ClipboardCheck className="w-4 h-4 text-gold" /> Il suo ruolo come beta partner
            </h3>
            <p className="text-[13px] leading-relaxed text-zinc-400">
              Il suo ruolo come beta partner non &egrave; semplicemente testare l&rsquo;app, ma aiutarci a
              capire come un professionista legge davvero una perizia: quali informazioni guarda per prime,
              quali rischi considera importanti, quali classificazioni sono utili e quali invece vanno
              corrette o semplificate.
            </p>
          </div>
        </div>

        {/* C. Sintesi feedback */}
        <h2 className="text-sm font-mono uppercase tracking-[0.18em] text-zinc-500 mb-4">Sintesi feedback</h2>
        <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-4 mb-10">
          <MetricCard icon={<MessageSquareText className="w-4 h-4" />} label="Feedback totali" value={synthesis.feedback_totali} accent="text-gold" />
          <MetricCard icon={<FileText className="w-4 h-4" />} label="Correzioni tecniche" value={synthesis.correzioni_tecniche} />
          <MetricCard icon={<AlertTriangle className="w-4 h-4" />} label="Informazioni mancanti" value={synthesis.informazioni_mancanti} />
          <MetricCard icon={<ArrowUpRight className="w-4 h-4" />} label="Class. troppo forti" value={synthesis.classificazioni_troppo_forti} />
          <MetricCard icon={<ArrowDownRight className="w-4 h-4" />} label="Class. troppo deboli" value={synthesis.classificazioni_troppo_deboli} />
          <MetricCard icon={<Flag className="w-4 h-4" />} label="Alta priorit&agrave;" value={synthesis.osservazioni_alta_priorita} accent="text-amber-400" />
        </div>

        {/* B. Le mie analisi */}
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-sm font-mono uppercase tracking-[0.18em] text-zinc-500">Le mie analisi</h2>
          <Link to="/history" className="text-xs text-gold hover:underline">Storico completo</Link>
        </div>

        <div className="rounded-2xl border border-zinc-800 bg-zinc-900/50 overflow-hidden">
          {loading ? (
            <div className="p-8 text-center text-sm text-zinc-500">Caricamento…</div>
          ) : analyses.length === 0 ? (
            <div className="p-10 text-center">
              <FileText className="w-10 h-10 text-zinc-700 mx-auto mb-3" />
              <p className="text-zinc-400 mb-1">Nessuna analisi ancora caricata.</p>
              <p className="text-sm text-zinc-600 mb-5">Carichi una perizia per generare il primo report da valutare.</p>
              <Button asChild variant="outline" className="border-zinc-700 text-zinc-200 hover:bg-zinc-800">
                <Link to="/analysis/new">Carica nuova perizia</Link>
              </Button>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-zinc-800 text-left text-[11px] font-mono uppercase tracking-wider text-zinc-500">
                    <th className="px-4 py-3 font-medium">File</th>
                    <th className="px-4 py-3 font-medium">Data</th>
                    <th className="px-4 py-3 font-medium">Stato</th>
                    <th className="px-4 py-3 font-medium text-center">Feedback</th>
                    <th className="px-4 py-3 font-medium text-center">Da risolvere</th>
                    <th className="px-4 py-3 font-medium">Ultimo feedback</th>
                    <th className="px-4 py-3 font-medium text-right">Azioni</th>
                  </tr>
                </thead>
                <tbody>
                  {analyses.map((a) => {
                    const sem = SEMAFORO_LABELS[a.semaforo_status];
                    return (
                      <tr key={a.analysis_id} className="border-b border-zinc-800/60 hover:bg-zinc-900/60">
                        <td className="px-4 py-3 text-zinc-200 max-w-[220px] truncate">{a.file_name || a.analysis_id}</td>
                        <td className="px-4 py-3 text-zinc-400">{formatDate(a.created_at)}</td>
                        <td className="px-4 py-3">
                          {sem ? <span className={`text-xs font-medium ${sem.cls}`}>{sem.label}</span> : <span className="text-xs text-zinc-500">{a.status || '—'}</span>}
                        </td>
                        <td className="px-4 py-3 text-center text-zinc-300 font-mono">{a.feedback_count}</td>
                        <td className="px-4 py-3 text-center">
                          {a.unresolved_feedback_count > 0 ? (
                            <span className="inline-flex items-center justify-center min-w-[1.5rem] rounded-full bg-amber-500/15 px-2 py-0.5 text-xs font-mono text-amber-400">
                              {a.unresolved_feedback_count}
                            </span>
                          ) : (
                            <span className="text-xs text-zinc-600">0</span>
                          )}
                        </td>
                        <td className="px-4 py-3 text-zinc-400">{formatDate(a.last_feedback_at)}</td>
                        <td className="px-4 py-3">
                          <div className="flex items-center justify-end gap-2">
                            <Button asChild size="sm" variant="outline" className="border-zinc-700 text-zinc-200 hover:bg-zinc-800 h-8">
                              <Link to={`/analysis/${a.analysis_id}`}>
                                Apri report <ExternalLink className="w-3.5 h-3.5 ml-1" />
                              </Link>
                            </Button>
                            <Button size="sm" className="bg-zinc-100 text-zinc-950 hover:bg-zinc-200 h-8" onClick={() => openFeedbackFor(a)}>
                              Condividi valutazione tecnica
                            </Button>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </main>

      <TechnicalFeedbackModal
        open={modalOpen}
        onOpenChange={setModalOpen}
        analysisId={modalContext.analysisId}
        caseId={modalContext.caseId}
        fileName={modalContext.fileName}
        prefill={{ feedbackLevel: 'report', sectionKey: 'altro' }}
        onSubmitted={() => { setModalOpen(false); loadSummary(); }}
      />
    </div>
  );
};

export default BetaDashboard;
