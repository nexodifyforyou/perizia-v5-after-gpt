import React, { useState, useEffect, useCallback } from 'react';
import { useParams, Link, useNavigate, useSearchParams } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { Sidebar } from './Dashboard';
import { Button } from '../components/ui/button';
import TechnicalFeedbackModal from '../components/TechnicalFeedbackModal';
import CorrectnessV2Tabs from '../components/correctness-v2/CorrectnessV2Tabs';
import { useCorrectnessV2CustomerView } from '../components/correctness-v2/useCustomerView';
import { useLotWorkspace } from '../components/correctness-v2/useLotWorkspace';
import { computeCorrectnessV2Visibility } from '../components/correctness-v2/visibility';
import { MessageSquarePlus, FileText, AlertTriangle, ArrowLeft, Trash2, X } from 'lucide-react';
import axios from 'axios';
import { toast } from 'sonner';

const API_URL = process.env.REACT_APP_BACKEND_URL;

// ---------------------------------------------------------------------------
// Analysis page.
//
// The Correctness V2 surface is the ONLY report surface: it always mounts once
// the availability probe resolves, and CustomerReportView owns every sub-state
// internally (ready / lot selection / preparing / busy / verification required
// / unavailable / no report), so no state ever produces a blank page.
//
// The page shell (header, technical feedback, delete) reads ONLY the metadata
// endpoint (/meta): analysis_id, case_id, case_title, file_name, created_at,
// pages_count, document_hash. It never fetches or renders the old report
// payload.
//
// Field-level confirmation ("Conferma necessaria") is intentionally NOT here:
// the old headline-verify modal was driven by the removed report body and is
// gone. The focused workflow lands in feature-customer-report-decision-workflow.
// ---------------------------------------------------------------------------
const AnalysisResult = () => {
  const { analysisId } = useParams();
  const navigate = useNavigate();
  const { user, logout } = useAuth();
  const [analysis, setAnalysis] = useState(null);
  const [loading, setLoading] = useState(true);
  const [showDeleteModal, setShowDeleteModal] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const [feedbackModalOpen, setFeedbackModalOpen] = useState(false);
  const canGiveTechnicalFeedback = Boolean(user?.is_beta_partner || user?.is_master_admin);

  // URL-persisted lot selection (Storico lot workspace, plan §F): the selected
  // lot lives in `?lot=` so refresh / back / forward / deep links keep it.
  const [searchParams, setSearchParams] = useSearchParams();
  const lotParam = searchParams.get('lot') || null;
  const handleSelectLot = useCallback((lotId) => {
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      if (lotId) next.set('lot', String(lotId));
      else next.delete('lot');
      return next;
    });
  }, [setSearchParams]);

  // Customer-safe per-lot workspace (pure read, zero side effects). When the
  // endpoint is unavailable (rollout 404 / failure) the page falls back to the
  // pre-workspace customer-view behavior.
  const workspaceState = useLotWorkspace(analysisId);
  // Multi-lot analyses land on the lot overview — never a stale latest-lot
  // report — until the customer explicitly opens a lot (`?lot=`).
  const lotOverviewActive = Boolean(
    workspaceState.available &&
    workspaceState.workspace?.analysis_state === 'LOT_OVERVIEW' &&
    !lotParam
  );

  // The customer view is fetched ONCE via this shared hook and handed to the
  // tabs, so the page's decisions never diverge from what renders. While the
  // lot overview is active the hook is disabled: returning to the overview
  // ("Torna ai lotti") is purely a URL change and fires NO API call.
  const isExactAdmin = Boolean(user?.correctness_v2_admin_view);
  const correctnessV2 = useCorrectnessV2CustomerView(analysisId, {
    enabled: !lotOverviewActive,
    selectedLotId: lotParam,
    onSelectLot: handleSelectLot,
  });
  const v2Resolved = Boolean(
    workspaceState.resolved && (lotOverviewActive || !correctnessV2.loading)
  );
  const correctnessV2Visibility = computeCorrectnessV2Visibility({
    isExactAdmin,
    v2Resolved,
  });

  const fetchAnalysis = async () => {
    try {
      const response = await axios.get(`${API_URL}/api/analysis/perizia/${analysisId}/meta`, {
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

  return (
    <div className="min-h-screen bg-[#09090b]">
      <Sidebar user={user} logout={logout} />

      <main className="px-4 pb-8 pt-24 sm:px-6 lg:ml-64 lg:px-8 lg:pt-8">
        {/* Back Button & Actions */}
        <div className="mb-6 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <Link to="/history" className="inline-flex items-center gap-2 text-zinc-400 hover:text-zinc-100 transition-colors">
            <ArrowLeft className="w-4 h-4" />
            Torna allo storico
          </Link>
          <div className="flex items-center gap-3 self-start sm:self-auto">
            {canGiveTechnicalFeedback && (
              <Button
                onClick={() => setFeedbackModalOpen(true)}
                variant="outline"
                data-testid="share-technical-feedback-btn"
                className="border-gold/40 text-gold hover:bg-gold/10 hover:border-gold/60"
              >
                <MessageSquarePlus className="w-4 h-4 mr-2" />
                Condividi valutazione tecnica
              </Button>
            )}
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

        {canGiveTechnicalFeedback && (
          <TechnicalFeedbackModal
            open={feedbackModalOpen}
            onOpenChange={setFeedbackModalOpen}
            analysisId={analysisId}
            caseId={analysis?.case_id || null}
            fileName={analysis?.file_name || null}
            documentHash={analysis?.document_hash || analysis?.input_sha256 || null}
            prefill={{ feedbackLevel: 'report', sectionKey: 'altro' }}
            onSubmitted={() => setFeedbackModalOpen(false)}
          />
        )}

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

        {/* Analysis metadata header (shell fields only, never report content). */}
        <div className="mb-6 rounded-xl border border-zinc-800 bg-zinc-900 p-5 sm:p-6">
          <h1 className="mb-2 text-2xl font-serif font-bold text-zinc-100 text-wrap-safe">
            {analysis.case_title || analysis.file_name || 'Analisi Perizia'}
          </h1>
          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-sm text-zinc-500">
            {analysis.case_id && <span className="font-mono">Case: {analysis.case_id}</span>}
            {analysis.case_id && <span>•</span>}
            <span>{analysis.pages_count || '?'} pagine</span>
            <span>•</span>
            <span>{analysis.created_at ? new Date(analysis.created_at).toLocaleString('it-IT') : '—'}</span>
          </div>
        </div>

        {/* While the V2 availability probe resolves, show a neutral placeholder
            so the page is never blank and never flashes an intermediate state. */}
        {correctnessV2Visibility.showLoadingPlaceholder && (
          <div className="mb-8 flex items-center gap-2 rounded-xl border border-zinc-800 bg-zinc-900 p-5 text-sm text-zinc-400">
            <div className="w-4 h-4 border-2 border-zinc-600 border-t-transparent rounded-full animate-spin" />
            Caricamento report…
          </div>
        )}

        {/* The Correctness V2 surface is the only report surface. It always
            mounts once resolved; CustomerReportView owns every sub-state. */}
        {!correctnessV2Visibility.showLoadingPlaceholder && correctnessV2Visibility.showV2Surface && (
          <CorrectnessV2Tabs
            analysisId={analysisId}
            canSeeAdminTab={correctnessV2Visibility.showAdminTab}
            customerState={correctnessV2}
            workspaceState={workspaceState}
            showLotOverview={lotOverviewActive}
            onOpenLot={handleSelectLot}
            backLabel={workspaceState.available ? 'Torna ai lotti' : undefined}
          />
        )}
      </main>
    </div>
  );
};

export default AnalysisResult;
