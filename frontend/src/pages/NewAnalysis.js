import React, { useState, useCallback, useRef, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { Sidebar } from './Dashboard';
import { Button } from '../components/ui/button';
import { 
  Upload, 
  FileText, 
  AlertCircle,
  CheckCircle
} from 'lucide-react';
import axios from 'axios';

const API_URL = process.env.REACT_APP_BACKEND_URL;
const TIMELINE_STAGES = [
  { key: 'RECEIVED', icon: '✅', it: 'Documento ricevuto', en: 'Document received' },
  { key: 'READ', icon: '📄', it: 'Lettura PDF', en: 'Reading PDF' },
  { key: 'QUALITY', icon: '🔍', it: 'Controllo qualità', en: 'Quality check' },
  { key: 'INDEX', icon: '🧾', it: 'Indicizzazione importi e date', en: 'Indexing amounts and dates' },
  { key: 'SECTIONS', icon: '🧩', it: 'Sezioni con evidenze', en: 'Building evidence-backed sections' },
  { key: 'RISKS', icon: '⚠️', it: 'Rischi e verifiche', en: 'Risks and checks' },
  { key: 'DECISION', icon: '✍️', it: 'Decisione rapida', en: 'Quick decision' },
  { key: 'FINALIZE', icon: '✅', it: 'Finalizzazione report', en: 'Finalizing report' },
  { key: 'DONE', icon: '🎉', it: 'Report pronto', en: 'Report ready' }
];

const STAGE_INDEX = TIMELINE_STAGES.reduce((acc, stage, index) => {
  acc[stage.key] = index;
  return acc;
}, {});

const formatElapsed = (elapsedSec) => {
  const minutes = Math.floor(elapsedSec / 60);
  const seconds = elapsedSec % 60;
  return `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
};

const GENERIC_UPLOAD_ERROR = {
  titleIt: 'Errore durante l’analisi',
  titleEn: 'Error during analysis',
  bodyIt: 'Non è stato possibile completare l’upload o l’elaborazione. Riprova tra poco.',
  bodyEn: 'Could not complete the upload or processing. Please try again shortly.'
};

const toDisplayString = (value) => {
  if (typeof value !== 'string') {
    return null;
  }
  const trimmed = value.trim();
  return trimmed || null;
};

const buildUploadErrorState = (err) => {
  const responseData = err?.response?.data;
  const backendDetail = responseData?.detail;
  const directMessage = responseData?.message;
  const directMessageIt = responseData?.message_it;
  const directMessageEn = responseData?.message_en;

  const messageIt =
    toDisplayString(backendDetail?.message_it) ||
    toDisplayString(backendDetail?.message_en) ||
    toDisplayString(directMessageIt) ||
    toDisplayString(directMessageEn) ||
    toDisplayString(directMessage) ||
    (typeof backendDetail === 'string' ? toDisplayString(backendDetail) : null);

  const messageEn =
    toDisplayString(backendDetail?.message_en) ||
    toDisplayString(backendDetail?.message_it) ||
    toDisplayString(directMessageEn) ||
    toDisplayString(directMessageIt) ||
    toDisplayString(directMessage) ||
    (typeof backendDetail === 'string' ? toDisplayString(backendDetail) : null);

  if (backendDetail?.code === 'INSUFFICIENT_PERIZIA_CREDITS') {
    const facts = [
      backendDetail?.pages_count != null ? `Pagine documento: ${backendDetail.pages_count}` : null,
      backendDetail?.required_credits != null ? `Crediti richiesti: ${backendDetail.required_credits}` : null,
      backendDetail?.remaining_credits != null ? `Crediti rimanenti: ${backendDetail.remaining_credits}` : null
    ].filter(Boolean);

    const factsEn = [
      backendDetail?.pages_count != null ? `Document pages: ${backendDetail.pages_count}` : null,
      backendDetail?.required_credits != null ? `Required credits: ${backendDetail.required_credits}` : null,
      backendDetail?.remaining_credits != null ? `Remaining credits: ${backendDetail.remaining_credits}` : null
    ].filter(Boolean);

    return {
      titleIt: 'Crediti insufficienti',
      titleEn: 'Insufficient credits',
      bodyIt: messageIt || 'Crediti insufficienti per analizzare questa perizia.',
      bodyEn: messageEn || 'Insufficient credits to analyze this appraisal.',
      detailsIt: facts,
      detailsEn: factsEn
    };
  }

  return {
    titleIt: GENERIC_UPLOAD_ERROR.titleIt,
    titleEn: GENERIC_UPLOAD_ERROR.titleEn,
    bodyIt: messageIt || GENERIC_UPLOAD_ERROR.bodyIt,
    bodyEn: messageEn || GENERIC_UPLOAD_ERROR.bodyEn
  };
};

const NewAnalysis = () => {
  const { user, logout } = useAuth();
  const navigate = useNavigate();
  const [file, setFile] = useState(null);
  const [dragging, setDragging] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [currentStage, setCurrentStage] = useState(STAGE_INDEX.RECEIVED);
  const [elapsedSec, setElapsedSec] = useState(0);
  const [awaitingResponse, setAwaitingResponse] = useState(false);
  const [error, setError] = useState(null);
  const elapsedIntervalRef = useRef(null);
  const stageIntervalRef = useRef(null);

  const clearUploadTimers = useCallback(() => {
    if (elapsedIntervalRef.current) {
      clearInterval(elapsedIntervalRef.current);
      elapsedIntervalRef.current = null;
    }
    if (stageIntervalRef.current) {
      clearInterval(stageIntervalRef.current);
      stageIntervalRef.current = null;
    }
  }, []);

  useEffect(() => {
    return () => {
      clearUploadTimers();
    };
  }, [clearUploadTimers]);

  const handleDragOver = useCallback((e) => {
    e.preventDefault();
    setDragging(true);
  }, []);

  const handleDragLeave = useCallback((e) => {
    e.preventDefault();
    setDragging(false);
  }, []);

  const handleDrop = useCallback((e) => {
    e.preventDefault();
    setDragging(false);
    setError(null);
    
    const droppedFile = e.dataTransfer.files[0];
    validateAndSetFile(droppedFile);
  }, []);

  const handleFileChange = (e) => {
    const selectedFile = e.target.files[0];
    setError(null);
    validateAndSetFile(selectedFile);
  };

  const validateAndSetFile = (selectedFile) => {
    if (!selectedFile) return;
    
    // Check if PDF
    if (!selectedFile.name.toLowerCase().endsWith('.pdf')) {
      setError({
        titleIt: 'File non valido',
        titleEn: 'Invalid file',
        bodyIt: 'Solo file PDF sono accettati.',
        bodyEn: 'Only PDF files are accepted.'
      });
      return;
    }
    
    if (selectedFile.type !== 'application/pdf') {
      setError({
        titleIt: 'File non valido',
        titleEn: 'Invalid file',
        bodyIt: 'Solo file PDF sono accettati.',
        bodyEn: 'Only PDF files are accepted.'
      });
      return;
    }
    
    // Check file size (max 50MB)
    if (selectedFile.size > 50 * 1024 * 1024) {
      setError({
        titleIt: 'File troppo grande',
        titleEn: 'File too large',
        bodyIt: 'Massimo 50MB.',
        bodyEn: 'Maximum 50MB.'
      });
      return;
    }
    
    setFile(selectedFile);
  };

  const handleUpload = async () => {
    if (!file) return;
    
    clearUploadTimers();
    setUploading(true);
    setCurrentStage(STAGE_INDEX.RECEIVED);
    setElapsedSec(0);
    setAwaitingResponse(true);
    setError(null);
    
    const formData = new FormData();
    formData.append('file', file);
    const startTime = Date.now();
    
    try {
      elapsedIntervalRef.current = setInterval(() => {
        setElapsedSec(Math.floor((Date.now() - startTime) / 1000));
      }, 1000);

      stageIntervalRef.current = setInterval(() => {
        setCurrentStage((prev) => {
          if (prev < STAGE_INDEX.DECISION) {
            return prev + 1;
          }
          if (prev === STAGE_INDEX.DECISION) {
            return STAGE_INDEX.SECTIONS;
          }
          if (prev === STAGE_INDEX.SECTIONS) {
            return STAGE_INDEX.RISKS;
          }
          if (prev === STAGE_INDEX.RISKS) {
            return STAGE_INDEX.DECISION;
          }
          return prev;
        });
      }, 1200);
      
      const response = await axios.post(`${API_URL}/api/analysis/perizia`, formData, {
        withCredentials: true,
        headers: {
          'Content-Type': 'multipart/form-data',
        },
        timeout: 300000 // 5 minute timeout for large documents
      });

      clearUploadTimers();
      setAwaitingResponse(false);
      setCurrentStage(STAGE_INDEX.FINALIZE);
      await new Promise((resolve) => setTimeout(resolve, 700));
      setCurrentStage(STAGE_INDEX.DONE);
      await new Promise((resolve) => setTimeout(resolve, 300));
      navigate(`/analysis/${response.data.analysis_id}`);
      
    } catch (err) {
      console.error('Upload error:', err);
      clearUploadTimers();
      setAwaitingResponse(false);
      setError(buildUploadErrorState(err));
      setUploading(false);
    }
  };

  return (
    <div className="min-h-screen bg-[#09090b]">
      <Sidebar user={user} logout={logout} />
      
      <main className="ml-64 p-8">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-serif font-bold text-zinc-100 mb-2">
            Nuova Analisi Perizia
          </h1>
          <p className="text-zinc-400">
            Carica un documento perizia CTU in formato PDF per l'analisi forense
          </p>
        </div>
        
        {/* Upload Zone */}
        <div className="max-w-2xl">
          <div
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onDrop={handleDrop}
            className={`upload-zone ${dragging ? 'dragging' : ''} ${file ? 'border-gold/50' : ''}`}
          >
            {!file ? (
              <>
                <Upload className={`w-16 h-16 mx-auto mb-6 ${dragging ? 'text-gold' : 'text-zinc-600'}`} />
                <h3 className="text-xl font-semibold text-zinc-100 mb-2">
                  Trascina qui il file PDF
                </h3>
                <p className="text-zinc-500 mb-6">
                  oppure clicca per selezionare
                </p>
                <input
                  type="file"
                  accept=".pdf,application/pdf"
                  onChange={handleFileChange}
                  className="hidden"
                  id="pdf-upload"
                  data-testid="pdf-upload-input"
                />
                <label htmlFor="pdf-upload">
                  <Button 
                    asChild
                    className="bg-zinc-800 text-zinc-100 hover:bg-zinc-700 cursor-pointer"
                  >
                    <span>Seleziona PDF</span>
                  </Button>
                </label>
                <p className="text-xs text-zinc-600 mt-6">
                  Solo file PDF • Massimo 50MB
                </p>
              </>
            ) : (
              <div className="text-center">
                <FileText className="w-16 h-16 text-gold mx-auto mb-4" />
                <h3 className="text-xl font-semibold text-zinc-100 mb-2">
                  {file.name}
                </h3>
                <p className="text-zinc-500 mb-6">
                  {(file.size / 1024 / 1024).toFixed(2)} MB
                </p>
                
                {uploading ? (
                  <div className="space-y-6 text-left max-w-xl mx-auto">
                    <div className="relative">
                      <div className="absolute left-[19px] top-0 bottom-0 w-px bg-zinc-800" />
                      <div className="space-y-4">
                        {TIMELINE_STAGES.map((stage, index) => {
                          const completed = index < currentStage;
                          const isCurrent = index === currentStage;
                          return (
                            <div key={stage.key} className={`relative flex items-start gap-4 ${index > currentStage ? 'opacity-45' : 'opacity-100'}`}>
                              <div className={`relative z-10 flex h-10 w-10 items-center justify-center rounded-full border text-sm ${
                                isCurrent
                                  ? 'border-gold bg-gold/15 shadow-[0_0_0_3px_rgba(214,178,64,0.15)]'
                                  : completed
                                  ? 'border-emerald-400/50 bg-emerald-400/10 text-emerald-300'
                                  : 'border-zinc-700 bg-zinc-900 text-zinc-400'
                              }`}>
                                {completed ? '✓' : stage.icon}
                              </div>
                              <div className="pt-0.5">
                                <p className={`text-sm ${isCurrent ? 'text-zinc-100 font-semibold' : 'text-zinc-300'}`}>{stage.it}</p>
                                <p className="text-xs text-zinc-500">{stage.en}</p>
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    </div>

                    <div className="space-y-2 border border-zinc-800 bg-zinc-900/60 rounded-lg p-4">
                      <p className="text-sm text-zinc-200">
                        Tempo trascorso: <span className="font-mono text-gold">{formatElapsed(elapsedSec)}</span>
                      </p>
                      <p className="text-xs text-zinc-500">Elapsed: {formatElapsed(elapsedSec)}</p>
                      <p className="text-sm text-zinc-300">Tempo tipico: 45–120s</p>
                      <p className="text-xs text-zinc-500">Typical time: 45–120s</p>
                      {awaitingResponse && elapsedSec > 120 ? (
                        <>
                          <p className="text-sm text-amber-300">
                            Sta richiedendo più del solito. Documento complesso o server occupato.
                          </p>
                          <p className="text-xs text-amber-200/80">
                            Taking longer than usual. Complex document or server busy.
                          </p>
                        </>
                      ) : (
                        <>
                          <p className="text-sm text-zinc-300">
                            La durata dipende da pagine, qualità del PDF e carico del server.
                          </p>
                          <p className="text-xs text-zinc-500">
                            Duration depends on pages, PDF quality, and server load.
                          </p>
                        </>
                      )}
                    </div>

                    <div className="text-center">
                      <p className="text-sm text-zinc-300">
                        Stiamo elaborando il documento. Non chiudere questa pagina.
                      </p>
                      <p className="text-xs text-zinc-500">
                        We are processing the document. Do not close this page.
                      </p>
                    </div>
                  </div>
                ) : (
                  <div className="flex gap-4 justify-center">
                    <Button
                      onClick={() => setFile(null)}
                      variant="outline"
                      className="border-zinc-700 text-zinc-400 hover:bg-zinc-800"
                    >
                      Cambia file
                    </Button>
                    <Button
                      onClick={handleUpload}
                      data-testid="start-analysis-btn"
                      className="bg-gold text-zinc-950 hover:bg-gold-dim gold-glow"
                    >
                      Avvia Analisi
                    </Button>
                  </div>
                )}
              </div>
            )}
          </div>
          
          {/* Error Display */}
          {error && (
            <div className="mt-4 p-4 bg-red-500/10 border border-red-500/30 rounded-lg flex items-start gap-3">
              <AlertCircle className="w-5 h-5 text-red-400 flex-shrink-0 mt-0.5" />
              <div className="space-y-1">
                <p className="text-red-300 text-sm font-semibold">
                  {error.titleIt}
                </p>
                <p className="text-red-200/90 text-xs">
                  {error.titleEn}
                </p>
                <p className="text-red-300 text-sm">
                  {error.bodyIt}
                </p>
                <p className="text-red-200/80 text-xs">
                  {error.bodyEn}
                </p>
                {error.detailsIt?.length > 0 && (
                  <div className="pt-2">
                    {error.detailsIt.map((detail, index) => (
                      <p key={`it-${index}`} className="text-red-200/90 text-xs">
                        {detail}
                      </p>
                    ))}
                    {error.detailsEn?.map((detail, index) => (
                      <p key={`en-${index}`} className="text-red-100/60 text-[11px]">
                        {detail}
                      </p>
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}
          
          {/* Instructions */}
          <div className="mt-8 bg-zinc-900/50 border border-zinc-800 rounded-xl p-6">
            <h3 className="text-lg font-semibold text-zinc-100 mb-4">Come funziona</h3>
            <ul className="space-y-3">
              {[
                'Carica la perizia CTU in formato PDF',
                'Il nostro engine AI analizza ogni pagina',
                'Estrazione automatica di dati, costi e rischi',
                'Report forense con sistema semaforo',
                'Evidenze tracciate con numero di pagina'
              ].map((step, i) => (
                <li key={i} className="flex items-start gap-3 text-sm text-zinc-400">
                  <CheckCircle className="w-5 h-5 text-emerald-400 flex-shrink-0 mt-0.5" />
                  {step}
                </li>
              ))}
            </ul>
          </div>
          
          {/* Quota Info */}
          <div className="mt-6 p-4 bg-zinc-900/50 border border-zinc-800 rounded-lg">
            <div className="flex items-center justify-between">
              <span className="text-sm text-zinc-500">Crediti disponibili</span>
              <span className="font-mono text-gold font-bold">
                {(user?.account?.perizia_credits?.total_available
                  ?? user?.perizia_credits?.total_available
                  ?? user?.quota?.perizia_scans_remaining
                  ?? 0)}
              </span>
            </div>
          </div>
          
          {/* Disclaimer */}
          <div className="mt-6 p-4 bg-zinc-900/50 border border-zinc-800 rounded-lg text-center">
            <p className="text-xs text-zinc-500">
              L'analisi automatica è uno strumento di supporto. Consultare sempre un professionista qualificato prima di procedere.
            </p>
            <p className="text-xs text-zinc-600 mt-1">
              Automatic analysis is a support tool. Always consult a qualified professional before proceeding.
            </p>
          </div>
        </div>
      </main>
    </div>
  );
};

export default NewAnalysis;
