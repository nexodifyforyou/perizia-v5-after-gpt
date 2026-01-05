import React, { useState, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { Sidebar } from './Dashboard';
import { Button } from '../components/ui/button';
import { Progress } from '../components/ui/progress';
import { 
  Upload, 
  FileText, 
  AlertCircle,
  CheckCircle,
  Loader2
} from 'lucide-react';
import axios from 'axios';
import { toast } from 'sonner';

const API_URL = process.env.REACT_APP_BACKEND_URL;

const NewAnalysis = () => {
  const { user, logout } = useAuth();
  const navigate = useNavigate();
  const [file, setFile] = useState(null);
  const [dragging, setDragging] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState(null);

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
      setError('Solo file PDF sono accettati / Only PDF files are accepted');
      return;
    }
    
    if (selectedFile.type !== 'application/pdf') {
      setError('Solo file PDF sono accettati / Only PDF files are accepted');
      return;
    }
    
    // Check file size (max 50MB)
    if (selectedFile.size > 50 * 1024 * 1024) {
      setError('File troppo grande. Massimo 50MB / File too large. Maximum 50MB');
      return;
    }
    
    setFile(selectedFile);
  };

  const handleUpload = async () => {
    if (!file) return;
    
    setUploading(true);
    setProgress(0);
    setError(null);
    
    const formData = new FormData();
    formData.append('file', file);
    
    try {
      // Slower progress animation - at least 20 seconds before reaching 90%
      // Increment by 4% every 1 second = 22.5 seconds to reach 90%
      const progressInterval = setInterval(() => {
        setProgress(prev => {
          if (prev < 90) {
            return Math.min(prev + 4, 90);
          }
          return prev;
        });
      }, 1000);
      
      const response = await axios.post(`${API_URL}/api/analysis/perizia`, formData, {
        withCredentials: true,
        headers: {
          'Content-Type': 'multipart/form-data',
        },
        timeout: 300000 // 5 minute timeout for large documents
      });
      
      clearInterval(progressInterval);
      
      // Smooth completion animation
      setProgress(95);
      await new Promise(resolve => setTimeout(resolve, 500));
      setProgress(100);
      
      toast.success('Analisi completata!');
      
      // Navigate to results after brief delay
      setTimeout(() => {
        navigate(`/analysis/${response.data.analysis_id}`);
      }, 1000);
      
    } catch (err) {
      console.error('Upload error:', err);
      const errorMessage = err.response?.data?.detail?.message_it || 
                          err.response?.data?.detail || 
                          'Errore durante l\'analisi';
      setError(errorMessage);
      toast.error(errorMessage);
    } finally {
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
                  <div className="space-y-4">
                    <Progress value={progress} className="h-2" />
                    <div className="flex items-center justify-center gap-2 text-gold">
                      <Loader2 className="w-5 h-5 animate-spin" />
                      <span className="font-mono text-sm">
                        {progress < 90 ? 'Analisi in corso...' : 'Elaborazione risultati...'}
                      </span>
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
            <div className="mt-4 p-4 bg-red-500/10 border border-red-500/30 rounded-lg flex items-center gap-3">
              <AlertCircle className="w-5 h-5 text-red-400 flex-shrink-0" />
              <p className="text-red-400 text-sm">{error}</p>
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
              <span className="text-sm text-zinc-500">Scansioni rimanenti</span>
              <span className="font-mono text-gold font-bold">
                {user?.quota?.perizia_scans_remaining || 0}
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
