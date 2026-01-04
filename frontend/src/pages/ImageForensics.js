import React, { useState, useCallback } from 'react';
import { useAuth } from '../context/AuthContext';
import { Sidebar } from './Dashboard';
import { Button } from '../components/ui/button';
import { Progress } from '../components/ui/progress';
import { 
  Upload, 
  Image as ImageIcon, 
  AlertCircle,
  CheckCircle,
  Loader2,
  Camera,
  X
} from 'lucide-react';
import axios from 'axios';
import { toast } from 'sonner';

const API_URL = process.env.REACT_APP_BACKEND_URL;

const ImageForensics = () => {
  const { user, logout } = useAuth();
  const [files, setFiles] = useState([]);
  const [dragging, setDragging] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [result, setResult] = useState(null);
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
    
    const droppedFiles = Array.from(e.dataTransfer.files);
    validateAndAddFiles(droppedFiles);
  }, []);

  const handleFileChange = (e) => {
    const selectedFiles = Array.from(e.target.files);
    setError(null);
    validateAndAddFiles(selectedFiles);
  };

  const validateAndAddFiles = (newFiles) => {
    const validExtensions = ['.jpg', '.jpeg', '.png', '.webp'];
    const validFiles = newFiles.filter(file => {
      const ext = '.' + file.name.split('.').pop().toLowerCase();
      return validExtensions.includes(ext);
    });
    
    if (validFiles.length !== newFiles.length) {
      setError('Alcuni file non sono immagini valide. Formati accettati: JPG, PNG, WebP');
    }
    
    setFiles(prev => [...prev, ...validFiles].slice(0, 10)); // Max 10 images
  };

  const removeFile = (index) => {
    setFiles(prev => prev.filter((_, i) => i !== index));
  };

  const handleUpload = async () => {
    if (files.length === 0) return;
    
    setUploading(true);
    setProgress(0);
    setError(null);
    setResult(null);
    
    const formData = new FormData();
    files.forEach(file => {
      formData.append('files', file);
    });
    
    try {
      const progressInterval = setInterval(() => {
        setProgress(prev => Math.min(prev + 15, 90));
      }, 300);
      
      const response = await axios.post(`${API_URL}/api/analysis/image`, formData, {
        withCredentials: true,
        headers: {
          'Content-Type': 'multipart/form-data',
        }
      });
      
      clearInterval(progressInterval);
      setProgress(100);
      setResult(response.data);
      toast.success('Analisi immagini completata!');
      
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

  const resetForm = () => {
    setFiles([]);
    setResult(null);
    setError(null);
    setProgress(0);
  };

  return (
    <div className="min-h-screen bg-[#09090b]">
      <Sidebar user={user} logout={logout} />
      
      <main className="ml-64 p-8">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-serif font-bold text-zinc-100 mb-2">
            Image Forensics
          </h1>
          <p className="text-zinc-400">
            Carica foto del sito per analisi visiva di difetti, materiali e conformità
          </p>
        </div>
        
        {!result ? (
          <div className="max-w-2xl">
            {/* Upload Zone */}
            <div
              onDragOver={handleDragOver}
              onDragLeave={handleDragLeave}
              onDrop={handleDrop}
              className={`upload-zone ${dragging ? 'dragging' : ''} ${files.length > 0 ? 'border-indigo-500/50' : ''}`}
            >
              {files.length === 0 ? (
                <>
                  <Camera className={`w-16 h-16 mx-auto mb-6 ${dragging ? 'text-indigo-400' : 'text-zinc-600'}`} />
                  <h3 className="text-xl font-semibold text-zinc-100 mb-2">
                    Trascina qui le immagini
                  </h3>
                  <p className="text-zinc-500 mb-6">
                    oppure clicca per selezionare
                  </p>
                  <input
                    type="file"
                    accept=".jpg,.jpeg,.png,.webp"
                    multiple
                    onChange={handleFileChange}
                    className="hidden"
                    id="image-upload"
                    data-testid="image-upload-input"
                  />
                  <label htmlFor="image-upload">
                    <Button 
                      asChild
                      className="bg-zinc-800 text-zinc-100 hover:bg-zinc-700 cursor-pointer"
                    >
                      <span>Seleziona Immagini</span>
                    </Button>
                  </label>
                  <p className="text-xs text-zinc-600 mt-6">
                    JPG, PNG, WebP • Massimo 10 immagini
                  </p>
                </>
              ) : (
                <div>
                  <div className="grid grid-cols-5 gap-2 mb-6">
                    {files.map((file, index) => (
                      <div key={index} className="relative group">
                        <img 
                          src={URL.createObjectURL(file)} 
                          alt={file.name}
                          className="w-full h-20 object-cover rounded-lg"
                        />
                        <button
                          onClick={() => removeFile(index)}
                          className="absolute -top-2 -right-2 w-6 h-6 bg-red-500 rounded-full flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity"
                        >
                          <X className="w-4 h-4 text-white" />
                        </button>
                      </div>
                    ))}
                    {files.length < 10 && (
                      <label 
                        htmlFor="image-upload-more"
                        className="w-full h-20 border-2 border-dashed border-zinc-700 rounded-lg flex items-center justify-center cursor-pointer hover:border-indigo-500/50 transition-colors"
                      >
                        <input
                          type="file"
                          accept=".jpg,.jpeg,.png,.webp"
                          multiple
                          onChange={handleFileChange}
                          className="hidden"
                          id="image-upload-more"
                        />
                        <Camera className="w-6 h-6 text-zinc-600" />
                      </label>
                    )}
                  </div>
                  
                  <p className="text-sm text-zinc-400 mb-6">
                    {files.length} immagini selezionate
                  </p>
                  
                  {uploading ? (
                    <div className="space-y-4">
                      <Progress value={progress} className="h-2" />
                      <div className="flex items-center justify-center gap-2 text-indigo-400">
                        <Loader2 className="w-5 h-5 animate-spin" />
                        <span className="font-mono text-sm">Analisi in corso...</span>
                      </div>
                    </div>
                  ) : (
                    <div className="flex gap-4 justify-center">
                      <Button
                        onClick={resetForm}
                        variant="outline"
                        className="border-zinc-700 text-zinc-400 hover:bg-zinc-800"
                      >
                        Rimuovi tutte
                      </Button>
                      <Button
                        onClick={handleUpload}
                        data-testid="start-image-analysis-btn"
                        className="bg-indigo-600 text-white hover:bg-indigo-700"
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
            
            {/* Quota Info */}
            <div className="mt-6 p-4 bg-zinc-900/50 border border-zinc-800 rounded-lg">
              <div className="flex items-center justify-between">
                <span className="text-sm text-zinc-500">Analisi immagini rimanenti</span>
                <span className="font-mono text-indigo-400 font-bold">
                  {user?.quota?.image_scans_remaining || 0}
                </span>
              </div>
            </div>
          </div>
        ) : (
          /* Results Display */
          <div className="max-w-3xl">
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6 mb-6">
              <div className="flex items-center justify-between mb-6">
                <h2 className="text-xl font-serif font-bold text-zinc-100">Risultati Analisi</h2>
                <Button onClick={resetForm} variant="outline" className="border-zinc-700">
                  Nuova Analisi
                </Button>
              </div>
              
              {/* Summary */}
              <div className="p-4 bg-zinc-950 rounded-lg mb-6">
                <p className="text-zinc-300">{result.result?.summary_it}</p>
                <p className="text-zinc-500 text-sm mt-2">{result.result?.summary_en}</p>
              </div>
              
              {/* Findings */}
              {result.result?.findings?.map((finding, index) => (
                <div key={index} className={`p-4 rounded-lg border mb-4 ${
                  finding.severity === 'HIGH' ? 'bg-red-500/10 border-red-500/30' :
                  finding.severity === 'MEDIUM' ? 'bg-amber-500/10 border-amber-500/30' :
                  'bg-zinc-800 border-zinc-700'
                }`}>
                  <div className="flex items-start justify-between mb-2">
                    <h3 className="font-semibold text-zinc-100">{finding.title_it}</h3>
                    <span className={`text-xs font-mono px-2 py-1 rounded ${
                      finding.severity === 'HIGH' ? 'bg-red-500/20 text-red-400' :
                      finding.severity === 'MEDIUM' ? 'bg-amber-500/20 text-amber-400' :
                      'bg-zinc-700 text-zinc-400'
                    }`}>
                      {finding.severity}
                    </span>
                  </div>
                  <p className="text-sm text-zinc-400">{finding.what_i_see_it}</p>
                  <p className="text-sm text-zinc-500 mt-2">{finding.why_it_matters_it}</p>
                </div>
              ))}
            </div>
          </div>
        )}
      </main>
    </div>
  );
};

export default ImageForensics;
