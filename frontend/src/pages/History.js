import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { Sidebar, SemaforoBadge } from './Dashboard';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../components/ui/tabs';
import { 
  FileText, 
  Image, 
  MessageSquare, 
  Search,
  ChevronRight,
  Calendar,
  Trash2,
  AlertTriangle,
  X
} from 'lucide-react';
import axios from 'axios';
import { toast } from 'sonner';

const API_URL = process.env.REACT_APP_BACKEND_URL;

// Delete Confirmation Modal
const DeleteModal = ({ isOpen, onClose, onConfirm, title, message, isLoading }) => {
  if (!isOpen) return null;
  
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/70" onClick={onClose} />
      <div className="relative bg-zinc-900 border border-zinc-800 rounded-xl p-6 max-w-md w-full mx-4 shadow-xl">
        <button 
          onClick={onClose}
          className="absolute top-4 right-4 text-zinc-500 hover:text-zinc-300"
        >
          <X className="w-5 h-5" />
        </button>
        
        <div className="flex items-center gap-3 mb-4">
          <div className="p-2 bg-red-500/20 rounded-lg">
            <AlertTriangle className="w-6 h-6 text-red-400" />
          </div>
          <h3 className="text-lg font-semibold text-zinc-100">{title}</h3>
        </div>
        
        <p className="text-zinc-400 text-sm mb-6">{message}</p>
        
        <div className="flex gap-3 justify-end">
          <Button 
            variant="outline" 
            onClick={onClose}
            disabled={isLoading}
            className="border-zinc-700 text-zinc-300 hover:bg-zinc-800"
          >
            Annulla
          </Button>
          <Button 
            onClick={onConfirm}
            disabled={isLoading}
            className="bg-red-600 hover:bg-red-700 text-white"
          >
            {isLoading ? (
              <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin mr-2" />
            ) : (
              <Trash2 className="w-4 h-4 mr-2" />
            )}
            Elimina
          </Button>
        </div>
      </div>
    </div>
  );
};

const History = () => {
  const { user, logout } = useAuth();
  const [periziaHistory, setPeriziaHistory] = useState([]);
  const [imageHistory, setImageHistory] = useState([]);
  const [assistantHistory, setAssistantHistory] = useState([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  
  // Delete state
  const [deleteModal, setDeleteModal] = useState({ isOpen: false, type: null, id: null, title: '', message: '' });
  const [isDeleting, setIsDeleting] = useState(false);

  useEffect(() => {
    fetchHistory();
  }, []);

  const fetchHistory = async () => {
    setLoading(true);
    try {
      const [periziaRes, imageRes, assistantRes] = await Promise.all([
        axios.get(`${API_URL}/api/history/perizia`, { withCredentials: true }),
        axios.get(`${API_URL}/api/history/images`, { withCredentials: true }),
        axios.get(`${API_URL}/api/history/assistant`, { withCredentials: true })
      ]);
      
      setPeriziaHistory(periziaRes.data.analyses || []);
      setImageHistory(imageRes.data.forensics || []);
      setAssistantHistory(assistantRes.data.conversations || []);
    } catch (error) {
      toast.error('Errore nel caricamento dello storico');
    } finally {
      setLoading(false);
    }
  };

  // Delete single perizia
  const handleDeletePerizia = async (analysisId) => {
    setIsDeleting(true);
    try {
      await axios.delete(`${API_URL}/api/analysis/perizia/${analysisId}`, { withCredentials: true });
      setPeriziaHistory(prev => prev.filter(a => a.analysis_id !== analysisId));
      toast.success('Analisi eliminata con successo');
    } catch (error) {
      toast.error('Errore durante l\'eliminazione');
    } finally {
      setIsDeleting(false);
      setDeleteModal({ isOpen: false, type: null, id: null, title: '', message: '' });
    }
  };

  // Delete single image forensics
  const handleDeleteImage = async (forensicsId) => {
    setIsDeleting(true);
    try {
      await axios.delete(`${API_URL}/api/analysis/images/${forensicsId}`, { withCredentials: true });
      setImageHistory(prev => prev.filter(f => f.forensics_id !== forensicsId));
      toast.success('Analisi immagini eliminata con successo');
    } catch (error) {
      toast.error('Errore durante l\'eliminazione');
    } finally {
      setIsDeleting(false);
      setDeleteModal({ isOpen: false, type: null, id: null, title: '', message: '' });
    }
  };

  // Delete single assistant QA
  const handleDeleteAssistant = async (qaId) => {
    setIsDeleting(true);
    try {
      await axios.delete(`${API_URL}/api/analysis/assistant/${qaId}`, { withCredentials: true });
      setAssistantHistory(prev => prev.filter(q => q.qa_id !== qaId));
      toast.success('Conversazione eliminata con successo');
    } catch (error) {
      toast.error('Errore durante l\'eliminazione');
    } finally {
      setIsDeleting(false);
      setDeleteModal({ isOpen: false, type: null, id: null, title: '', message: '' });
    }
  };

  // Delete all history
  const handleDeleteAll = async () => {
    setIsDeleting(true);
    try {
      const response = await axios.delete(`${API_URL}/api/history/all`, { withCredentials: true });
      setPeriziaHistory([]);
      setImageHistory([]);
      setAssistantHistory([]);
      toast.success(`Eliminati ${response.data.deleted.total} elementi`);
    } catch (error) {
      toast.error('Errore durante l\'eliminazione');
    } finally {
      setIsDeleting(false);
      setDeleteModal({ isOpen: false, type: null, id: null, title: '', message: '' });
    }
  };

  // Open delete modal
  const openDeleteModal = (type, id = null, itemName = '') => {
    if (type === 'all') {
      const total = periziaHistory.length + imageHistory.length + assistantHistory.length;
      setDeleteModal({
        isOpen: true,
        type: 'all',
        id: null,
        title: 'Elimina tutto lo storico',
        message: `Sei sicuro di voler eliminare tutto lo storico? Questa azione eliminerà ${total} elementi (${periziaHistory.length} perizie, ${imageHistory.length} immagini, ${assistantHistory.length} conversazioni) e non può essere annullata.`
      });
    } else if (type === 'perizia') {
      setDeleteModal({
        isOpen: true,
        type: 'perizia',
        id,
        title: 'Elimina analisi perizia',
        message: `Sei sicuro di voler eliminare l'analisi "${itemName}"? Questa azione non può essere annullata.`
      });
    } else if (type === 'image') {
      setDeleteModal({
        isOpen: true,
        type: 'image',
        id,
        title: 'Elimina analisi immagini',
        message: 'Sei sicuro di voler eliminare questa analisi immagini? Questa azione non può essere annullata.'
      });
    } else if (type === 'assistant') {
      setDeleteModal({
        isOpen: true,
        type: 'assistant',
        id,
        title: 'Elimina conversazione',
        message: 'Sei sicuro di voler eliminare questa conversazione? Questa azione non può essere annullata.'
      });
    }
  };

  // Handle confirm delete
  const handleConfirmDelete = () => {
    if (deleteModal.type === 'all') {
      handleDeleteAll();
    } else if (deleteModal.type === 'perizia') {
      handleDeletePerizia(deleteModal.id);
    } else if (deleteModal.type === 'image') {
      handleDeleteImage(deleteModal.id);
    } else if (deleteModal.type === 'assistant') {
      handleDeleteAssistant(deleteModal.id);
    }
  };

  const filteredPerizia = periziaHistory.filter(item => 
    item.case_title?.toLowerCase().includes(search.toLowerCase()) ||
    item.case_id?.toLowerCase().includes(search.toLowerCase())
  );

  const totalItems = periziaHistory.length + imageHistory.length + assistantHistory.length;

  return (
    <div className="min-h-screen bg-[#09090b]">
      <Sidebar user={user} logout={logout} />
      
      <main className="ml-64 p-8">
        {/* Header */}
        <div className="flex items-start justify-between mb-8">
          <div>
            <h1 className="text-3xl font-serif font-bold text-zinc-100 mb-2">
              Storico Analisi
            </h1>
            <p className="text-zinc-400">
              Visualizza tutte le tue analisi passate
            </p>
          </div>
          
          {totalItems > 0 && (
            <Button
              onClick={() => openDeleteModal('all')}
              variant="outline"
              className="border-red-500/30 text-red-400 hover:bg-red-500/10 hover:border-red-500/50"
              data-testid="delete-all-btn"
            >
              <Trash2 className="w-4 h-4 mr-2" />
              Elimina Tutto ({totalItems})
            </Button>
          )}
        </div>
        
        {/* Search */}
        <div className="relative mb-6 max-w-md">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-zinc-500" />
          <Input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Cerca per nome o ID..."
            data-testid="history-search-input"
            className="pl-10 bg-zinc-900 border-zinc-800 text-zinc-100"
          />
        </div>
        
        {/* Tabs */}
        <Tabs defaultValue="perizia" className="w-full">
          <TabsList className="bg-zinc-900 border border-zinc-800 p-1 mb-6">
            <TabsTrigger value="perizia" data-testid="history-tab-perizia" className="flex items-center gap-2">
              <FileText className="w-4 h-4" />
              Perizie ({periziaHistory.length})
            </TabsTrigger>
            <TabsTrigger value="images" data-testid="history-tab-images" className="flex items-center gap-2">
              <Image className="w-4 h-4" />
              Immagini ({imageHistory.length})
            </TabsTrigger>
            <TabsTrigger value="assistant" data-testid="history-tab-assistant" className="flex items-center gap-2">
              <MessageSquare className="w-4 h-4" />
              Assistente ({assistantHistory.length})
            </TabsTrigger>
          </TabsList>
          
          {/* Perizia History */}
          <TabsContent value="perizia">
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden">
              {loading ? (
                <div className="p-8 text-center">
                  <div className="w-8 h-8 border-2 border-gold border-t-transparent rounded-full animate-spin mx-auto mb-2" />
                  <p className="text-zinc-500 text-sm">Caricamento...</p>
                </div>
              ) : filteredPerizia.length > 0 ? (
                <div className="divide-y divide-zinc-800">
                  {filteredPerizia.map((analysis) => (
                    <div
                      key={analysis.analysis_id}
                      data-testid={`history-item-${analysis.analysis_id}`}
                      className="flex items-center justify-between p-4 hover:bg-zinc-800/50 transition-colors group"
                    >
                      <Link
                        to={`/analysis/${analysis.analysis_id}`}
                        className="flex items-center gap-4 flex-1"
                      >
                        <FileText className="w-5 h-5 text-gold" />
                        <div>
                          <p className="text-sm font-medium text-zinc-100">
                            {analysis.case_title || analysis.file_name}
                          </p>
                          <div className="flex items-center gap-3 mt-1">
                            <span className="text-xs text-zinc-500 font-mono">{analysis.case_id}</span>
                            <span className="text-xs text-zinc-600 flex items-center gap-1">
                              <Calendar className="w-3 h-3" />
                              {new Date(analysis.created_at).toLocaleDateString('it-IT')}
                            </span>
                          </div>
                        </div>
                      </Link>
                      <div className="flex items-center gap-4">
                        <SemaforoBadge status={
                          analysis.result?.semaforo_generale?.status || 
                          analysis.result?.result?.semaforo_generale?.status || 
                          'AMBER'
                        } />
                        <button
                          onClick={(e) => {
                            e.preventDefault();
                            openDeleteModal('perizia', analysis.analysis_id, analysis.case_title || analysis.file_name);
                          }}
                          className="p-2 text-zinc-600 hover:text-red-400 hover:bg-red-500/10 rounded-lg opacity-0 group-hover:opacity-100 transition-all"
                          data-testid={`delete-perizia-${analysis.analysis_id}`}
                          title="Elimina"
                        >
                          <Trash2 className="w-4 h-4" />
                        </button>
                        <Link to={`/analysis/${analysis.analysis_id}`}>
                          <ChevronRight className="w-5 h-5 text-zinc-600" />
                        </Link>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="p-8 text-center">
                  <FileText className="w-12 h-12 text-zinc-700 mx-auto mb-4" />
                  <p className="text-zinc-400">
                    {search ? 'Nessun risultato trovato' : 'Nessuna analisi perizia'}
                  </p>
                </div>
              )}
            </div>
          </TabsContent>
          
          {/* Images History */}
          <TabsContent value="images">
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden">
              {loading ? (
                <div className="p-8 text-center">
                  <div className="w-8 h-8 border-2 border-indigo-400 border-t-transparent rounded-full animate-spin mx-auto mb-2" />
                  <p className="text-zinc-500 text-sm">Caricamento...</p>
                </div>
              ) : imageHistory.length > 0 ? (
                <div className="divide-y divide-zinc-800">
                  {imageHistory.map((forensics) => (
                    <div
                      key={forensics.forensics_id}
                      className="flex items-center justify-between p-4 hover:bg-zinc-800/50 transition-colors group"
                    >
                      <div className="flex items-center gap-4 flex-1">
                        <Image className="w-5 h-5 text-indigo-400" />
                        <div>
                          <p className="text-sm font-medium text-zinc-100">
                            {forensics.image_count} immagini analizzate
                          </p>
                          <span className="text-xs text-zinc-500 font-mono">{forensics.case_id}</span>
                        </div>
                      </div>
                      <div className="flex items-center gap-4">
                        <span className="text-xs text-zinc-600">
                          {new Date(forensics.created_at).toLocaleDateString('it-IT')}
                        </span>
                        <button
                          onClick={() => openDeleteModal('image', forensics.forensics_id)}
                          className="p-2 text-zinc-600 hover:text-red-400 hover:bg-red-500/10 rounded-lg opacity-0 group-hover:opacity-100 transition-all"
                          data-testid={`delete-image-${forensics.forensics_id}`}
                          title="Elimina"
                        >
                          <Trash2 className="w-4 h-4" />
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="p-8 text-center">
                  <Image className="w-12 h-12 text-zinc-700 mx-auto mb-4" />
                  <p className="text-zinc-400">Nessuna analisi immagini</p>
                </div>
              )}
            </div>
          </TabsContent>
          
          {/* Assistant History */}
          <TabsContent value="assistant">
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden">
              {loading ? (
                <div className="p-8 text-center">
                  <div className="w-8 h-8 border-2 border-emerald-400 border-t-transparent rounded-full animate-spin mx-auto mb-2" />
                  <p className="text-zinc-500 text-sm">Caricamento...</p>
                </div>
              ) : assistantHistory.length > 0 ? (
                <div className="divide-y divide-zinc-800">
                  {assistantHistory.map((qa) => (
                    <div
                      key={qa.qa_id}
                      className="p-4 hover:bg-zinc-800/50 transition-colors group"
                    >
                      <div className="flex items-start gap-4">
                        <MessageSquare className="w-5 h-5 text-emerald-400 flex-shrink-0 mt-0.5" />
                        <div className="flex-1 min-w-0">
                          <p className="text-sm font-medium text-zinc-100 truncate">
                            {qa.question}
                          </p>
                          <p className="text-xs text-zinc-500 mt-1 line-clamp-2">
                            {qa.result?.result?.answer_it}
                          </p>
                        </div>
                        <div className="flex items-center gap-3 flex-shrink-0">
                          <span className="text-xs text-zinc-600">
                            {new Date(qa.created_at).toLocaleDateString('it-IT')}
                          </span>
                          <button
                            onClick={() => openDeleteModal('assistant', qa.qa_id)}
                            className="p-2 text-zinc-600 hover:text-red-400 hover:bg-red-500/10 rounded-lg opacity-0 group-hover:opacity-100 transition-all"
                            data-testid={`delete-assistant-${qa.qa_id}`}
                            title="Elimina"
                          >
                            <Trash2 className="w-4 h-4" />
                          </button>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="p-8 text-center">
                  <MessageSquare className="w-12 h-12 text-zinc-700 mx-auto mb-4" />
                  <p className="text-zinc-400">Nessuna conversazione</p>
                </div>
              )}
            </div>
          </TabsContent>
        </Tabs>
      </main>
      
      {/* Delete Confirmation Modal */}
      <DeleteModal
        isOpen={deleteModal.isOpen}
        onClose={() => setDeleteModal({ isOpen: false, type: null, id: null, title: '', message: '' })}
        onConfirm={handleConfirmDelete}
        title={deleteModal.title}
        message={deleteModal.message}
        isLoading={isDeleting}
      />
    </div>
  );
};

export default History;
