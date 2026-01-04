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
  Calendar
} from 'lucide-react';
import axios from 'axios';
import { toast } from 'sonner';

const API_URL = process.env.REACT_APP_BACKEND_URL;

const History = () => {
  const { user, logout } = useAuth();
  const [periziaHistory, setPeriziaHistory] = useState([]);
  const [imageHistory, setImageHistory] = useState([]);
  const [assistantHistory, setAssistantHistory] = useState([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');

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

  const filteredPerizia = periziaHistory.filter(item => 
    item.case_title?.toLowerCase().includes(search.toLowerCase()) ||
    item.case_id?.toLowerCase().includes(search.toLowerCase())
  );

  return (
    <div className="min-h-screen bg-[#09090b]">
      <Sidebar user={user} logout={logout} />
      
      <main className="ml-64 p-8">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-serif font-bold text-zinc-100 mb-2">
            Storico Analisi
          </h1>
          <p className="text-zinc-400">
            Visualizza tutte le tue analisi passate
          </p>
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
                    <Link
                      key={analysis.analysis_id}
                      to={`/analysis/${analysis.analysis_id}`}
                      data-testid={`history-item-${analysis.analysis_id}`}
                      className="flex items-center justify-between p-4 hover:bg-zinc-800/50 transition-colors"
                    >
                      <div className="flex items-center gap-4">
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
                      </div>
                      <div className="flex items-center gap-4">
                        <SemaforoBadge status={
                          analysis.result?.semaforo_generale?.status || 
                          analysis.result?.result?.semaforo_generale?.status || 
                          'AMBER'
                        } />
                        <ChevronRight className="w-5 h-5 text-zinc-600" />
                      </div>
                    </Link>
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
                      className="flex items-center justify-between p-4 hover:bg-zinc-800/50 transition-colors"
                    >
                      <div className="flex items-center gap-4">
                        <Image className="w-5 h-5 text-indigo-400" />
                        <div>
                          <p className="text-sm font-medium text-zinc-100">
                            {forensics.image_count} immagini analizzate
                          </p>
                          <span className="text-xs text-zinc-500 font-mono">{forensics.case_id}</span>
                        </div>
                      </div>
                      <span className="text-xs text-zinc-600">
                        {new Date(forensics.created_at).toLocaleDateString('it-IT')}
                      </span>
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
                      className="p-4 hover:bg-zinc-800/50 transition-colors"
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
                        <span className="text-xs text-zinc-600 flex-shrink-0">
                          {new Date(qa.created_at).toLocaleDateString('it-IT')}
                        </span>
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
    </div>
  );
};

export default History;
