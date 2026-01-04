import React, { useState, useRef, useEffect } from 'react';
import { useAuth } from '../context/AuthContext';
import { Sidebar } from './Dashboard';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { ScrollArea } from '../components/ui/scroll-area';
import { 
  Send, 
  Bot, 
  User,
  Loader2,
  AlertCircle,
  Info
} from 'lucide-react';
import axios from 'axios';
import { toast } from 'sonner';

const API_URL = process.env.REACT_APP_BACKEND_URL;

const Assistant = () => {
  const { user, logout } = useAuth();
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const scrollRef = useRef(null);

  useEffect(() => {
    // Add welcome message
    setMessages([{
      role: 'assistant',
      content_it: 'Ciao! Sono il tuo assistente Nexodify. Posso aiutarti con domande sulle aste immobiliari italiane, perizie CTU e analisi documentale. Come posso assisterti oggi?',
      content_en: 'Hello! I\'m your Nexodify assistant. I can help you with questions about Italian real estate auctions, CTU appraisals, and document analysis. How can I help you today?'
    }]);
  }, []);

  useEffect(() => {
    // Scroll to bottom on new messages
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!input.trim() || loading) return;

    const userMessage = input.trim();
    setInput('');
    
    // Add user message
    setMessages(prev => [...prev, { role: 'user', content: userMessage }]);
    setLoading(true);

    try {
      const response = await axios.post(`${API_URL}/api/analysis/assistant`, 
        { question: userMessage },
        { withCredentials: true }
      );

      const answer = response.data.result;
      setMessages(prev => [...prev, {
        role: 'assistant',
        content_it: answer.answer_it,
        content_en: answer.answer_en,
        disclaimer_it: answer.safe_disclaimer_it,
        disclaimer_en: answer.safe_disclaimer_en
      }]);

    } catch (err) {
      console.error('Assistant error:', err);
      const errorMessage = err.response?.data?.detail?.message_it || 
                          err.response?.data?.detail || 
                          'Errore durante la richiesta';
      
      setMessages(prev => [...prev, {
        role: 'error',
        content: errorMessage
      }]);
      toast.error(errorMessage);
    } finally {
      setLoading(false);
    }
  };

  const suggestedQuestions = [
    "Cosa significa 'condono edilizio'?",
    "Come funziona un'asta immobiliare?",
    "Quali sono i rischi di un immobile occupato?",
    "Cosa verificare prima di un'offerta?"
  ];

  return (
    <div className="min-h-screen bg-[#09090b]">
      <Sidebar user={user} logout={logout} />
      
      <main className="ml-64 h-screen flex flex-col">
        {/* Header */}
        <div className="p-8 pb-4 border-b border-zinc-800">
          <h1 className="text-3xl font-serif font-bold text-zinc-100 mb-2">
            Assistente AI
          </h1>
          <p className="text-zinc-400">
            Chiedi informazioni su aste immobiliari, perizie e documentazione
          </p>
        </div>
        
        {/* Chat Area */}
        <div className="flex-1 overflow-hidden flex flex-col">
          <ScrollArea className="flex-1 p-8" ref={scrollRef}>
            <div className="max-w-3xl mx-auto space-y-6">
              {messages.map((message, index) => (
                <div 
                  key={index}
                  className={`flex gap-4 ${message.role === 'user' ? 'justify-end' : ''}`}
                >
                  {message.role !== 'user' && (
                    <div className={`w-10 h-10 rounded-full flex items-center justify-center flex-shrink-0 ${
                      message.role === 'error' ? 'bg-red-500/20' : 'bg-emerald-500/20'
                    }`}>
                      {message.role === 'error' ? (
                        <AlertCircle className="w-5 h-5 text-red-400" />
                      ) : (
                        <Bot className="w-5 h-5 text-emerald-400" />
                      )}
                    </div>
                  )}
                  
                  <div className={`max-w-[80%] ${message.role === 'user' ? 'order-first' : ''}`}>
                    <div className={`p-4 rounded-xl ${
                      message.role === 'user' 
                        ? 'bg-gold text-zinc-950' 
                        : message.role === 'error'
                          ? 'bg-red-500/10 border border-red-500/30'
                          : 'bg-zinc-900 border border-zinc-800'
                    }`}>
                      {message.role === 'user' ? (
                        <p>{message.content}</p>
                      ) : message.role === 'error' ? (
                        <p className="text-red-400">{message.content}</p>
                      ) : (
                        <>
                          <p className="text-zinc-100">{message.content_it}</p>
                          {message.content_en && message.content_en !== message.content_it && (
                            <p className="text-zinc-500 text-sm mt-3 pt-3 border-t border-zinc-800">
                              {message.content_en}
                            </p>
                          )}
                        </>
                      )}
                    </div>
                    
                    {/* Disclaimer */}
                    {message.disclaimer_it && (
                      <div className="flex items-center gap-2 mt-2 text-xs text-zinc-600">
                        <Info className="w-3 h-3" />
                        {message.disclaimer_it}
                      </div>
                    )}
                  </div>
                  
                  {message.role === 'user' && (
                    <div className="w-10 h-10 rounded-full flex items-center justify-center flex-shrink-0 bg-zinc-800">
                      <User className="w-5 h-5 text-zinc-400" />
                    </div>
                  )}
                </div>
              ))}
              
              {loading && (
                <div className="flex gap-4">
                  <div className="w-10 h-10 rounded-full flex items-center justify-center flex-shrink-0 bg-emerald-500/20">
                    <Loader2 className="w-5 h-5 text-emerald-400 animate-spin" />
                  </div>
                  <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
                    <div className="flex items-center gap-2 text-zinc-400">
                      <span className="w-2 h-2 bg-emerald-400 rounded-full animate-pulse" />
                      <span className="w-2 h-2 bg-emerald-400 rounded-full animate-pulse delay-100" />
                      <span className="w-2 h-2 bg-emerald-400 rounded-full animate-pulse delay-200" />
                    </div>
                  </div>
                </div>
              )}
            </div>
          </ScrollArea>
          
          {/* Suggested Questions */}
          {messages.length <= 1 && (
            <div className="px-8 py-4 border-t border-zinc-800">
              <p className="text-xs text-zinc-500 mb-3">Domande suggerite:</p>
              <div className="flex flex-wrap gap-2">
                {suggestedQuestions.map((q, i) => (
                  <button
                    key={i}
                    onClick={() => setInput(q)}
                    className="px-3 py-2 bg-zinc-900 border border-zinc-800 rounded-lg text-sm text-zinc-400 hover:text-zinc-100 hover:border-zinc-700 transition-colors"
                  >
                    {q}
                  </button>
                ))}
              </div>
            </div>
          )}
          
          {/* Input */}
          <div className="p-8 pt-4 border-t border-zinc-800">
            <form onSubmit={handleSubmit} className="max-w-3xl mx-auto flex gap-4">
              <Input
                value={input}
                onChange={(e) => setInput(e.target.value)}
                placeholder="Scrivi la tua domanda..."
                data-testid="assistant-input"
                className="flex-1 bg-zinc-900 border-zinc-800 text-zinc-100 placeholder:text-zinc-600 focus:border-gold focus:ring-gold"
                disabled={loading}
              />
              <Button 
                type="submit"
                data-testid="assistant-send-btn"
                disabled={!input.trim() || loading}
                className="bg-gold text-zinc-950 hover:bg-gold-dim disabled:opacity-50"
              >
                <Send className="w-5 h-5" />
              </Button>
            </form>
            
            <div className="mt-4 text-center space-y-2">
              <p className="text-xs text-zinc-600">
                Messaggi rimanenti: <span className="font-mono text-emerald-400">{user?.quota?.assistant_messages_remaining || 0}</span>
              </p>
              <p className="text-xs text-zinc-500">
                Le informazioni fornite hanno carattere informativo e non costituiscono consulenza legale.
              </p>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
};

export default Assistant;
