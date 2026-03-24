import React, { useState, useEffect } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { Button } from '../components/ui/button';
import { 
  FileText, 
  Image, 
  MessageSquare, 
  Plus, 
  LogOut,
  User,
  CreditCard,
  History,
  Scale,
  ChevronRight,
  AlertTriangle,
  CheckCircle,
  Clock,
  Shield,
  Users
} from 'lucide-react';
import axios from 'axios';
import { toast } from 'sonner';
import { getAccountState } from '../lib/featureAccess';

const API_URL = process.env.REACT_APP_BACKEND_URL;

// Sidebar Navigation Component
const Sidebar = ({ user, logout }) => {
  const navigate = useNavigate();
  const accountState = getAccountState(user);
  const featureAccess = accountState.featureAccess;
  const availablePeriziaCredits =
    accountState?.periziaCredits?.totalAvailable ??
    accountState?.quota?.perizia_scans_remaining ??
    0;
  
  const navItems = [
    { icon: <FileText className="w-5 h-5" />, label: 'Dashboard', path: '/dashboard' },
    { icon: <Plus className="w-5 h-5" />, label: 'Nuova Analisi', path: '/analysis/new' },
    { icon: <Image className="w-5 h-5" />, label: 'Image Forensics', path: '/forensics', disabled: !featureAccess.canUseImageForensics, badge: 'In arrivo' },
    { icon: <MessageSquare className="w-5 h-5" />, label: 'Assistente', path: '/assistant', disabled: !featureAccess.canUseAssistant, badge: 'In arrivo' },
    { icon: <History className="w-5 h-5" />, label: 'Storico', path: '/history' },
    { icon: <CreditCard className="w-5 h-5" />, label: 'Abbonamento', path: '/billing' },
    { icon: <User className="w-5 h-5" />, label: 'Profilo', path: '/profile' },
  ];
  const adminItems = [
    { icon: <Shield className="w-5 h-5" />, label: 'Overview', path: '/admin' },
    { icon: <Users className="w-5 h-5" />, label: 'Utenti', path: '/admin/users' },
    { icon: <FileText className="w-5 h-5" />, label: 'Perizie', path: '/admin/perizie' },
    { icon: <Image className="w-5 h-5" />, label: 'Immagini', path: '/admin/images' },
    { icon: <MessageSquare className="w-5 h-5" />, label: 'Assistente', path: '/admin/assistant' },
    { icon: <CreditCard className="w-5 h-5" />, label: 'Transazioni', path: '/admin/transactions' },
  ];

  const handleLogout = async () => {
    await logout();
    navigate('/', { replace: true });
  };

  return (
    <aside className="fixed left-0 top-0 h-screen w-64 bg-zinc-950 border-r border-zinc-800 flex flex-col z-40">
      {/* Logo */}
      <div className="p-6 border-b border-zinc-800">
        <Link to="/dashboard" className="flex items-center gap-3">
          <Scale className="w-8 h-8 text-gold" />
          <span className="text-xl font-serif font-bold text-zinc-100">Nexodify</span>
        </Link>
      </div>
      
      {/* Navigation */}
      <nav className="flex-1 p-4 space-y-1">
        {navItems.map((item) => (
          <Link
            key={item.path}
            to={item.path}
            data-testid={`nav-${item.path.replace(/\//g, '-').replace(/^-/, '')}`}
            onClick={(event) => {
              if (!item.disabled) return;
              event.preventDefault();
              toast.info(`${item.label}: funzionalita non ancora disponibile`);
            }}
            className={`flex items-center gap-3 px-4 py-3 rounded-lg text-sm font-medium transition-all ${
              window.location.pathname === item.path || window.location.pathname.startsWith(item.path + '/')
                ? 'bg-gold/10 text-gold'
                : item.disabled
                  ? 'text-zinc-600 bg-zinc-950/70 cursor-not-allowed'
                  : 'text-zinc-400 hover:text-zinc-100 hover:bg-zinc-900'
            }`}
            aria-disabled={item.disabled ? 'true' : 'false'}
          >
            {item.icon}
            <span className="flex-1">{item.label}</span>
            {item.badge && (
              <span className={`text-[10px] font-mono uppercase tracking-wider ${item.disabled ? 'text-zinc-500' : 'text-zinc-600'}`}>
                {item.badge}
              </span>
            )}
          </Link>
        ))}
        {user?.is_master_admin && (
          <div className="pt-6">
            <p className="px-4 text-xs font-mono text-zinc-500 uppercase tracking-wider mb-2">ADMIN</p>
            {adminItems.map((item) => (
              <Link
                key={item.path}
                to={item.path}
                className={`flex items-center gap-3 px-4 py-3 rounded-lg text-sm font-medium transition-all ${
                  window.location.pathname === item.path || window.location.pathname.startsWith(item.path + '/')
                    ? 'bg-gold/10 text-gold'
                    : 'text-zinc-400 hover:text-zinc-100 hover:bg-zinc-900'
                }`}
              >
                {item.icon}
                {item.label}
              </Link>
            ))}
          </div>
        )}
      </nav>
      
      {/* User Info */}
      <div className="p-4 border-t border-zinc-800">
        <div className="flex items-center gap-3 mb-4">
          {user?.picture ? (
            <img src={user.picture} alt={user.name} className="w-10 h-10 rounded-full" />
          ) : (
            <div className="w-10 h-10 rounded-full bg-gold/20 flex items-center justify-center">
              <User className="w-5 h-5 text-gold" />
            </div>
          )}
          <div className="flex-1 min-w-0">
            <p className="text-sm font-medium text-zinc-100 truncate">{user?.name}</p>
            <p className="text-xs text-zinc-500 truncate">{user?.email}</p>
          </div>
        </div>
        <div className="mb-4 rounded-xl border border-zinc-800 bg-zinc-900/80 p-4">
          <div className="flex items-start justify-between gap-3 mb-3">
            <div>
              <p className="text-[11px] font-mono uppercase tracking-[0.24em] text-zinc-500">Piano attuale</p>
              <p className="mt-1 text-sm font-semibold text-zinc-100">{accountState.planLabel}</p>
            </div>
            <CreditCard className="w-4 h-4 text-gold flex-shrink-0 mt-0.5" />
          </div>
          <div className="flex items-end justify-between gap-3 mb-3">
            <div>
              <p className="text-[11px] font-mono uppercase tracking-[0.24em] text-zinc-500">Crediti disponibili</p>
              <p className="mt-1 text-xl font-mono font-bold text-gold">{availablePeriziaCredits}</p>
            </div>
            <span className="text-[11px] text-zinc-500">Perizie</span>
          </div>
          <Button asChild className="w-full bg-zinc-100 text-zinc-950 hover:bg-zinc-200">
            <Link to="/billing">Ricarica crediti</Link>
          </Button>
        </div>
        <Button 
          variant="outline" 
          onClick={handleLogout}
          data-testid="logout-btn"
          className="w-full border-zinc-700 text-zinc-400 hover:text-zinc-100 hover:bg-zinc-800"
        >
          <LogOut className="w-4 h-4 mr-2" />
          Esci
        </Button>
      </div>
    </aside>
  );
};

// Semaforo Badge Component
const normalizeSemaforoStatus = (status) => {
  const normalized = String(status || '').trim().toUpperCase();
  if (['GREEN', 'VERDE', 'BASSO RISCHIO'].includes(normalized)) return 'GREEN';
  if (['RED', 'ROSSO', 'ALTO RISCHIO'].includes(normalized)) return 'RED';
  if (['AMBER', 'GIALLO', 'ATTENZIONE'].includes(normalized)) return 'AMBER';
  return null;
};

const SemaforoBadge = ({ status }) => {
  const config = {
    GREEN: { bg: 'bg-emerald-500/10', text: 'text-emerald-400', border: 'border-emerald-500/30', label: 'Basso Rischio' },
    AMBER: { bg: 'bg-amber-500/10', text: 'text-amber-400', border: 'border-amber-500/30', label: 'Attenzione' },
    RED: { bg: 'bg-red-500/10', text: 'text-red-400', border: 'border-red-500/30', label: 'Alto Rischio' },
  };
  
  const normalizedStatus = normalizeSemaforoStatus(status) || 'AMBER';
  const c = config[normalizedStatus] || config.AMBER;
  
  return (
    <span className={`inline-flex items-center gap-2 px-3 py-1 rounded-full text-xs font-mono font-bold uppercase ${c.bg} ${c.text} border ${c.border}`}>
      <span className={`w-2 h-2 rounded-full ${normalizedStatus === 'GREEN' ? 'bg-emerald-500' : normalizedStatus === 'RED' ? 'bg-red-500' : 'bg-amber-500'}`} />
      {c.label}
    </span>
  );
};

const Dashboard = () => {
  const { user, logout, accountState } = useAuth();
  const navigate = useNavigate();
  const featureAccess = accountState.featureAccess;
  const availablePeriziaCredits =
    accountState?.periziaCredits?.totalAvailable ??
    accountState?.quota?.perizia_scans_remaining ??
    0;
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchStats();
  }, []);

  const fetchStats = async () => {
    try {
      const response = await axios.get(`${API_URL}/api/dashboard/stats`, {
        withCredentials: true
      });
      setStats(response.data);
    } catch (error) {
      toast.error('Failed to load dashboard stats');
    } finally {
      setLoading(false);
    }
  };

  const handleLockedFeatureClick = (featureName) => {
    toast.info(`${featureName}: funzionalita non ancora disponibile`);
  };

  return (
    <div className="min-h-screen bg-[#09090b]">
      <Sidebar user={user} logout={logout} />
      
      <main className="ml-64 p-8">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-serif font-bold text-zinc-100 mb-2">
            Benvenuto, {user?.name?.split(' ')[0]}
          </h1>
          <p className="text-zinc-400">
            Il tuo centro di controllo per analisi forensi immobiliari
          </p>
        </div>
        
        {/* Quick Stats */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
            <div className="flex items-center justify-between mb-4">
              <FileText className="w-8 h-8 text-gold" />
              <span className="text-xs font-mono text-zinc-500 uppercase">Perizie</span>
            </div>
            <p className="text-3xl font-bold text-zinc-100">{stats?.total_analyses || 0}</p>
            <p className="text-sm text-zinc-500">Analisi totali</p>
          </div>
          
          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
            <div className="flex items-center justify-between mb-4">
              <Image className="w-8 h-8 text-indigo-400" />
              <span className="text-xs font-mono text-zinc-500 uppercase">Immagini</span>
            </div>
            <p className="text-3xl font-bold text-zinc-100">{stats?.total_image_forensics || 0}</p>
            <p className="text-sm text-zinc-500">Forensics immagini</p>
          </div>
          
          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
            <div className="flex items-center justify-between mb-4">
              <MessageSquare className="w-8 h-8 text-emerald-400" />
              <span className="text-xs font-mono text-zinc-500 uppercase">Assistente</span>
            </div>
            <p className="text-3xl font-bold text-zinc-100">{stats?.total_assistant_queries || 0}</p>
            <p className="text-sm text-zinc-500">Domande</p>
          </div>
          
          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
            <div className="flex items-center justify-between mb-4">
              <span className={`px-2 py-1 rounded text-xs font-mono font-bold uppercase ${
                accountState.planId === 'enterprise' ? 'bg-gold/20 text-gold' :
                accountState.planId === 'pro' ? 'bg-indigo-500/20 text-indigo-400' :
                'bg-zinc-800 text-zinc-400'
              }`}>
                {accountState.planLabel}
              </span>
            </div>
            <p className="text-3xl font-bold text-zinc-100">{availablePeriziaCredits}</p>
            <p className="text-sm text-zinc-500">Crediti disponibili</p>
          </div>
        </div>
        
        {/* Quick Actions */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
          <Button 
            onClick={() => navigate('/analysis/new')}
            data-testid="quick-new-analysis-btn"
            className="bg-gold text-zinc-950 hover:bg-gold-dim h-auto py-6 flex-col gap-2"
          >
            <Plus className="w-8 h-8" />
            <span className="font-semibold">Nuova Analisi Perizia</span>
          </Button>
          
          <Button 
            onClick={() => (
              featureAccess.canUseImageForensics
                ? navigate('/forensics')
                : handleLockedFeatureClick('Image Forensics')
            )}
            data-testid="quick-image-forensics-btn"
            className="bg-zinc-900 border border-zinc-800 text-zinc-100 hover:bg-zinc-800 h-auto py-6 flex-col gap-2"
            variant="outline"
          >
            <Image className={`w-8 h-8 ${featureAccess.canUseImageForensics ? 'text-indigo-400' : 'text-zinc-600'}`} />
            <span className="font-semibold">Image Forensics</span>
            {!featureAccess.canUseImageForensics && (
              <span className="text-[10px] font-mono uppercase tracking-wider text-zinc-500">In arrivo</span>
            )}
          </Button>
          
          <Button 
            onClick={() => (
              featureAccess.canUseAssistant
                ? navigate('/assistant')
                : handleLockedFeatureClick('Assistente')
            )}
            data-testid="quick-assistant-btn"
            className="bg-zinc-900 border border-zinc-800 text-zinc-100 hover:bg-zinc-800 h-auto py-6 flex-col gap-2"
            variant="outline"
          >
            <MessageSquare className={`w-8 h-8 ${featureAccess.canUseAssistant ? 'text-emerald-400' : 'text-zinc-600'}`} />
            <span className="font-semibold">Chiedi all'Assistente</span>
            {!featureAccess.canUseAssistant && (
              <span className="text-[10px] font-mono uppercase tracking-wider text-zinc-500">In arrivo</span>
            )}
          </Button>
        </div>
        
        {/* Recent Analyses */}
        <div className="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden">
          <div className="p-6 border-b border-zinc-800 flex items-center justify-between">
            <h2 className="text-xl font-serif font-bold text-zinc-100">Analisi Recenti</h2>
            <Link to="/history" className="text-gold text-sm hover:underline flex items-center gap-1">
              Vedi tutte <ChevronRight className="w-4 h-4" />
            </Link>
          </div>
          
          <div className="divide-y divide-zinc-800">
            {loading ? (
              <div className="p-8 text-center">
                <div className="w-8 h-8 border-2 border-gold border-t-transparent rounded-full animate-spin mx-auto mb-2" />
                <p className="text-zinc-500 text-sm">Caricamento...</p>
              </div>
            ) : stats?.recent_analyses?.length > 0 ? (
              stats.recent_analyses.map((analysis) => (
                <Link
                  key={analysis.analysis_id}
                  to={`/analysis/${analysis.analysis_id}`}
                  data-testid={`recent-analysis-${analysis.analysis_id}`}
                  className="flex items-center justify-between p-4 hover:bg-zinc-800/50 transition-colors"
                >
                  <div className="flex items-center gap-4">
                    <FileText className="w-5 h-5 text-zinc-500" />
                    <div>
                      <p className="text-sm font-medium text-zinc-100">{analysis.case_title || analysis.case_id}</p>
                      <p className="text-xs text-zinc-500">
                        {new Date(analysis.created_at).toLocaleDateString('it-IT')}
                      </p>
                    </div>
                  </div>
                  <SemaforoBadge status={analysis.semaforo_status || analysis.result?.section_1_semaforo_generale?.status || analysis.result?.semaforo_generale?.status || analysis.result?.result?.section_1_semaforo_generale?.status || analysis.result?.result?.semaforo_generale?.status} />
                </Link>
              ))
            ) : (
              <div className="p-8 text-center">
                <FileText className="w-12 h-12 text-zinc-700 mx-auto mb-4" />
                <p className="text-zinc-400 mb-4">Nessuna analisi ancora</p>
                <Button onClick={() => navigate('/analysis/new')} className="bg-gold text-zinc-950 hover:bg-gold-dim">
                  Inizia la prima analisi
                </Button>
              </div>
            )}
          </div>
        </div>
        
        {/* Quota Warning */}
        {availablePeriziaCredits <= 1 && accountState.planId === 'free' && (
          <div className="mt-8 bg-amber-500/10 border border-amber-500/30 rounded-xl p-6 flex items-center gap-4">
            <AlertTriangle className="w-8 h-8 text-amber-400 flex-shrink-0" />
            <div className="flex-1">
              <p className="text-amber-400 font-semibold mb-1">Quota quasi esaurita</p>
              <p className="text-zinc-400 text-sm">
                Hai ancora {availablePeriziaCredits} crediti disponibili. 
                Passa a Pro per continuare senza limiti.
              </p>
            </div>
            <Button onClick={() => navigate('/billing')} className="bg-gold text-zinc-950 hover:bg-gold-dim">
              Upgrade
            </Button>
          </div>
        )}
        
        {/* Disclaimer */}
        <div className="mt-8 p-4 bg-zinc-900/50 border border-zinc-800 rounded-lg text-center">
          <p className="text-xs text-zinc-500">
            Nexodify è una piattaforma di supporto all'analisi documentale. Non costituisce consulenza legale, fiscale o professionale.
          </p>
          <p className="text-xs text-zinc-600 mt-1">
            Nexodify is a document analysis support platform. It does not constitute legal, tax, or professional advice.
          </p>
        </div>
      </main>
    </div>
  );
};

export { Sidebar, SemaforoBadge };
export default Dashboard;
