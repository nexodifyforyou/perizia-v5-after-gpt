import React, { useEffect, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { Sidebar } from './Dashboard';
import { Button } from '../components/ui/button';
import { 
  User,
  Mail,
  Calendar,
  Shield,
  LogOut
} from 'lucide-react';
import { Trash2 } from 'lucide-react';
import { deleteRetainedPdf, getRetainedPdfs } from '../lib/api/perizia';
import { toast } from 'sonner';

const Profile = () => {
  const { user, logout, accountState } = useAuth();
  const navigate = useNavigate();
  const [retainedPdfs, setRetainedPdfs] = useState([]);
  const [deletingAnalysisId, setDeletingAnalysisId] = useState(null);
  const retentionAvailable = Boolean(
    user?.account?.feature_access?.pdf_retention_offer_enabled
    || user?.feature_access?.pdf_retention_offer_enabled
  );

  useEffect(() => {
    if (!retentionAvailable) return undefined;
    let active = true;
    getRetainedPdfs()
      .then((response) => {
        if (active) setRetainedPdfs(response.data?.retained_pdfs || []);
      })
      .catch(() => {
        if (active) setRetainedPdfs([]);
      });
    return () => {
      active = false;
    };
  }, [retentionAvailable]);

  const handleDeleteRetainedPdf = async (analysisId) => {
    setDeletingAnalysisId(analysisId);
    try {
      await deleteRetainedPdf(analysisId);
      setRetainedPdfs((rows) => rows.filter((row) => row.analysis_id !== analysisId));
      toast.success('PDF originale conservato eliminato e consenso revocato.');
    } catch (_error) {
      toast.error('Eliminazione non completata. Riprova: il sistema non dichiarerà eliminati dati ancora presenti.');
    } finally {
      setDeletingAnalysisId(null);
    }
  };

  const handleLogout = async () => {
    await logout();
    navigate('/', { replace: true });
  };

  return (
    <div className="min-h-screen bg-[#09090b]">
      <Sidebar user={user} logout={logout} />
      
      <main className="px-4 pb-8 pt-24 sm:px-6 lg:ml-64 lg:px-8 lg:pt-8">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-serif font-bold text-zinc-100 mb-2">
            Profilo
          </h1>
          <p className="text-zinc-400">
            Visualizza le informazioni del tuo account
          </p>
        </div>

        {/* Profile Card */}
        <div className="max-w-2xl">
          <div className="rounded-xl border border-zinc-800 bg-zinc-900 p-5 sm:p-8">
            {/* Avatar */}
            <div className="mb-8 flex flex-col gap-4 border-b border-zinc-800 pb-8 sm:flex-row sm:items-center sm:gap-6">
              {user?.picture ? (
                <img 
                  src={user.picture} 
                  alt={user.name}
                  className="w-24 h-24 rounded-full border-4 border-gold/20"
                />
              ) : (
                <div className="w-24 h-24 rounded-full bg-gold/20 flex items-center justify-center border-4 border-gold/20">
                  <User className="w-12 h-12 text-gold" />
                </div>
              )}
              <div className="min-w-0">
                <h2 className="text-2xl font-serif font-bold text-zinc-100 text-wrap-safe">{user?.name}</h2>
                <p className="text-zinc-500 text-wrap-safe">{user?.email}</p>
                {user?.is_master_admin && (
                  <span className="inline-flex items-center gap-1 mt-2 px-3 py-1 bg-gold/20 text-gold text-xs font-mono rounded-full">
                    <Shield className="w-3 h-3" />
                    MASTER ADMIN
                  </span>
                )}
              </div>
            </div>

            {/* Details */}
            <div className="space-y-6">
              <div className="flex items-center gap-4">
                <div className="w-10 h-10 rounded-lg bg-zinc-800 flex items-center justify-center">
                  <Mail className="w-5 h-5 text-zinc-400" />
                </div>
                <div>
                  <p className="text-xs text-zinc-500">Email</p>
                  <p className="text-zinc-100">{user?.email}</p>
                </div>
              </div>

              <div className="flex items-center gap-4">
                <div className="w-10 h-10 rounded-lg bg-zinc-800 flex items-center justify-center">
                  <Shield className="w-5 h-5 text-zinc-400" />
                </div>
                <div>
                  <p className="text-xs text-zinc-500">Piano</p>
                  <p className="text-zinc-100 capitalize">{accountState.planLabel}</p>
                </div>
              </div>

              <div className="flex items-center gap-4">
                <div className="w-10 h-10 rounded-lg bg-zinc-800 flex items-center justify-center">
                  <Calendar className="w-5 h-5 text-zinc-400" />
                </div>
                <div>
                  <p className="text-xs text-zinc-500">ID Utente</p>
                  <p className="text-zinc-100 font-mono text-sm">{user?.user_id}</p>
                </div>
              </div>
            </div>

            {/* Logout Button */}
            <div className="mt-8 pt-8 border-t border-zinc-800">
              <div className="flex flex-wrap gap-3">
                <Button asChild className="bg-gold text-zinc-950 hover:bg-gold-dim">
                  <Link to="/billing">Ricarica crediti</Link>
                </Button>
                <Button 
                  onClick={handleLogout}
                  data-testid="profile-logout-btn"
                  variant="outline"
                  className="border-red-500/30 text-red-400 hover:bg-red-500/10"
                >
                  <LogOut className="w-4 h-4 mr-2" />
                  Disconnetti
                </Button>
              </div>
            </div>
          </div>

          {/* Quota Summary */}
          <div className="mt-6 bg-zinc-900 border border-zinc-800 rounded-xl p-6">
            <h3 className="text-lg font-semibold text-zinc-100 mb-4">Quota Utilizzo</h3>
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
              <div className="text-center p-4 bg-zinc-950 rounded-lg">
                <p className="text-2xl font-mono font-bold text-gold">
                  {accountState.quota.perizia_scans_remaining}
                </p>
                <p className="text-xs text-zinc-500 mt-1">Perizie</p>
              </div>
              <div className="text-center p-4 bg-zinc-950 rounded-lg">
                <p className="text-2xl font-mono font-bold text-indigo-400">
                  {accountState.quota.image_scans_remaining}
                </p>
                <p className="text-xs text-zinc-500 mt-1">Immagini</p>
              </div>
              <div className="text-center p-4 bg-zinc-950 rounded-lg">
                <p className="text-2xl font-mono font-bold text-emerald-400">
                  {accountState.quota.assistant_messages_remaining}
                </p>
                <p className="text-xs text-zinc-500 mt-1">Messaggi</p>
              </div>
            </div>
          </div>

          {retentionAvailable && (
            <div className="mt-6 rounded-xl border border-zinc-800 bg-zinc-900 p-6">
              <h3 className="text-lg font-semibold text-zinc-100">PDF diagnostici conservati</h3>
              <p className="mt-2 text-sm text-zinc-500">
                La conservazione è facoltativa, cifrata e limitata nel tempo. Puoi revocare
                il consenso eliminando subito la copia associata a una singola analisi.
              </p>
              {retainedPdfs.length === 0 ? (
                <p className="mt-4 text-sm text-zinc-400">Nessun PDF originale conservato.</p>
              ) : (
                <div className="mt-4 space-y-3">
                  {retainedPdfs.map((row) => (
                    <div key={row.analysis_id} className="flex flex-col gap-3 rounded-lg bg-zinc-950 p-4 sm:flex-row sm:items-center sm:justify-between">
                      <div>
                        <p className="font-mono text-xs text-zinc-300">{row.analysis_id}</p>
                        <p className="mt-1 text-xs text-zinc-500">
                          Eliminazione automatica entro {new Date(row.expires_at).toLocaleDateString('it-IT')}
                        </p>
                      </div>
                      <Button
                        onClick={() => handleDeleteRetainedPdf(row.analysis_id)}
                        disabled={deletingAnalysisId === row.analysis_id}
                        variant="outline"
                        className="border-red-500/30 text-red-400 hover:bg-red-500/10"
                      >
                        <Trash2 className="mr-2 h-4 w-4" />
                        {deletingAnalysisId === row.analysis_id ? 'Eliminazione…' : 'Revoca ed elimina'}
                      </Button>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </div>
      </main>
    </div>
  );
};

export default Profile;
