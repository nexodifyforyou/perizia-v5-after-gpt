import React from 'react';
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

const Profile = () => {
  const { user, logout } = useAuth();

  return (
    <div className="min-h-screen bg-[#09090b]">
      <Sidebar user={user} logout={logout} />
      
      <main className="ml-64 p-8">
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
          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-8">
            {/* Avatar */}
            <div className="flex items-center gap-6 mb-8 pb-8 border-b border-zinc-800">
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
              <div>
                <h2 className="text-2xl font-serif font-bold text-zinc-100">{user?.name}</h2>
                <p className="text-zinc-500">{user?.email}</p>
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
                  <p className="text-zinc-100 capitalize">{user?.plan || 'Free'}</p>
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
              <Button 
                onClick={logout}
                data-testid="profile-logout-btn"
                variant="outline"
                className="border-red-500/30 text-red-400 hover:bg-red-500/10"
              >
                <LogOut className="w-4 h-4 mr-2" />
                Disconnetti
              </Button>
            </div>
          </div>

          {/* Quota Summary */}
          <div className="mt-6 bg-zinc-900 border border-zinc-800 rounded-xl p-6">
            <h3 className="text-lg font-semibold text-zinc-100 mb-4">Quota Utilizzo</h3>
            <div className="grid grid-cols-3 gap-4">
              <div className="text-center p-4 bg-zinc-950 rounded-lg">
                <p className="text-2xl font-mono font-bold text-gold">
                  {user?.quota?.perizia_scans_remaining || 0}
                </p>
                <p className="text-xs text-zinc-500 mt-1">Perizie</p>
              </div>
              <div className="text-center p-4 bg-zinc-950 rounded-lg">
                <p className="text-2xl font-mono font-bold text-indigo-400">
                  {user?.quota?.image_scans_remaining || 0}
                </p>
                <p className="text-xs text-zinc-500 mt-1">Immagini</p>
              </div>
              <div className="text-center p-4 bg-zinc-950 rounded-lg">
                <p className="text-2xl font-mono font-bold text-emerald-400">
                  {user?.quota?.assistant_messages_remaining || 0}
                </p>
                <p className="text-xs text-zinc-500 mt-1">Messaggi</p>
              </div>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
};

export default Profile;
