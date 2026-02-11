import React, { useEffect, useState } from 'react';
import axios from 'axios';
import { Button } from '../../components/ui/button';
import AdminLayout from './AdminLayout';
import { Users, FileText, Image, MessageSquare, CreditCard } from 'lucide-react';
import { toast } from 'sonner';
import { useNavigate } from 'react-router-dom';

const API_URL = process.env.REACT_APP_BACKEND_URL;

const StatCard = ({ icon, label, value }) => (
  <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
    <div className="flex items-center justify-between mb-4">
      {icon}
      <span className="text-xs font-mono text-zinc-500 uppercase">{label}</span>
    </div>
    <p className="text-3xl font-bold text-zinc-100">{value}</p>
  </div>
);

const AdminOverview = () => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const navigate = useNavigate();

  useEffect(() => {
    const fetchOverview = async () => {
      try {
        const response = await axios.get(`${API_URL}/api/admin/overview`, { withCredentials: true });
        setData(response.data);
      } catch (error) {
        toast.error('Errore nel caricamento overview admin');
      } finally {
        setLoading(false);
      }
    };
    fetchOverview();
  }, []);

  const totals = data?.totals || {};
  const last30 = data?.last_30d || {};
  const planCounts = data?.plan_counts || {};

  return (
    <AdminLayout title="Admin Overview" subtitle="Panoramica generale (GOD mode)">
      {loading ? (
        <div className="text-zinc-400 font-mono text-sm">Loading...</div>
      ) : (
        <>
          <div className="grid grid-cols-1 md:grid-cols-5 gap-4 mb-8">
            <StatCard icon={<Users className="w-8 h-8 text-gold" />} label="Utenti" value={totals.users || 0} />
            <StatCard icon={<FileText className="w-8 h-8 text-gold" />} label="Perizie" value={totals.perizie || 0} />
            <StatCard icon={<Image className="w-8 h-8 text-indigo-400" />} label="Immagini" value={totals.images || 0} />
            <StatCard icon={<MessageSquare className="w-8 h-8 text-emerald-400" />} label="Assistente" value={totals.assistant_qas || 0} />
            <StatCard icon={<CreditCard className="w-8 h-8 text-amber-400" />} label="Transazioni" value={totals.transactions || 0} />
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <p className="text-sm text-zinc-500 mb-2">Ultimi 30 giorni</p>
              <div className="space-y-2 text-sm text-zinc-200">
                <div className="flex justify-between"><span>Perizie</span><span>{last30.perizie || 0}</span></div>
                <div className="flex justify-between"><span>Immagini</span><span>{last30.images || 0}</span></div>
                <div className="flex justify-between"><span>Assistente</span><span>{last30.assistant_qas || 0}</span></div>
                <div className="flex justify-between"><span>Utenti Attivi</span><span>{last30.active_users || 0}</span></div>
                <div className="flex justify-between"><span>Pagato EUR</span><span>€{(last30.paid_eur || 0).toFixed(2)}</span></div>
              </div>
            </div>
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <p className="text-sm text-zinc-500 mb-2">Distribuzione Piani</p>
              <div className="space-y-2 text-sm text-zinc-200">
                <div className="flex justify-between"><span>Free</span><span>{planCounts.free || 0}</span></div>
                <div className="flex justify-between"><span>Pro</span><span>{planCounts.pro || 0}</span></div>
                <div className="flex justify-between"><span>Enterprise</span><span>{planCounts.enterprise || 0}</span></div>
                <div className="flex justify-between"><span>Altro</span><span>{planCounts.other || 0}</span></div>
              </div>
            </div>
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <p className="text-sm text-zinc-500 mb-2">Note</p>
              <p className="text-sm text-zinc-400">
                Panoramica operativa per monitorare crescita, uso e attività.
              </p>
            </div>
          </div>

          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-serif font-bold text-zinc-100">Top utenti (30d)</h3>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-zinc-500 border-b border-zinc-800">
                    <th className="py-2">Email</th>
                    <th className="py-2">Piano</th>
                    <th className="py-2">Perizie</th>
                    <th className="py-2">Immagini</th>
                    <th className="py-2">Assistente</th>
                    <th className="py-2">Ultima attività</th>
                    <th className="py-2"></th>
                  </tr>
                </thead>
                <tbody>
                  {(data?.top_users_30d || []).map((u) => (
                    <tr key={u.user_id} className="border-b border-zinc-800 text-zinc-200">
                      <td className="py-2">{u.email || '-'}</td>
                      <td className="py-2 capitalize">{u.plan || '-'}</td>
                      <td className="py-2">{u.perizie || 0}</td>
                      <td className="py-2">{u.images || 0}</td>
                      <td className="py-2">{u.assistant_qas || 0}</td>
                      <td className="py-2">{u.last_active_at ? new Date(u.last_active_at).toLocaleString() : '-'}</td>
                      <td className="py-2 text-right">
                        <Button
                          variant="outline"
                          className="border-zinc-700 text-zinc-300"
                          onClick={() => navigate(`/admin/users/${u.user_id}`)}
                        >
                          View
                        </Button>
                      </td>
                    </tr>
                  ))}
                  {(!data?.top_users_30d || data.top_users_30d.length === 0) && (
                    <tr>
                      <td colSpan="7" className="py-6 text-center text-zinc-500">Nessun dato disponibile</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </AdminLayout>
  );
};

export default AdminOverview;
