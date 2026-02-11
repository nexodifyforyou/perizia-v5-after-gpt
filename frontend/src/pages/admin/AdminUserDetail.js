import React, { useEffect, useState } from 'react';
import axios from 'axios';
import { useParams, useNavigate } from 'react-router-dom';
import AdminLayout from './AdminLayout';
import { Button } from '../../components/ui/button';
import { toast } from 'sonner';

const API_URL = process.env.REACT_APP_BACKEND_URL;

const AdminUserDetail = () => {
  const { userId } = useParams();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const navigate = useNavigate();

  useEffect(() => {
    const fetchDetail = async () => {
      try {
        const response = await axios.get(`${API_URL}/api/admin/users/${userId}`, { withCredentials: true });
        setData(response.data);
      } catch (error) {
        toast.error('Errore nel caricamento utente');
      } finally {
        setLoading(false);
      }
    };
    fetchDetail();
  }, [userId]);

  const user = data?.user || {};

  return (
    <AdminLayout title="Dettaglio Utente" subtitle={user.email || ''}>
      <div className="mb-6">
        <Button variant="outline" className="border-zinc-700 text-zinc-300" onClick={() => navigate('/admin/users')}>
          Back to Users
        </Button>
      </div>

      {loading ? (
        <div className="text-zinc-400 font-mono text-sm">Loading...</div>
      ) : (
        <>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <p className="text-sm text-zinc-500 mb-1">Plan</p>
              <p className="text-2xl font-serif font-bold text-zinc-100 capitalize">{user.plan || '-'}</p>
              <p className="text-xs text-zinc-500 mt-2">Created {user.created_at ? new Date(user.created_at).toLocaleDateString() : '-'}</p>
            </div>
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <p className="text-sm text-zinc-500 mb-1">Quota</p>
              <p className="text-sm text-zinc-200">Perizie: {user.quota?.perizia_scans_remaining || 0}</p>
              <p className="text-sm text-zinc-200">Immagini: {user.quota?.image_scans_remaining || 0}</p>
              <p className="text-sm text-zinc-200">Assistente: {user.quota?.assistant_messages_remaining || 0}</p>
            </div>
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
              <p className="text-sm text-zinc-500 mb-1">Note interne</p>
              <p className="text-sm text-zinc-200">{user.notes?.internal_status || 'N/A'}</p>
              <p className="text-xs text-zinc-400 mt-2">{user.notes?.note || 'Nessuna nota'}</p>
              <p className="text-xs text-zinc-500 mt-2">Tags: {(user.notes?.tags || []).join(', ') || '-'}</p>
            </div>
          </div>

          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6">
            <h3 className="text-lg font-serif font-bold text-zinc-100 mb-4">Recent Activity</h3>
            <div className="space-y-3">
              {(data?.recent_activity || []).map((item) => (
                <div key={`${item.type}-${item.id}`} className="flex items-center justify-between bg-zinc-950 border border-zinc-800 rounded-lg p-3">
                  <div>
                    <p className="text-sm text-zinc-200 uppercase font-mono">{item.type}</p>
                    <p className="text-xs text-zinc-500">Case: {item.case_id || '-'} | Run: {item.run_id || '-'}</p>
                    <p className="text-xs text-zinc-500">Created: {item.created_at ? new Date(item.created_at).toLocaleString() : '-'}</p>
                  </div>
                  <div className="text-right text-xs text-zinc-400">
                    {item.summary?.semaforo && <div>Semaforo: {item.summary.semaforo}</div>}
                    {item.summary?.image_count !== undefined && <div>Images: {item.summary.image_count}</div>}
                    {item.summary?.question_preview && <div>{item.summary.question_preview}</div>}
                  </div>
                </div>
              ))}
              {(!data?.recent_activity || data.recent_activity.length === 0) && (
                <div className="text-zinc-500 text-sm">Nessuna attività recente</div>
              )}
            </div>
          </div>
        </>
      )}
    </AdminLayout>
  );
};

export default AdminUserDetail;
