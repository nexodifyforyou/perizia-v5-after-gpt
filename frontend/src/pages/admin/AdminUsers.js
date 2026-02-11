import React, { useCallback, useEffect, useState } from 'react';
import axios from 'axios';
import AdminLayout from './AdminLayout';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { toast } from 'sonner';
import { useNavigate } from 'react-router-dom';

const API_URL = process.env.REACT_APP_BACKEND_URL;

const AdminUsers = () => {
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(true);
  const [q, setQ] = useState('');
  const [plan, setPlan] = useState('');
  const [sort, setSort] = useState('created_at');
  const [order, setOrder] = useState('desc');
  const [page, setPage] = useState(1);
  const [pageSize] = useState(20);
  const [total, setTotal] = useState(0);
  const [editingUser, setEditingUser] = useState(null);
  const [notesUser, setNotesUser] = useState(null);
  const [planValue, setPlanValue] = useState('free');
  const [quotaValues, setQuotaValues] = useState({});
  const [notesValue, setNotesValue] = useState('');
  const [notesTags, setNotesTags] = useState('');
  const [notesStatus, setNotesStatus] = useState('OK');
  const navigate = useNavigate();

  const fetchUsers = useCallback(async () => {
    setLoading(true);
    try {
      const response = await axios.get(`${API_URL}/api/admin/users`, {
        params: { q: q || undefined, plan: plan || undefined, sort, order, page, page_size: pageSize },
        withCredentials: true
      });
      setItems(response.data.items || []);
      setTotal(response.data.total || 0);
    } catch (error) {
      toast.error('Errore nel caricamento utenti');
    } finally {
      setLoading(false);
    }
  }, [q, plan, sort, order, page, pageSize]);

  useEffect(() => {
    fetchUsers();
  }, [fetchUsers]);

  const openEditModal = (user) => {
    setEditingUser(user);
    setPlanValue(user.plan || 'free');
    setQuotaValues({
      perizia_scans_remaining: user.quota?.perizia_scans_remaining ?? 0,
      image_scans_remaining: user.quota?.image_scans_remaining ?? 0,
      assistant_messages_remaining: user.quota?.assistant_messages_remaining ?? 0
    });
  };

  const openNotesModal = (user) => {
    setNotesUser(user);
    setNotesValue(user.notes?.note || '');
    setNotesTags((user.notes?.tags || []).join(', '));
    setNotesStatus(user.notes?.internal_status || 'OK');
  };

  const handlePlanSave = async () => {
    if (!editingUser) return;
    try {
      await axios.patch(`${API_URL}/api/admin/users/${editingUser.user_id}`, {
        plan: planValue,
        quota: {
          perizia_scans_remaining: parseInt(quotaValues.perizia_scans_remaining, 10),
          image_scans_remaining: parseInt(quotaValues.image_scans_remaining, 10),
          assistant_messages_remaining: parseInt(quotaValues.assistant_messages_remaining, 10)
        }
      }, { withCredentials: true });
      toast.success('Piano/quota aggiornati');
      setEditingUser(null);
      fetchUsers();
    } catch (error) {
      toast.error('Errore aggiornamento piano/quota');
    }
  };

  const handleNotesSave = async () => {
    if (!notesUser) return;
    try {
      await axios.put(`${API_URL}/api/admin/users/${notesUser.user_id}/notes`, {
        note: notesValue,
        tags: notesTags.split(',').map(t => t.trim()).filter(Boolean),
        internal_status: notesStatus
      }, { withCredentials: true });
      toast.success('Note aggiornate');
      setNotesUser(null);
      fetchUsers();
    } catch (error) {
      toast.error('Errore aggiornamento note');
    }
  };

  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  return (
    <AdminLayout title="Utenti" subtitle="Gestione clienti, piani e quote">
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4 mb-6">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
          <Input
            placeholder="Search email, name, user_id"
            value={q}
            onChange={(e) => { setQ(e.target.value); setPage(1); }}
            className="bg-zinc-950 border-zinc-800 text-zinc-100"
          />
          <select
            value={plan}
            onChange={(e) => { setPlan(e.target.value); setPage(1); }}
            className="h-9 rounded-md border border-zinc-800 bg-zinc-950 px-3 text-sm text-zinc-100"
          >
            <option value="">All Plans</option>
            <option value="free">Free</option>
            <option value="pro">Pro</option>
            <option value="enterprise">Enterprise</option>
          </select>
          <select
            value={sort}
            onChange={(e) => setSort(e.target.value)}
            className="h-9 rounded-md border border-zinc-800 bg-zinc-950 px-3 text-sm text-zinc-100"
          >
            <option value="created_at">Created</option>
            <option value="last_active_at">Last Active</option>
            <option value="usage_30d.perizie">Usage 30d (Perizie)</option>
          </select>
          <select
            value={order}
            onChange={(e) => setOrder(e.target.value)}
            className="h-9 rounded-md border border-zinc-800 bg-zinc-950 px-3 text-sm text-zinc-100"
          >
            <option value="desc">Desc</option>
            <option value="asc">Asc</option>
          </select>
        </div>
      </div>

      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
        {loading ? (
          <div className="text-zinc-400 font-mono text-sm">Loading...</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-zinc-500 border-b border-zinc-800">
                  <th className="py-2">Email</th>
                  <th className="py-2">Name</th>
                  <th className="py-2">Plan</th>
                  <th className="py-2">Created</th>
                  <th className="py-2">Last Active</th>
                  <th className="py-2">Usage 30d (P/I/A)</th>
                  <th className="py-2">Lifetime (P/I/A)</th>
                  <th className="py-2">Quota (P/I/A)</th>
                  <th className="py-2">Internal Status</th>
                  <th className="py-2"></th>
                </tr>
              </thead>
              <tbody>
                {items.map((u) => (
                  <tr key={u.user_id} className="border-b border-zinc-800 text-zinc-200">
                    <td className="py-2">{u.email}</td>
                    <td className="py-2">{u.name}</td>
                    <td className="py-2 capitalize">{u.plan}</td>
                    <td className="py-2">{u.created_at ? new Date(u.created_at).toLocaleDateString() : '-'}</td>
                    <td className="py-2">{u.last_active_at ? new Date(u.last_active_at).toLocaleString() : '-'}</td>
                    <td className="py-2">{u.usage_30d?.perizie || 0}/{u.usage_30d?.images || 0}/{u.usage_30d?.assistant_qas || 0}</td>
                    <td className="py-2">{u.lifetime?.perizie || 0}/{u.lifetime?.images || 0}/{u.lifetime?.assistant_qas || 0}</td>
                    <td className="py-2">{u.quota?.perizia_scans_remaining || 0}/{u.quota?.image_scans_remaining || 0}/{u.quota?.assistant_messages_remaining || 0}</td>
                    <td className="py-2">{u.notes?.internal_status || '-'}</td>
                    <td className="py-2 text-right">
                      <div className="flex gap-2 justify-end">
                        <Button variant="outline" className="border-zinc-700 text-zinc-300" onClick={() => navigate(`/admin/users/${u.user_id}`)}>
                          Details
                        </Button>
                        <Button variant="outline" className="border-zinc-700 text-zinc-300" onClick={() => openEditModal(u)}>
                          Edit Plan/Quota
                        </Button>
                        <Button variant="outline" className="border-zinc-700 text-zinc-300" onClick={() => openNotesModal(u)}>
                          Edit Notes
                        </Button>
                      </div>
                    </td>
                  </tr>
                ))}
                {items.length === 0 && (
                  <tr>
                    <td colSpan="10" className="py-6 text-center text-zinc-500">Nessun utente</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="flex items-center justify-between mt-4">
        <p className="text-sm text-zinc-500">Page {page} / {totalPages} (Total {total})</p>
        <div className="flex gap-2">
          <Button variant="outline" className="border-zinc-700 text-zinc-300" disabled={page <= 1} onClick={() => setPage(page - 1)}>Prev</Button>
          <Button variant="outline" className="border-zinc-700 text-zinc-300" disabled={page >= totalPages} onClick={() => setPage(page + 1)}>Next</Button>
        </div>
      </div>

      {editingUser && (
        <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50">
          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6 w-full max-w-lg">
            <h3 className="text-lg font-serif font-bold text-zinc-100 mb-4">Edit Plan/Quota</h3>
            <div className="space-y-3">
              <select
                value={planValue}
                onChange={(e) => setPlanValue(e.target.value)}
                className="h-9 w-full rounded-md border border-zinc-800 bg-zinc-950 px-3 text-sm text-zinc-100"
              >
                <option value="free">Free</option>
                <option value="pro">Pro</option>
                <option value="enterprise">Enterprise</option>
              </select>
              <Input
                type="number"
                value={quotaValues.perizia_scans_remaining}
                onChange={(e) => setQuotaValues({ ...quotaValues, perizia_scans_remaining: e.target.value })}
                className="bg-zinc-950 border-zinc-800 text-zinc-100"
                placeholder="Perizia scans remaining"
              />
              <Input
                type="number"
                value={quotaValues.image_scans_remaining}
                onChange={(e) => setQuotaValues({ ...quotaValues, image_scans_remaining: e.target.value })}
                className="bg-zinc-950 border-zinc-800 text-zinc-100"
                placeholder="Image scans remaining"
              />
              <Input
                type="number"
                value={quotaValues.assistant_messages_remaining}
                onChange={(e) => setQuotaValues({ ...quotaValues, assistant_messages_remaining: e.target.value })}
                className="bg-zinc-950 border-zinc-800 text-zinc-100"
                placeholder="Assistant messages remaining"
              />
            </div>
            <div className="flex justify-end gap-2 mt-6">
              <Button variant="outline" className="border-zinc-700 text-zinc-300" onClick={() => setEditingUser(null)}>Cancel</Button>
              <Button className="bg-gold text-zinc-950 hover:bg-gold-dim" onClick={handlePlanSave}>Save</Button>
            </div>
          </div>
        </div>
      )}

      {notesUser && (
        <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50">
          <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-6 w-full max-w-lg">
            <h3 className="text-lg font-serif font-bold text-zinc-100 mb-4">Edit Notes</h3>
            <div className="space-y-3">
              <select
                value={notesStatus}
                onChange={(e) => setNotesStatus(e.target.value)}
                className="h-9 w-full rounded-md border border-zinc-800 bg-zinc-950 px-3 text-sm text-zinc-100"
              >
                <option value="OK">OK</option>
                <option value="WATCH">WATCH</option>
                <option value="BLOCKED">BLOCKED</option>
              </select>
              <Input
                value={notesTags}
                onChange={(e) => setNotesTags(e.target.value)}
                className="bg-zinc-950 border-zinc-800 text-zinc-100"
                placeholder="Tags (comma separated)"
              />
              <textarea
                value={notesValue}
                onChange={(e) => setNotesValue(e.target.value)}
                className="w-full h-28 rounded-md border border-zinc-800 bg-zinc-950 px-3 py-2 text-sm text-zinc-100"
                placeholder="Internal note"
              />
            </div>
            <div className="flex justify-end gap-2 mt-6">
              <Button variant="outline" className="border-zinc-700 text-zinc-300" onClick={() => setNotesUser(null)}>Cancel</Button>
              <Button className="bg-gold text-zinc-950 hover:bg-gold-dim" onClick={handleNotesSave}>Save</Button>
            </div>
          </div>
        </div>
      )}
    </AdminLayout>
  );
};

export default AdminUsers;
