import React, { useEffect, useState } from 'react';
import axios from 'axios';
import AdminLayout from './AdminLayout';
import { Input } from '../../components/ui/input';
import { Button } from '../../components/ui/button';
import { toast } from 'sonner';

const API_URL = process.env.REACT_APP_BACKEND_URL;

const AdminTransactions = () => {
  const [items, setItems] = useState([]);
  const [q, setQ] = useState('');
  const [status, setStatus] = useState('');
  const [page, setPage] = useState(1);
  const [pageSize] = useState(20);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);

  const fetchData = async () => {
    setLoading(true);
    try {
      const response = await axios.get(`${API_URL}/api/admin/transactions`, {
        params: { q: q || undefined, status: status || undefined, page, page_size: pageSize },
        withCredentials: true
      });
      setItems(response.data.items || []);
      setTotal(response.data.total || 0);
    } catch (error) {
      toast.error('Errore nel caricamento transazioni');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, [q, status, page]);

  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  return (
    <AdminLayout title="Transazioni" subtitle="Pagamenti e sessioni">
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4 mb-6">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          <Input
            placeholder="Search session_id, transaction_id, user_id"
            value={q}
            onChange={(e) => { setQ(e.target.value); setPage(1); }}
            className="bg-zinc-950 border-zinc-800 text-zinc-100"
          />
          <select
            value={status}
            onChange={(e) => { setStatus(e.target.value); setPage(1); }}
            className="h-9 rounded-md border border-zinc-800 bg-zinc-950 px-3 text-sm text-zinc-100"
          >
            <option value="">All Status</option>
            <option value="paid">paid</option>
            <option value="unpaid">unpaid</option>
            <option value="pending">pending</option>
            <option value="expired">expired</option>
          </select>
          <Button variant="outline" className="border-zinc-700 text-zinc-300" onClick={() => fetchData()}>
            Refresh
          </Button>
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
                  <th className="py-2">Transaction ID</th>
                  <th className="py-2">Session ID</th>
                  <th className="py-2">Email</th>
                  <th className="py-2">Plan</th>
                  <th className="py-2">Status</th>
                  <th className="py-2">Payment Status</th>
                  <th className="py-2">Amount</th>
                  <th className="py-2">Created</th>
                </tr>
              </thead>
              <tbody>
                {items.map((item) => (
                  <tr key={item.transaction_id} className="border-b border-zinc-800 text-zinc-200">
                    <td className="py-2">{item.transaction_id}</td>
                    <td className="py-2">{item.session_id}</td>
                    <td className="py-2">{item.email || '-'}</td>
                    <td className="py-2">{item.plan_id || '-'}</td>
                    <td className="py-2">{item.status || '-'}</td>
                    <td className="py-2">{item.payment_status || '-'}</td>
                    <td className="py-2">{item.amount} {item.currency?.toUpperCase()}</td>
                    <td className="py-2">{item.created_at ? new Date(item.created_at).toLocaleString() : '-'}</td>
                  </tr>
                ))}
                {items.length === 0 && (
                  <tr>
                    <td colSpan="8" className="py-6 text-center text-zinc-500">Nessuna transazione</td>
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
    </AdminLayout>
  );
};

export default AdminTransactions;
