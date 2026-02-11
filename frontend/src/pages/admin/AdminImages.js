import React, { useCallback, useEffect, useState } from 'react';
import axios from 'axios';
import AdminLayout from './AdminLayout';
import { Input } from '../../components/ui/input';
import { Button } from '../../components/ui/button';
import { toast } from 'sonner';

const API_URL = process.env.REACT_APP_BACKEND_URL;

const AdminImages = () => {
  const [items, setItems] = useState([]);
  const [q, setQ] = useState('');
  const [dateFrom, setDateFrom] = useState('');
  const [dateTo, setDateTo] = useState('');
  const [page, setPage] = useState(1);
  const [pageSize] = useState(20);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const response = await axios.get(`${API_URL}/api/admin/images`, {
        params: {
          q: q || undefined,
          date_from: dateFrom || undefined,
          date_to: dateTo || undefined,
          page,
          page_size: pageSize
        },
        withCredentials: true
      });
      setItems(response.data.items || []);
      setTotal(response.data.total || 0);
    } catch (error) {
      toast.error('Errore nel caricamento immagini');
    } finally {
      setLoading(false);
    }
  }, [q, dateFrom, dateTo, page, pageSize]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  return (
    <AdminLayout title="Immagini" subtitle="Analisi immagini (lightweight)">
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4 mb-6">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
          <Input
            placeholder="Search case_id, run_id, forensics_id"
            value={q}
            onChange={(e) => { setQ(e.target.value); setPage(1); }}
            className="bg-zinc-950 border-zinc-800 text-zinc-100"
          />
          <Input
            type="date"
            value={dateFrom}
            onChange={(e) => { setDateFrom(e.target.value); setPage(1); }}
            className="bg-zinc-950 border-zinc-800 text-zinc-100"
          />
          <Input
            type="date"
            value={dateTo}
            onChange={(e) => { setDateTo(e.target.value); setPage(1); }}
            className="bg-zinc-950 border-zinc-800 text-zinc-100"
          />
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
                  <th className="py-2">Forensics ID</th>
                  <th className="py-2">Email</th>
                  <th className="py-2">Case ID</th>
                  <th className="py-2">Run ID</th>
                  <th className="py-2">Revision</th>
                  <th className="py-2">Image Count</th>
                  <th className="py-2">Created</th>
                </tr>
              </thead>
              <tbody>
                {items.map((item) => (
                  <tr key={item.forensics_id} className="border-b border-zinc-800 text-zinc-200">
                    <td className="py-2">{item.forensics_id}</td>
                    <td className="py-2">{item.email || '-'}</td>
                    <td className="py-2">{item.case_id}</td>
                    <td className="py-2">{item.run_id}</td>
                    <td className="py-2">{item.revision ?? '-'}</td>
                    <td className="py-2">{item.image_count ?? 0}</td>
                    <td className="py-2">{item.created_at ? new Date(item.created_at).toLocaleString() : '-'}</td>
                  </tr>
                ))}
                {items.length === 0 && (
                  <tr>
                    <td colSpan="7" className="py-6 text-center text-zinc-500">Nessuna analisi immagine</td>
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

export default AdminImages;
