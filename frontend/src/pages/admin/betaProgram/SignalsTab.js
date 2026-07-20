import React, { useCallback, useEffect, useState } from 'react';
import axios from 'axios';
import { toast } from 'sonner';
import { Input } from '../../../components/ui/input';
import { Button } from '../../../components/ui/button';
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from '../../../components/ui/select';

const API_URL = process.env.REACT_APP_BACKEND_URL;
const ALL = '__all__';

// Operational signals — derived by the system, NOT statements made by the tester.
const SIGNAL_LABELS = {
  REPORT_READY: 'Report pronto',
  VERIFICATION_REQUIRED: 'Verifica richiesta',
  SERVICE_BUSY: 'Servizio occupato',
  SERVICE_UNAVAILABLE: 'Servizio non disponibile',
  DOCUMENT_NOT_READABLE: 'PDF illeggibile',
  LOT_REPORT_REUSED: 'Report riutilizzato',
  LOT_JOB_DEDUPLICATED: 'Job deduplicato',
  LOT_RERUN_FORCED: 'Rigenerazione forzata',
  FAILED_RERUN_SAFE_REPORT_PRESERVED: 'Rigenerazione fallita (report preservato)',
  CONFIRMATION_REQUIRED: 'Conferma richiesta',
  CONFIRMATION_COMPLETED: 'Conferma completata',
};
const SIGNAL_OPTIONS = Object.entries(SIGNAL_LABELS).map(([value, label]) => ({ value, label }));

const fmt = (v) => (v ? new Date(v).toLocaleString('it-IT') : '—');

const SignalsTab = ({ active }) => {
  const [items, setItems] = useState([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [pageSize] = useState(50);
  const [loading, setLoading] = useState(true);
  const [signalFilter, setSignalFilter] = useState(ALL);
  const [analysisId, setAnalysisId] = useState('');

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const params = { page, page_size: pageSize };
      if (signalFilter !== ALL) params.signal = signalFilter;
      if (analysisId.trim()) params.analysis_id = analysisId.trim();
      const res = await axios.get(`${API_URL}/api/admin/beta-program/signals`, { params, withCredentials: true });
      setItems(res.data.items || []);
      setTotal(res.data.total || 0);
    } catch (err) {
      toast.error('Errore nel caricamento dei segnali');
    } finally {
      setLoading(false);
    }
  }, [page, pageSize, signalFilter, analysisId]);

  useEffect(() => { if (active) fetchData(); }, [active, fetchData]);

  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  return (
    <div data-testid="beta-signals">
      <div className="mb-4 rounded-lg border border-zinc-800 bg-zinc-950/50 p-3">
        <p className="text-[11px] text-zinc-500">
          I segnali sono eventi operativi registrati automaticamente dal sistema — non sono dichiarazioni del tester.
        </p>
      </div>

      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4 mb-4">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          <Select value={signalFilter} onValueChange={(v) => { setSignalFilter(v); setPage(1); }}>
            <SelectTrigger className="bg-zinc-950 border-zinc-800 text-zinc-100"><SelectValue placeholder="Tutti i segnali" /></SelectTrigger>
            <SelectContent className="bg-zinc-900 border-zinc-700 text-zinc-100 max-h-72">
              <SelectItem value={ALL} className="text-zinc-200 focus:bg-zinc-800">Tutti i segnali</SelectItem>
              {SIGNAL_OPTIONS.map((o) => (
                <SelectItem key={o.value} value={o.value} className="text-zinc-200 focus:bg-zinc-800">{o.label}</SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Input placeholder="Analysis ID" value={analysisId}
            onChange={(e) => { setAnalysisId(e.target.value); setPage(1); }}
            className="bg-zinc-950 border-zinc-800 text-zinc-100" />
          <Button variant="outline" className="border-zinc-700 text-zinc-300" onClick={fetchData}>Aggiorna</Button>
        </div>
      </div>

      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
        {loading ? (
          <div className="text-zinc-400 font-mono text-sm py-6">Loading...</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-zinc-500 border-b border-zinc-800 text-[11px] font-mono uppercase tracking-wider">
                  <th className="py-2 px-2">Data</th>
                  <th className="py-2 px-2">Segnale</th>
                  <th className="py-2 px-2">Analisi</th>
                  <th className="py-2 px-2">Lotto</th>
                  <th className="py-2 px-2">Stato</th>
                  <th className="py-2 px-2">Durata (s)</th>
                </tr>
              </thead>
              <tbody>
                {items.map((it) => (
                  <tr key={it.event_id} className="border-b border-zinc-800/60 text-zinc-200" data-testid="beta-signal-row">
                    <td className="py-2 px-2 text-zinc-400 whitespace-nowrap">{fmt(it.created_at)}</td>
                    <td className="py-2 px-2">{SIGNAL_LABELS[it.event_type] || it.event_type}</td>
                    <td className="py-2 px-2 max-w-[180px] truncate font-mono text-zinc-400">{it.analysis_id}</td>
                    <td className="py-2 px-2 text-zinc-400">{it.lot_id || '—'}</td>
                    <td className="py-2 px-2 text-zinc-400">{it.status || '—'}</td>
                    <td className="py-2 px-2 text-zinc-400">{it.duration_seconds != null ? it.duration_seconds : '—'}</td>
                  </tr>
                ))}
                {items.length === 0 && (
                  <tr><td colSpan="6" className="py-6 text-center text-zinc-500">Nessun segnale</td></tr>
                )}
              </tbody>
            </table>
          </div>
        )}
        <div className="mt-4 flex items-center justify-between">
          <p className="text-sm text-zinc-500">Pagina {page} / {totalPages} (Totale {total})</p>
          <div className="flex gap-2">
            <Button variant="outline" className="border-zinc-700 text-zinc-300" disabled={page <= 1} onClick={() => setPage(page - 1)}>Prev</Button>
            <Button variant="outline" className="border-zinc-700 text-zinc-300" disabled={page >= totalPages} onClick={() => setPage(page + 1)}>Next</Button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default SignalsTab;
