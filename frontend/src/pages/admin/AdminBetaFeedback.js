import React, { useCallback, useEffect, useState } from 'react';
import axios from 'axios';
import AdminLayout from './AdminLayout';
import { Input } from '../../components/ui/input';
import { Button } from '../../components/ui/button';
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from '../../components/ui/select';
import { toast } from 'sonner';
import { X, Download } from 'lucide-react';
import {
  SECTION_KEYS, FEEDBACK_TYPES, PRIORITIES,
} from '../../components/TechnicalFeedbackModal';

const API_URL = process.env.REACT_APP_BACKEND_URL;

const STATUSES = [
  { value: 'new', label: 'Nuovo' },
  { value: 'reviewed', label: 'Revisionato' },
  { value: 'accepted', label: 'Accettato' },
  { value: 'rejected', label: 'Rifiutato' },
  { value: 'fixed', label: 'Corretto' },
  { value: 'needs_clarification', label: 'Da chiarire' },
];

const ERROR_CATEGORY_GROUPS = [
  'over_classification', 'under_classification', 'missing_information',
  'wrong_source_page', 'wrong_extracted_value', 'duplicate_output',
];

const ALL = '__all__';

const labelFor = (list, value) => list.find((o) => o.value === value)?.label || value || '—';
const fmt = (v) => (v ? new Date(v).toLocaleString('it-IT') : '—');

const FilterSelect = ({ value, onValueChange, placeholder, options }) => (
  <Select value={value} onValueChange={onValueChange}>
    <SelectTrigger className="bg-zinc-950 border-zinc-800 text-zinc-100">
      <SelectValue placeholder={placeholder} />
    </SelectTrigger>
    <SelectContent className="bg-zinc-900 border-zinc-700 text-zinc-100 max-h-72">
      <SelectItem value={ALL} className="text-zinc-200 focus:bg-zinc-800">{placeholder}</SelectItem>
      {options.map((o) => (
        <SelectItem key={o.value} value={o.value} className="text-zinc-200 focus:bg-zinc-800">{o.label}</SelectItem>
      ))}
    </SelectContent>
  </Select>
);

const MetricCard = ({ label, value, accent = 'text-zinc-100' }) => (
  <div className="rounded-xl border border-zinc-800 bg-zinc-900 p-4">
    <p className="text-[10px] font-mono uppercase tracking-wider text-zinc-500 mb-2">{label}</p>
    <p className={`text-2xl font-mono font-bold ${accent}`}>{value ?? 0}</p>
  </div>
);

const AdminBetaFeedback = () => {
  const [items, setItems] = useState([]);
  const [metrics, setMetrics] = useState({});
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [pageSize] = useState(50);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState(null);
  const [savingStatus, setSavingStatus] = useState(false);

  const [filters, setFilters] = useState({
    user_email: '', analysis_id: '', section_key: ALL, feedback_type: ALL,
    priority: ALL, status: ALL, date_from: '', date_to: '',
  });

  const buildParams = useCallback((extra = {}) => {
    const p = {};
    if (filters.user_email) p.user_email = filters.user_email;
    if (filters.analysis_id) p.analysis_id = filters.analysis_id;
    if (filters.section_key !== ALL) p.section_key = filters.section_key;
    if (filters.feedback_type !== ALL) p.feedback_type = filters.feedback_type;
    if (filters.priority !== ALL) p.priority = filters.priority;
    if (filters.status !== ALL) p.status = filters.status;
    if (filters.date_from) p.date_from = filters.date_from;
    if (filters.date_to) p.date_to = filters.date_to;
    return { ...p, ...extra };
  }, [filters]);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const res = await axios.get(`${API_URL}/api/admin/beta-feedback`, {
        params: buildParams({ page, page_size: pageSize }),
        withCredentials: true,
      });
      setItems(res.data.items || []);
      setTotal(res.data.total || 0);
      setMetrics(res.data.metrics || {});
    } catch (err) {
      toast.error('Errore nel caricamento dei feedback');
    } finally {
      setLoading(false);
    }
  }, [buildParams, page, pageSize]);

  useEffect(() => { fetchData(); }, [fetchData]);

  const setFilter = (key, val) => { setFilters((f) => ({ ...f, [key]: val })); setPage(1); };

  const handleExport = async (format) => {
    try {
      const res = await axios.get(`${API_URL}/api/admin/beta-feedback/export`, {
        params: buildParams({ format }),
        withCredentials: true,
        responseType: format === 'json' ? 'json' : 'blob',
      });
      const blob = format === 'json'
        ? new Blob([JSON.stringify(res.data, null, 2)], { type: 'application/json' })
        : res.data;
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `beta_feedback_export.${format === 'json' ? 'json' : 'csv'}`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
    } catch (err) {
      toast.error('Errore durante l’export');
    }
  };

  const updateStatus = async (feedbackId, status, adminNotes) => {
    setSavingStatus(true);
    try {
      const res = await axios.patch(`${API_URL}/api/admin/beta-feedback/${feedbackId}`, {
        status, admin_notes: adminNotes,
      }, { withCredentials: true });
      toast.success('Feedback aggiornato');
      setSelected(res.data.feedback);
      fetchData();
    } catch (err) {
      toast.error('Errore aggiornamento stato');
    } finally {
      setSavingStatus(false);
    }
  };

  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const errorBreakdown = metrics.error_category_breakdown || {};

  return (
    <AdminLayout title="Beta Feedback" subtitle="Centro di controllo del feedback esperto dei beta partner">
      {/* A. Top metrics */}
      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-3 mb-6">
        <MetricCard label="Totali" value={metrics.total} accent="text-gold" />
        <MetricCard label="Nuovi" value={metrics.new} accent="text-amber-400" />
        <MetricCard label="Accettati" value={metrics.accepted} accent="text-emerald-400" />
        <MetricCard label="Alta/Bloccante" value={metrics.high_priority} accent="text-red-400" />
        <div className="rounded-xl border border-zinc-800 bg-zinc-900 p-4">
          <p className="text-[10px] font-mono uppercase tracking-wider text-zinc-500 mb-2">Top categoria errore</p>
          <p className="text-sm font-medium text-zinc-200 break-words">{metrics.top_error_category || '—'}</p>
        </div>
        <div className="rounded-xl border border-zinc-800 bg-zinc-900 p-4">
          <p className="text-[10px] font-mono uppercase tracking-wider text-zinc-500 mb-2">Sezione critica</p>
          <p className="text-sm font-medium text-zinc-200 break-words">{labelFor(SECTION_KEYS, metrics.top_problematic_section)}</p>
        </div>
      </div>

      {/* B. Filters */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4 mb-4">
        <div className="grid grid-cols-1 md:grid-cols-3 xl:grid-cols-4 gap-3">
          <Input placeholder="Email tester" value={filters.user_email}
            onChange={(e) => setFilter('user_email', e.target.value)}
            className="bg-zinc-950 border-zinc-800 text-zinc-100" />
          <Input placeholder="Analysis ID" value={filters.analysis_id}
            onChange={(e) => setFilter('analysis_id', e.target.value)}
            className="bg-zinc-950 border-zinc-800 text-zinc-100" />
          <FilterSelect value={filters.section_key} onValueChange={(v) => setFilter('section_key', v)} placeholder="Tutte le sezioni" options={SECTION_KEYS} />
          <FilterSelect value={filters.feedback_type} onValueChange={(v) => setFilter('feedback_type', v)} placeholder="Tutti i tipi" options={FEEDBACK_TYPES} />
          <FilterSelect value={filters.priority} onValueChange={(v) => setFilter('priority', v)} placeholder="Tutte le priorità" options={PRIORITIES} />
          <FilterSelect value={filters.status} onValueChange={(v) => setFilter('status', v)} placeholder="Tutti gli stati" options={STATUSES} />
          <Input type="date" value={filters.date_from} onChange={(e) => setFilter('date_from', e.target.value)} className="bg-zinc-950 border-zinc-800 text-zinc-100" />
          <Input type="date" value={filters.date_to} onChange={(e) => setFilter('date_to', e.target.value)} className="bg-zinc-950 border-zinc-800 text-zinc-100" />
        </div>
        <div className="flex flex-wrap gap-2 mt-3">
          <Button variant="outline" className="border-zinc-700 text-zinc-300" onClick={() => fetchData()}>Aggiorna</Button>
          <div className="flex-1" />
          <Button variant="outline" className="border-zinc-700 text-zinc-200" onClick={() => handleExport('json')}>
            <Download className="w-4 h-4 mr-1" /> Export JSON
          </Button>
          <Button variant="outline" className="border-zinc-700 text-zinc-200" onClick={() => handleExport('csv')}>
            <Download className="w-4 h-4 mr-1" /> Export CSV
          </Button>
        </div>
      </div>

      {/* C. Feedback table */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4 mb-6">
        {loading ? (
          <div className="text-zinc-400 font-mono text-sm py-6">Loading...</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-zinc-500 border-b border-zinc-800 text-[11px] font-mono uppercase tracking-wider">
                  <th className="py-2 px-2">Data</th>
                  <th className="py-2 px-2">Tester</th>
                  <th className="py-2 px-2">File/Report</th>
                  <th className="py-2 px-2">Sezione</th>
                  <th className="py-2 px-2">Elemento</th>
                  <th className="py-2 px-2">Tipo</th>
                  <th className="py-2 px-2">Priorità</th>
                  <th className="py-2 px-2">Osservazione</th>
                  <th className="py-2 px-2">Stato</th>
                </tr>
              </thead>
              <tbody>
                {items.map((it) => (
                  <tr key={it.id} className="border-b border-zinc-800/60 text-zinc-200 hover:bg-zinc-800/40 cursor-pointer"
                    onClick={() => setSelected(it)} data-testid="beta-fb-row">
                    <td className="py-2 px-2 text-zinc-400 whitespace-nowrap">{fmt(it.created_at)}</td>
                    <td className="py-2 px-2 max-w-[150px] truncate">{it.user_email}</td>
                    <td className="py-2 px-2 max-w-[150px] truncate">{it.file_name || it.analysis_id || '—'}</td>
                    <td className="py-2 px-2">{it.section_label_it}</td>
                    <td className="py-2 px-2 max-w-[140px] truncate">{it.item_reference?.item_title || '—'}</td>
                    <td className="py-2 px-2">{labelFor(FEEDBACK_TYPES, it.feedback_type)}</td>
                    <td className="py-2 px-2">{labelFor(PRIORITIES, it.priority)}</td>
                    <td className="py-2 px-2 max-w-[200px] truncate text-zinc-400">{it.expert_comment}</td>
                    <td className="py-2 px-2">{labelFor(STATUSES, it.status)}</td>
                  </tr>
                ))}
                {items.length === 0 && (
                  <tr><td colSpan="9" className="py-6 text-center text-zinc-500">Nessun feedback</td></tr>
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

      {/* F. Learning dataset summary */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4 mb-6">
        <h3 className="text-sm font-mono uppercase tracking-wider text-zinc-500 mb-3">Sintesi dataset di apprendimento</h3>
        <div className="grid grid-cols-2 md:grid-cols-4 xl:grid-cols-7 gap-3">
          {ERROR_CATEGORY_GROUPS.map((cat) => (
            <div key={cat} className="rounded-lg border border-zinc-800 bg-zinc-950/50 p-3">
              <p className="text-[10px] font-mono text-zinc-500 mb-1 break-words">{cat}</p>
              <p className="text-lg font-mono font-bold text-zinc-200">{errorBreakdown[cat] || 0}</p>
            </div>
          ))}
          <div className="rounded-lg border border-zinc-800 bg-zinc-950/50 p-3">
            <p className="text-[10px] font-mono text-zinc-500 mb-1 break-words">confirmed_correct</p>
            <p className="text-lg font-mono font-bold text-emerald-400">{errorBreakdown['low_utility'] != null ? '' : ''}{(metrics.total || 0) - Object.values(errorBreakdown).reduce((a, b) => a + b, 0)}</p>
          </div>
        </div>
      </div>

      {/* D. Detail drawer */}
      {selected && (
        <div className="fixed inset-0 z-50 flex justify-end">
          <div className="absolute inset-0 bg-black/60" onClick={() => setSelected(null)} />
          <div className="relative h-full w-full max-w-xl overflow-y-auto bg-zinc-900 border-l border-zinc-800 p-6 shadow-2xl">
            <button onClick={() => setSelected(null)} className="absolute top-4 right-4 text-zinc-500 hover:text-zinc-200">
              <X className="w-5 h-5" />
            </button>
            <h3 className="text-lg font-serif font-bold text-zinc-100 mb-1">Dettaglio feedback</h3>
            <p className="text-xs font-mono text-zinc-500 mb-5 break-all">{selected.id}</p>

            <DetailRow label="Tester" value={`${selected.user_email} (${selected.beta_partner_name || ''})`} />
            <DetailRow label="Report" value={selected.file_name || selected.analysis_id} />
            <DetailRow label="Sezione" value={selected.section_label_it} />
            <DetailRow label="Tipo" value={labelFor(FEEDBACK_TYPES, selected.feedback_type)} />
            <DetailRow label="Priorità" value={labelFor(PRIORITIES, selected.priority)} />
            <DetailRow label="Pagina" value={selected.page_reference || selected.item_reference?.page_reference || '—'} />

            <Section title="Output AI originale">
              <pre className="text-xs text-zinc-400 whitespace-pre-wrap break-words bg-zinc-950 rounded-lg p-3 border border-zinc-800">
{JSON.stringify(selected.original_ai_output || {}, null, 2)}
              </pre>
            </Section>

            <Section title="Osservazione tecnica">
              <p className="text-sm text-zinc-200 whitespace-pre-wrap">{selected.expert_comment}</p>
            </Section>
            {selected.expected_correction && (
              <Section title="Correzione suggerita">
                <p className="text-sm text-zinc-200 whitespace-pre-wrap">{selected.expected_correction}</p>
              </Section>
            )}
            <Section title="Classificazione attesa">
              <p className="text-sm text-zinc-300">{selected.expected_classification || '—'}</p>
            </Section>
            <Section title="Evidence / pagina">
              <p className="text-sm text-zinc-300">{selected.item_reference?.evidence_quote || selected.original_ai_output?.evidence || '—'}</p>
            </Section>
            <Section title="Learning label">
              <pre className="text-xs text-zinc-400 whitespace-pre-wrap bg-zinc-950 rounded-lg p-3 border border-zinc-800">
{JSON.stringify(selected.learning_label || {}, null, 2)}
              </pre>
            </Section>

            <Section title="Stato">
              <div className="flex flex-wrap gap-2 mb-3">
                {STATUSES.map((s) => (
                  <Button key={s.value} size="sm"
                    variant={selected.status === s.value ? 'default' : 'outline'}
                    className={selected.status === s.value ? 'bg-gold text-zinc-950' : 'border-zinc-700 text-zinc-300'}
                    disabled={savingStatus}
                    onClick={() => updateStatus(selected.id, s.value, selected.admin_notes)}>
                    {s.label}
                  </Button>
                ))}
              </div>
              <AdminNotesEditor selected={selected} savingStatus={savingStatus} onSave={updateStatus} />
              {selected.reviewed_by && (
                <p className="text-xs text-zinc-500 mt-2">Revisionato da {selected.reviewed_by} • {fmt(selected.reviewed_at)}</p>
              )}
            </Section>
          </div>
        </div>
      )}
    </AdminLayout>
  );
};

const DetailRow = ({ label, value }) => (
  <div className="flex items-start justify-between gap-4 py-2 border-b border-zinc-800/60">
    <span className="text-xs font-mono uppercase tracking-wider text-zinc-500">{label}</span>
    <span className="text-sm text-zinc-200 text-right break-words max-w-[60%]">{value || '—'}</span>
  </div>
);

const Section = ({ title, children }) => (
  <div className="mt-5">
    <p className="text-xs font-mono uppercase tracking-wider text-zinc-500 mb-2">{title}</p>
    {children}
  </div>
);

const AdminNotesEditor = ({ selected, savingStatus, onSave }) => {
  const [notes, setNotes] = useState(selected.admin_notes || '');
  useEffect(() => { setNotes(selected.admin_notes || ''); }, [selected.id]); // eslint-disable-line
  return (
    <div>
      <Input placeholder="Note admin" value={notes} onChange={(e) => setNotes(e.target.value)}
        className="bg-zinc-950 border-zinc-800 text-zinc-100 mb-2" />
      <Button size="sm" variant="outline" className="border-zinc-700 text-zinc-200"
        disabled={savingStatus} onClick={() => onSave(selected.id, selected.status, notes)}>
        Salva note
      </Button>
    </div>
  );
};

export default AdminBetaFeedback;
