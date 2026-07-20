import React, { useCallback, useEffect, useState } from 'react';
import axios from 'axios';
import { toast } from 'sonner';
import { X, Download } from 'lucide-react';
import { Input } from '../../../components/ui/input';
import { Button } from '../../../components/ui/button';
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from '../../../components/ui/select';
import {
  SECTION_KEYS, FEEDBACK_TYPES, PRIORITIES,
} from '../../../components/TechnicalFeedbackModal';

const API_URL = process.env.REACT_APP_BACKEND_URL;
const ALL = '__all__';

const STATUSES = [
  { value: 'new', label: 'Nuovo' },
  { value: 'reviewed', label: 'In valutazione' },
  { value: 'accepted', label: 'Pianificato' },
  { value: 'fixed', label: 'Risolto' },
  { value: 'rejected', label: 'Non applicabile' },
  { value: 'needs_clarification', label: 'Da chiarire' },
];

// Owner interpretation category — separate from the tester's own feedback_type,
// which is never overwritten.
const OWNER_CATEGORIES = [
  { value: 'accuratezza_report', label: 'Accuratezza del report' },
  { value: 'informazione_mancante', label: 'Informazione mancante' },
  { value: 'informazione_confusa', label: 'Informazione confusa' },
  { value: 'esperienza_utente', label: 'Esperienza utente' },
  { value: 'velocita', label: 'Velocità' },
  { value: 'selezione_lotti', label: 'Selezione lotti' },
  { value: 'conferme', label: 'Conferme' },
  { value: 'problema_tecnico', label: 'Problema tecnico' },
  { value: 'feedback_positivo', label: 'Feedback positivo' },
  { value: 'richiesta_funzionalita', label: 'Richiesta funzionalità' },
  { value: 'altro', label: 'Altro' },
];
const OWNER_PRIORITIES = [
  { value: 'bassa', label: 'Bassa' },
  { value: 'media', label: 'Media' },
  { value: 'alta', label: 'Alta' },
  { value: 'bloccante', label: 'Critica' },
];

const labelFor = (list, value) => list.find((o) => o.value === value)?.label || value || '—';
const fmt = (v) => (v ? new Date(v).toLocaleString('it-IT') : '—');

const FilterSelect = ({ value, onValueChange, placeholder, options }) => (
  <Select value={value} onValueChange={onValueChange}>
    <SelectTrigger className="bg-zinc-950 border-zinc-800 text-zinc-100"><SelectValue placeholder={placeholder} /></SelectTrigger>
    <SelectContent className="bg-zinc-900 border-zinc-700 text-zinc-100 max-h-72">
      <SelectItem value={ALL} className="text-zinc-200 focus:bg-zinc-800">{placeholder}</SelectItem>
      {options.map((o) => (
        <SelectItem key={o.value} value={o.value} className="text-zinc-200 focus:bg-zinc-800">{o.label}</SelectItem>
      ))}
    </SelectContent>
  </Select>
);

const Metric = ({ label, value, accent = 'text-zinc-100' }) => (
  <div className="rounded-xl border border-zinc-800 bg-zinc-900 p-4">
    <p className="text-[10px] font-mono uppercase tracking-wider text-zinc-500 mb-2">{label}</p>
    <p className={`text-2xl font-mono font-bold ${accent}`}>{value ?? 0}</p>
  </div>
);

const FeedbackTab = ({ active }) => {
  const [items, setItems] = useState([]);
  const [metrics, setMetrics] = useState({});
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [pageSize] = useState(50);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState(null);
  const [saving, setSaving] = useState(false);
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
      const res = await axios.get(`${API_URL}/api/admin/beta-program/feedback`, {
        params: buildParams({ page, page_size: pageSize }), withCredentials: true,
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

  useEffect(() => { if (active) fetchData(); }, [active, fetchData]);

  const setFilter = (key, val) => { setFilters((f) => ({ ...f, [key]: val })); setPage(1); };

  const handleExport = async (format) => {
    try {
      const res = await axios.get(`${API_URL}/api/admin/beta-program/feedback/export`, {
        params: buildParams({ format }), withCredentials: true,
        responseType: format === 'json' ? 'json' : 'blob',
      });
      const blob = format === 'json'
        ? new Blob([JSON.stringify(res.data, null, 2)], { type: 'application/json' })
        : res.data;
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url; link.download = `beta_feedback_export.${format === 'json' ? 'json' : 'csv'}`;
      document.body.appendChild(link); link.click(); link.remove();
      window.URL.revokeObjectURL(url);
    } catch (err) {
      toast.error("Errore durante l'export");
    }
  };

  const patchFeedback = async (feedbackId, payload) => {
    setSaving(true);
    try {
      const res = await axios.patch(`${API_URL}/api/admin/beta-program/feedback/${feedbackId}`, payload, { withCredentials: true });
      toast.success('Feedback aggiornato');
      setSelected(res.data.feedback);
      fetchData();
    } catch (err) {
      toast.error('Errore aggiornamento feedback');
    } finally {
      setSaving(false);
    }
  };

  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  return (
    <div data-testid="beta-feedback">
      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-3 mb-6">
        <Metric label="Totali" value={metrics.total} accent="text-gold" />
        <Metric label="Nuovi" value={metrics.new} accent="text-amber-400" />
        <Metric label="Accettati" value={metrics.accepted} accent="text-emerald-400" />
        <Metric label="Alta/Bloccante" value={metrics.high_priority} accent="text-red-400" />
        <div className="rounded-xl border border-zinc-800 bg-zinc-900 p-4">
          <p className="text-[10px] font-mono uppercase tracking-wider text-zinc-500 mb-2">Top categoria errore</p>
          <p className="text-sm font-medium text-zinc-200 break-words">{metrics.top_error_category || '—'}</p>
        </div>
        <div className="rounded-xl border border-zinc-800 bg-zinc-900 p-4">
          <p className="text-[10px] font-mono uppercase tracking-wider text-zinc-500 mb-2">Sezione critica</p>
          <p className="text-sm font-medium text-zinc-200 break-words">{labelFor(SECTION_KEYS, metrics.top_problematic_section)}</p>
        </div>
      </div>

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
          <Button variant="outline" className="border-zinc-700 text-zinc-300" onClick={fetchData}>Aggiorna</Button>
          <div className="flex-1" />
          <Button variant="outline" className="border-zinc-700 text-zinc-200" onClick={() => handleExport('json')}><Download className="w-4 h-4 mr-1" /> Export JSON</Button>
          <Button variant="outline" className="border-zinc-700 text-zinc-200" onClick={() => handleExport('csv')}><Download className="w-4 h-4 mr-1" /> Export CSV</Button>
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
                  <th className="py-2 px-2">Tester</th>
                  <th className="py-2 px-2">Report</th>
                  <th className="py-2 px-2">Sezione</th>
                  <th className="py-2 px-2">Tipo (tester)</th>
                  <th className="py-2 px-2">Osservazione (verbatim)</th>
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
                    <td className="py-2 px-2">{labelFor(FEEDBACK_TYPES, it.feedback_type)}</td>
                    <td className="py-2 px-2 max-w-[220px] truncate text-zinc-400" data-testid="beta-fb-verbatim">{it.expert_comment}</td>
                    <td className="py-2 px-2">{labelFor(STATUSES, it.status)}</td>
                  </tr>
                ))}
                {items.length === 0 && (
                  <tr><td colSpan="7" className="py-6 text-center text-zinc-500">Nessun feedback</td></tr>
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

      {selected && (
        <FeedbackDrawer selected={selected} saving={saving} onClose={() => setSelected(null)} onPatch={patchFeedback} />
      )}
    </div>
  );
};

const Section = ({ title, children, accent }) => (
  <div className="mt-5">
    <p className={`text-xs font-mono uppercase tracking-wider mb-2 ${accent || 'text-zinc-500'}`}>{title}</p>
    {children}
  </div>
);

const FeedbackDrawer = ({ selected, saving, onClose, onPatch }) => {
  const [notes, setNotes] = useState(selected.admin_notes || '');
  useEffect(() => { setNotes(selected.admin_notes || ''); }, [selected.id]); // eslint-disable-line

  return (
    <div className="fixed inset-0 z-50 flex justify-end" data-testid="beta-fb-drawer">
      <div className="absolute inset-0 bg-black/60" onClick={onClose} />
      <div className="relative h-full w-full max-w-xl overflow-y-auto bg-zinc-900 border-l border-zinc-800 p-6 shadow-2xl">
        <button onClick={onClose} className="absolute top-4 right-4 text-zinc-500 hover:text-zinc-200"><X className="w-5 h-5" /></button>
        <h3 className="text-lg font-serif font-bold text-zinc-100 mb-1">Dettaglio feedback</h3>
        <p className="text-xs font-mono text-zinc-500 mb-5 break-all">{selected.user_email} • {fmt(selected.created_at)}</p>

        {/* Tester's own words — verbatim, read-only. */}
        <Section title="Dichiarazione del tester (verbatim)" accent="text-emerald-400">
          <p className="text-sm text-zinc-100 whitespace-pre-wrap bg-zinc-950 rounded-lg p-3 border border-zinc-800" data-testid="beta-fb-drawer-verbatim">{selected.expert_comment || '—'}</p>
          {selected.expected_correction && (
            <p className="mt-2 text-sm text-zinc-300 whitespace-pre-wrap"><span className="text-zinc-500">Correzione suggerita dal tester: </span>{selected.expected_correction}</p>
          )}
          <p className="mt-2 text-[11px] text-zinc-500">Tipo dichiarato dal tester: {labelFor(FEEDBACK_TYPES, selected.feedback_type)} • Priorità tester: {labelFor(PRIORITIES, selected.priority)}</p>
        </Section>

        <Section title="Riferimento analisi/lotto">
          <p className="text-sm text-zinc-300">{selected.file_name || selected.analysis_id || '—'} {selected.item_reference?.item_scope_label ? `• ${selected.item_reference.item_scope_label}` : ''}</p>
        </Section>

        {/* Owner interpretation — clearly separated; never rewrites tester text. */}
        <Section title="Interpretazione owner" accent="text-gold">
          <div className="space-y-3">
            <div>
              <p className="text-[11px] text-zinc-500 mb-1">Stato</p>
              <div className="flex flex-wrap gap-2">
                {STATUSES.map((s) => (
                  <Button key={s.value} size="sm"
                    variant={selected.status === s.value ? 'default' : 'outline'}
                    className={selected.status === s.value ? 'bg-gold text-zinc-950' : 'border-zinc-700 text-zinc-300'}
                    disabled={saving}
                    onClick={() => onPatch(selected.id, { status: s.value })}
                    data-testid={`beta-fb-status-${s.value}`}>{s.label}</Button>
                ))}
              </div>
            </div>
            <div>
              <p className="text-[11px] text-zinc-500 mb-1">Categoria (owner)</p>
              <Select value={selected.owner_category || undefined} onValueChange={(v) => onPatch(selected.id, { category: v })}>
                <SelectTrigger className="bg-zinc-950 border-zinc-800 text-zinc-100" data-testid="beta-fb-owner-category"><SelectValue placeholder="Assegna categoria" /></SelectTrigger>
                <SelectContent className="bg-zinc-900 border-zinc-700 text-zinc-100 max-h-72">
                  {OWNER_CATEGORIES.map((o) => (
                    <SelectItem key={o.value} value={o.value} className="text-zinc-200 focus:bg-zinc-800">{o.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div>
              <p className="text-[11px] text-zinc-500 mb-1">Priorità (owner)</p>
              <Select value={selected.owner_priority || undefined} onValueChange={(v) => onPatch(selected.id, { priority: v })}>
                <SelectTrigger className="bg-zinc-950 border-zinc-800 text-zinc-100" data-testid="beta-fb-owner-priority"><SelectValue placeholder="Assegna priorità" /></SelectTrigger>
                <SelectContent className="bg-zinc-900 border-zinc-700 text-zinc-100">
                  {OWNER_PRIORITIES.map((o) => (
                    <SelectItem key={o.value} value={o.value} className="text-zinc-200 focus:bg-zinc-800">{o.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div>
              <p className="text-[11px] text-zinc-500 mb-1">Nota interna (owner)</p>
              <Input placeholder="Nota interna" value={notes} onChange={(e) => setNotes(e.target.value)}
                className="bg-zinc-950 border-zinc-800 text-zinc-100 mb-2" data-testid="beta-fb-owner-note" />
              <Button size="sm" variant="outline" className="border-zinc-700 text-zinc-200"
                disabled={saving} onClick={() => onPatch(selected.id, { admin_notes: notes })}>Salva nota</Button>
            </div>
          </div>
          {selected.reviewed_by && (
            <p className="text-xs text-zinc-500 mt-3">Revisionato da {selected.reviewed_by} • {fmt(selected.reviewed_at)}</p>
          )}
        </Section>
      </div>
    </div>
  );
};

export default FeedbackTab;
