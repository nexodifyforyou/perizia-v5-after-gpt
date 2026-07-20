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

const STATUS_LABELS = { PENDING: 'In attesa', ACTIVE: 'Attivo', REVOKED: 'Revocato' };
const STATUS_ACCENT = {
  PENDING: 'text-amber-400 border-amber-500/40 bg-amber-500/10',
  ACTIVE: 'text-emerald-400 border-emerald-500/40 bg-emerald-500/10',
  REVOKED: 'text-zinc-400 border-zinc-600/40 bg-zinc-700/10',
};
const PARTNER_TYPES = [
  { value: 'geometra', label: 'Geometra' },
  { value: 'avvocato', label: 'Avvocato' },
  { value: 'investitore', label: 'Investitore' },
  { value: 'altro', label: 'Altro' },
];

const fmt = (v) => (v ? new Date(v).toLocaleString('it-IT') : '—');

const StatusBadge = ({ status }) => (
  <span className={`inline-flex items-center rounded-full border px-2 py-0.5 text-[11px] font-mono uppercase ${STATUS_ACCENT[status] || ''}`}>
    {STATUS_LABELS[status] || status}
  </span>
);

const isValidEmail = (email) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(email || '').trim());

const AddTesterForm = ({ onAdded }) => {
  const [email, setEmail] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [partnerType, setPartnerType] = useState('geometra');
  const [note, setNote] = useState('');
  const [saving, setSaving] = useState(false);
  const [emailError, setEmailError] = useState('');

  const submit = async () => {
    if (!isValidEmail(email)) {
      setEmailError('Inserisci un indirizzo email valido');
      return;
    }
    setEmailError('');
    setSaving(true);
    try {
      await axios.post(`${API_URL}/api/admin/beta-program/testers`, {
        email: email.trim(),
        display_name: displayName.trim() || null,
        partner_type: partnerType,
        internal_note: note.trim() || null,
      }, { withCredentials: true });
      toast.success('Tester aggiunto');
      setEmail(''); setDisplayName(''); setNote('');
      onAdded();
    } catch (err) {
      const code = err?.response?.data?.detail?.reason_code;
      if (code === 'MEMBERSHIP_EXISTS') toast.error('Esiste già una membership per questa email');
      else if (code === 'MEMBERSHIP_REVOKED') toast.error('Membership revocata: usa Riattiva');
      else if (code === 'OWNER_CANNOT_BE_TESTER') toast.error("L'owner/admin non può essere un tester");
      else toast.error(err?.response?.data?.detail?.reason_human || 'Errore aggiunta tester');
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4 mb-4" data-testid="beta-add-tester-form">
      <h3 className="text-sm font-semibold text-zinc-100 mb-3">Aggiungi tester</h3>
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-3">
        <div>
          <Input placeholder="Email *" value={email}
            onChange={(e) => { setEmail(e.target.value); if (emailError) setEmailError(''); }}
            className="bg-zinc-950 border-zinc-800 text-zinc-100" data-testid="beta-add-email" />
          {emailError && <p className="mt-1 text-[11px] text-red-400" data-testid="beta-add-email-error">{emailError}</p>}
        </div>
        <Input placeholder="Nome (opzionale)" value={displayName}
          onChange={(e) => setDisplayName(e.target.value)}
          className="bg-zinc-950 border-zinc-800 text-zinc-100" />
        <Select value={partnerType} onValueChange={setPartnerType}>
          <SelectTrigger className="bg-zinc-950 border-zinc-800 text-zinc-100"><SelectValue /></SelectTrigger>
          <SelectContent className="bg-zinc-900 border-zinc-700 text-zinc-100">
            {PARTNER_TYPES.map((o) => (
              <SelectItem key={o.value} value={o.value} className="text-zinc-200 focus:bg-zinc-800">{o.label}</SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Input placeholder="Nota interna (opzionale)" value={note}
          onChange={(e) => setNote(e.target.value)}
          className="bg-zinc-950 border-zinc-800 text-zinc-100" />
      </div>
      <div className="mt-3">
        <Button className="bg-gold text-zinc-950 hover:bg-gold-dim" disabled={saving}
          onClick={submit} data-testid="beta-add-submit">Aggiungi tester</Button>
      </div>
    </div>
  );
};

const RevokeDialog = ({ tester, onClose, onConfirm, busy }) => (
  <div className="fixed inset-0 z-50 flex items-center justify-center p-4" data-testid="beta-revoke-dialog">
    <div className="absolute inset-0 bg-black/60" onClick={onClose} />
    <div className="relative w-full max-w-lg rounded-xl border border-zinc-800 bg-zinc-900 p-6 shadow-2xl">
      <h3 className="text-lg font-serif font-bold text-zinc-100 mb-2">Revoca accesso beta</h3>
      <p className="text-sm text-zinc-400 mb-3">
        Stai per revocare l'accesso beta di <span className="text-zinc-200">{tester.normalized_email}</span>.
      </p>
      <ul className="text-sm text-zinc-300 space-y-1 mb-4 list-disc pl-5">
        <li>L'account resta attivo e l'utente può ancora accedere.</li>
        <li>Le analisi e i report storici restano disponibili.</li>
        <li>Il feedback resta conservato.</li>
        <li>Viene rimosso solo l'accesso illimitato beta.</li>
        <li>Da quel momento si applicano le normali regole del piano.</li>
        <li>I crediti già acquistati restano preservati.</li>
      </ul>
      <div className="flex justify-end gap-2">
        <Button variant="outline" className="border-zinc-700 text-zinc-300" onClick={onClose} disabled={busy}>Annulla</Button>
        <Button className="bg-red-600 text-white hover:bg-red-500" onClick={onConfirm} disabled={busy}
          data-testid="beta-revoke-confirm">Revoca accesso beta</Button>
      </div>
    </div>
  </div>
);

const TestersTab = ({ active }) => {
  const [items, setItems] = useState([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [pageSize] = useState(25);
  const [loading, setLoading] = useState(true);
  const [statusFilter, setStatusFilter] = useState(ALL);
  const [query, setQuery] = useState('');
  const [revokeTarget, setRevokeTarget] = useState(null);
  const [busy, setBusy] = useState(false);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const params = { page, page_size: pageSize };
      if (statusFilter !== ALL) params.status = statusFilter;
      if (query.trim()) params.q = query.trim();
      const res = await axios.get(`${API_URL}/api/admin/beta-program/testers`, { params, withCredentials: true });
      setItems(res.data.items || []);
      setTotal(res.data.total || 0);
    } catch (err) {
      toast.error('Errore nel caricamento dei tester');
    } finally {
      setLoading(false);
    }
  }, [page, pageSize, statusFilter, query]);

  useEffect(() => { if (active) fetchData(); }, [active, fetchData]);

  const doRevoke = async () => {
    if (!revokeTarget) return;
    setBusy(true);
    try {
      await axios.post(`${API_URL}/api/admin/beta-program/testers/${revokeTarget.membership_id}/revoke`, {}, { withCredentials: true });
      toast.success('Accesso beta revocato');
      setRevokeTarget(null);
      fetchData();
    } catch (err) {
      toast.error('Errore durante la revoca');
    } finally {
      setBusy(false);
    }
  };

  const doReactivate = async (tester) => {
    setBusy(true);
    try {
      await axios.post(`${API_URL}/api/admin/beta-program/testers/${tester.membership_id}/reactivate`, {}, { withCredentials: true });
      toast.success('Accesso beta riattivato');
      fetchData();
    } catch (err) {
      toast.error('Errore durante la riattivazione');
    } finally {
      setBusy(false);
    }
  };

  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  return (
    <div data-testid="beta-testers">
      <AddTesterForm onAdded={fetchData} />

      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4 mb-4">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          <Input placeholder="Cerca per email o nome" value={query}
            onChange={(e) => { setQuery(e.target.value); setPage(1); }}
            className="bg-zinc-950 border-zinc-800 text-zinc-100" data-testid="beta-testers-search" />
          <Select value={statusFilter} onValueChange={(v) => { setStatusFilter(v); setPage(1); }}>
            <SelectTrigger className="bg-zinc-950 border-zinc-800 text-zinc-100"><SelectValue placeholder="Tutti gli stati" /></SelectTrigger>
            <SelectContent className="bg-zinc-900 border-zinc-700 text-zinc-100">
              <SelectItem value={ALL} className="text-zinc-200 focus:bg-zinc-800">Tutti gli stati</SelectItem>
              <SelectItem value="PENDING" className="text-zinc-200 focus:bg-zinc-800">In attesa</SelectItem>
              <SelectItem value="ACTIVE" className="text-zinc-200 focus:bg-zinc-800">Attivo</SelectItem>
              <SelectItem value="REVOKED" className="text-zinc-200 focus:bg-zinc-800">Revocato</SelectItem>
            </SelectContent>
          </Select>
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
                  <th className="py-2 px-2">Nome</th>
                  <th className="py-2 px-2">Email</th>
                  <th className="py-2 px-2">Stato</th>
                  <th className="py-2 px-2">Account</th>
                  <th className="py-2 px-2">Aggiunto</th>
                  <th className="py-2 px-2">Attivato</th>
                  <th className="py-2 px-2">Revocato</th>
                  <th className="py-2 px-2">Perizie</th>
                  <th className="py-2 px-2">Feedback</th>
                  <th className="py-2 px-2 text-right">Azioni</th>
                </tr>
              </thead>
              <tbody>
                {items.map((it) => (
                  <tr key={it.membership_id} className="border-b border-zinc-800/60 text-zinc-200" data-testid="beta-tester-row">
                    <td className="py-2 px-2 max-w-[140px] truncate">{it.display_name || '—'}</td>
                    <td className="py-2 px-2 max-w-[180px] truncate">{it.normalized_email}</td>
                    <td className="py-2 px-2"><StatusBadge status={it.status} /></td>
                    <td className="py-2 px-2 text-zinc-400">{it.account_linked ? 'Account registrato' : 'Registrazione in attesa'}</td>
                    <td className="py-2 px-2 text-zinc-400 whitespace-nowrap">{fmt(it.added_at)}</td>
                    <td className="py-2 px-2 text-zinc-400 whitespace-nowrap">{fmt(it.activated_at)}</td>
                    <td className="py-2 px-2 text-zinc-400 whitespace-nowrap">{fmt(it.revoked_at)}</td>
                    <td className="py-2 px-2">{it.analyses_total ?? 0}</td>
                    <td className="py-2 px-2">{it.feedback_total ?? 0}</td>
                    <td className="py-2 px-2 text-right">
                      {it.status === 'REVOKED' ? (
                        <Button size="sm" variant="outline" className="border-emerald-600/50 text-emerald-400"
                          disabled={busy} onClick={() => doReactivate(it)} data-testid="beta-reactivate-btn">Riattiva</Button>
                      ) : (
                        <Button size="sm" variant="outline" className="border-red-600/50 text-red-400"
                          disabled={busy} onClick={() => setRevokeTarget(it)} data-testid="beta-revoke-btn">Revoca</Button>
                      )}
                    </td>
                  </tr>
                ))}
                {items.length === 0 && (
                  <tr><td colSpan="10" className="py-6 text-center text-zinc-500">Nessun tester</td></tr>
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

      {revokeTarget && (
        <RevokeDialog tester={revokeTarget} busy={busy} onClose={() => setRevokeTarget(null)} onConfirm={doRevoke} />
      )}
    </div>
  );
};

export default TestersTab;
