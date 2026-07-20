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

// --- Beta quota (configurable perizia allowance) -----------------------
// Quota is a second, orthogonal entitlement axis on top of membership status
// (§D of docs/beta_perizia_limits_plan.md): UNLIMITED (default, unchanged
// behaviour) or LIMITED to N analyses per phase. "Rimanenti" (remaining) is
// always the primary number shown to the owner, never merely "consumed".
const quotaModeLabel = (quota) => {
  if (!quota || quota.mode === 'UNLIMITED') return 'Illimitata';
  return `Limitata a ${quota.limit ?? '—'} perizie`;
};

const quotaStateLabel = (quota) => {
  if (!quota || quota.mode === 'UNLIMITED') return 'Illimitata';
  if (quota.state === 'EXHAUSTED') return 'Limite raggiunto';
  return 'Disponibile';
};

const quotaStateAccent = (quota) => {
  const label = quotaStateLabel(quota);
  if (label === 'Limite raggiunto') return 'text-red-400 border-red-500/40 bg-red-500/10';
  if (label === 'Illimitata') return 'text-gold border-gold/40 bg-gold/10';
  return 'text-emerald-400 border-emerald-500/40 bg-emerald-500/10';
};

const QuotaCell = ({ quota }) => {
  const remaining = Math.max(0, Number(quota?.remaining ?? 0));
  const reserved = Number(quota?.reserved ?? 0);
  const consumed = Number(quota?.consumed ?? 0);
  const isLimited = quota && quota.mode === 'LIMITED';
  return (
    <div className="min-w-[160px]">
      <p className="font-medium text-zinc-200">{quotaModeLabel(quota)}</p>
      {isLimited && (
        <div className="mt-1 space-y-0.5 text-[11px] text-zinc-400">
          <p className="text-sm font-semibold text-zinc-100" data-testid="quota-remaining-line">
            {remaining} perizie rimanenti su {quota.limit ?? '—'}
          </p>
          <p>Utilizzate: {consumed}</p>
          {reserved > 0 && <p>In elaborazione: {reserved}</p>}
          <p>Rimanenti: {remaining}</p>
        </div>
      )}
      <span className={`mt-1 inline-block rounded-full border px-2 py-0.5 text-[10px] font-mono uppercase ${quotaStateAccent(quota)}`}>
        {quotaStateLabel(quota)}
      </span>
    </div>
  );
};

const NewPhaseDialog = ({ tester, onClose, onConfirmed }) => {
  const [busy, setBusy] = useState(false);

  const confirm = async () => {
    setBusy(true);
    try {
      await axios.post(
        `${API_URL}/api/admin/beta-program/testers/${tester.membership_id}/quota/new-phase`,
        { confirm: true },
        { withCredentials: true },
      );
      toast.success('Nuova fase beta avviata');
      onConfirmed();
    } catch (err) {
      toast.error(err?.response?.data?.detail?.reason_human || 'Errore durante avvio nuova fase');
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center p-4" data-testid="beta-new-phase-dialog">
      <div className="absolute inset-0 bg-black/60" onClick={onClose} />
      <div className="relative w-full max-w-lg rounded-xl border border-zinc-800 bg-zinc-900 p-6 shadow-2xl">
        <h3 className="text-lg font-serif font-bold text-zinc-100 mb-2">Avvia nuova fase beta</h3>
        <p className="text-sm text-zinc-300 mb-4">
          Verrà avviata una nuova fase beta. Le analisi e l'utilizzo della fase precedente resteranno nello storico.
        </p>
        <div className="flex justify-end gap-2">
          <Button variant="outline" className="border-zinc-700 text-zinc-300" onClick={onClose} disabled={busy}>Annulla</Button>
          <Button className="bg-gold text-zinc-950 hover:bg-gold-dim" onClick={confirm} disabled={busy}
            data-testid="new-phase-confirm-btn">Conferma nuova fase</Button>
        </div>
      </div>
    </div>
  );
};

const QuotaDialog = ({ tester, onClose, onSaved }) => {
  const quota = tester.quota || {};
  const [mode, setMode] = useState(quota.mode === 'LIMITED' ? 'LIMITED' : 'UNLIMITED');
  const [limitInput, setLimitInput] = useState(quota.limit != null ? String(quota.limit) : '');
  const [validationError, setValidationError] = useState('');
  const [saving, setSaving] = useState(false);
  const [showNewPhase, setShowNewPhase] = useState(false);

  const consumed = Number(quota.consumed ?? 0);
  const reserved = Number(quota.reserved ?? 0);
  const parsedLimit = Number(limitInput);
  const isValidLimit = mode === 'UNLIMITED' || (Number.isInteger(parsedLimit) && parsedLimit > 0);
  const remainingAfter = mode === 'LIMITED' && isValidLimit
    ? Math.max(0, parsedLimit - consumed - reserved)
    : null;
  const lowersBelowConsumed = mode === 'LIMITED' && isValidLimit && parsedLimit < (consumed + reserved);

  const submit = async () => {
    if (!isValidLimit) {
      setValidationError('Inserisci un numero intero maggiore di zero.');
      return;
    }
    setValidationError('');
    setSaving(true);
    try {
      await axios.patch(
        `${API_URL}/api/admin/beta-program/testers/${tester.membership_id}/quota`,
        { quota_mode: mode, analysis_limit: mode === 'LIMITED' ? parsedLimit : null },
        { withCredentials: true },
      );
      toast.success('Quota aggiornata');
      onSaved();
    } catch (err) {
      toast.error(err?.response?.data?.detail?.reason_human || 'Errore aggiornamento quota');
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4" data-testid="beta-quota-dialog">
      <div className="absolute inset-0 bg-black/60" onClick={onClose} />
      <div className="relative w-full max-w-lg rounded-xl border border-zinc-800 bg-zinc-900 p-6 shadow-2xl">
        <h3 className="text-lg font-serif font-bold text-zinc-100 mb-2">Gestisci limite</h3>
        <p className="text-sm text-zinc-400 mb-4">
          Stai per modificare la quota beta di <span className="text-zinc-200">{tester.normalized_email}</span>.
        </p>

        <div className="mb-4">
          <p className="mb-2 text-xs font-mono uppercase tracking-wide text-zinc-500">Modalità</p>
          <div className="flex gap-2">
            <Button type="button" variant="outline"
              className={mode === 'UNLIMITED' ? 'border-gold text-gold' : 'border-zinc-700 text-zinc-300'}
              onClick={() => { setMode('UNLIMITED'); setValidationError(''); }}
              data-testid="quota-mode-unlimited">Illimitata</Button>
            <Button type="button" variant="outline"
              className={mode === 'LIMITED' ? 'border-gold text-gold' : 'border-zinc-700 text-zinc-300'}
              onClick={() => setMode('LIMITED')}
              data-testid="quota-mode-limited">Limitata</Button>
          </div>
        </div>

        {mode === 'LIMITED' && (
          <div className="mb-4">
            <label className="mb-2 block text-xs font-mono uppercase tracking-wide text-zinc-500">
              Numero massimo di perizie
            </label>
            <Input type="number" min="1" value={limitInput}
              onChange={(e) => { setLimitInput(e.target.value); if (validationError) setValidationError(''); }}
              className="bg-zinc-950 border-zinc-800 text-zinc-100" data-testid="quota-limit-input" />
            {validationError && (
              <p className="mt-1 text-[11px] text-red-400" data-testid="quota-limit-error">{validationError}</p>
            )}

            <div className="mt-3 space-y-1 rounded-lg border border-zinc-800 bg-zinc-950/60 p-3 text-sm text-zinc-300" data-testid="quota-preview">
              <p>Nuovo limite: {isValidLimit ? parsedLimit : '—'}</p>
              <p>Già utilizzate: {consumed}</p>
              <p>Rimanenti dopo la modifica: {remainingAfter != null ? remainingAfter : '—'}</p>
            </div>

            {lowersBelowConsumed && (
              <div className="mt-3 rounded-lg border border-amber-500/30 bg-amber-500/10 p-3 text-sm text-amber-200" data-testid="quota-lower-warning">
                Il nuovo limite è inferiore alle perizie già utilizzate: l'utilizzo registrato resta invariato, la
                quota risulterà esaurita e non verrà applicato alcun addebito retroattivo.
              </div>
            )}

            <div className="mt-4">
              <Button type="button" variant="outline" className="border-zinc-700 text-zinc-300"
                onClick={() => setShowNewPhase(true)} data-testid="beta-new-phase-open-btn">
                Avvia nuova fase beta
              </Button>
            </div>
          </div>
        )}

        <div className="mt-4 flex justify-end gap-2">
          <Button variant="outline" className="border-zinc-700 text-zinc-300" onClick={onClose} disabled={saving}>Annulla</Button>
          <Button className="bg-gold text-zinc-950 hover:bg-gold-dim" onClick={submit} disabled={saving}
            data-testid="quota-save-btn">Salva</Button>
        </div>
      </div>

      {showNewPhase && (
        <NewPhaseDialog
          tester={tester}
          onClose={() => setShowNewPhase(false)}
          onConfirmed={() => { setShowNewPhase(false); onSaved(); }}
        />
      )}
    </div>
  );
};

const PhasesDialog = ({ tester, onClose }) => {
  const [phases, setPhases] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let mounted = true;
    axios.get(`${API_URL}/api/admin/beta-program/testers/${tester.membership_id}/quota/phases`, { withCredentials: true })
      .then((res) => { if (mounted) setPhases(res.data?.items || res.data?.phases || []); })
      .catch(() => { if (mounted) toast.error('Errore nel caricamento dello storico fasi'); })
      .finally(() => { if (mounted) setLoading(false); });
    return () => { mounted = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tester.membership_id]);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4" data-testid="beta-phases-dialog">
      <div className="absolute inset-0 bg-black/60" onClick={onClose} />
      <div className="relative w-full max-w-2xl rounded-xl border border-zinc-800 bg-zinc-900 p-6 shadow-2xl">
        <h3 className="text-lg font-serif font-bold text-zinc-100 mb-1">Storico fasi beta</h3>
        <p className="text-sm text-zinc-400 mb-4">{tester.normalized_email}</p>
        {loading ? (
          <p className="py-6 text-sm text-zinc-400">Caricamento...</p>
        ) : phases.length === 0 ? (
          <p className="py-6 text-sm text-zinc-500">Nessuna fase storica disponibile.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm" data-testid="beta-phases-table">
              <thead>
                <tr className="border-b border-zinc-800 text-left text-[11px] font-mono uppercase tracking-wider text-zinc-500">
                  <th className="py-2 px-2">Fase</th>
                  <th className="py-2 px-2">Limite</th>
                  <th className="py-2 px-2">Utilizzate</th>
                  <th className="py-2 px-2">Inizio</th>
                  <th className="py-2 px-2">Fine</th>
                  <th className="py-2 px-2">Attore</th>
                </tr>
              </thead>
              <tbody>
                {phases.map((p) => (
                  <tr key={p.quota_version} className="border-b border-zinc-800/60 text-zinc-200">
                    <td className="py-2 px-2">v{p.quota_version}</td>
                    <td className="py-2 px-2">{p.limit != null ? p.limit : 'Illimitata'}</td>
                    <td className="py-2 px-2">{p.consumed ?? 0}</td>
                    <td className="py-2 px-2 whitespace-nowrap">{fmt(p.started_at)}</td>
                    <td className="py-2 px-2 whitespace-nowrap">{fmt(p.ended_at)}</td>
                    <td className="py-2 px-2">{p.actor_email || p.actor || '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        <div className="mt-4 flex justify-end">
          <Button variant="outline" className="border-zinc-700 text-zinc-300" onClick={onClose}>Chiudi</Button>
        </div>
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
  const [quotaTarget, setQuotaTarget] = useState(null);
  const [phasesTarget, setPhasesTarget] = useState(null);

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
                  <th className="py-2 px-2">Quota beta</th>
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
                    <td className="py-2 px-2"><QuotaCell quota={it.quota} /></td>
                    <td className="py-2 px-2 text-right">
                      <div className="flex flex-wrap justify-end gap-2">
                        <Button size="sm" variant="outline" className="border-zinc-700 text-zinc-300"
                          disabled={busy} onClick={() => setQuotaTarget(it)} data-testid="beta-quota-manage-btn">
                          Gestisci limite
                        </Button>
                        <Button size="sm" variant="outline" className="border-zinc-700 text-zinc-300"
                          disabled={busy} onClick={() => setPhasesTarget(it)} data-testid="beta-phases-open-btn">
                          Storico fasi
                        </Button>
                        {it.status === 'REVOKED' ? (
                          <Button size="sm" variant="outline" className="border-emerald-600/50 text-emerald-400"
                            disabled={busy} onClick={() => doReactivate(it)} data-testid="beta-reactivate-btn">Riattiva</Button>
                        ) : (
                          <Button size="sm" variant="outline" className="border-red-600/50 text-red-400"
                            disabled={busy} onClick={() => setRevokeTarget(it)} data-testid="beta-revoke-btn">Revoca</Button>
                        )}
                      </div>
                    </td>
                  </tr>
                ))}
                {items.length === 0 && (
                  <tr><td colSpan="11" className="py-6 text-center text-zinc-500">Nessun tester</td></tr>
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

      {quotaTarget && (
        <QuotaDialog
          tester={quotaTarget}
          onClose={() => setQuotaTarget(null)}
          onSaved={() => { setQuotaTarget(null); fetchData(); }}
        />
      )}

      {phasesTarget && (
        <PhasesDialog tester={phasesTarget} onClose={() => setPhasesTarget(null)} />
      )}
    </div>
  );
};

export default TestersTab;
