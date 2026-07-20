import React, { useCallback, useEffect, useState } from 'react';
import axios from 'axios';
import { toast } from 'sonner';

const API_URL = process.env.REACT_APP_BACKEND_URL;

const Metric = ({ label, value, accent = 'text-zinc-100', hint }) => (
  <div className="rounded-xl border border-zinc-800 bg-zinc-900 p-4">
    <p className="text-[10px] font-mono uppercase tracking-wider text-zinc-500 mb-2">{label}</p>
    <p className={`text-2xl font-mono font-bold ${accent}`}>{value ?? 0}</p>
    {hint && <p className="mt-1 text-[11px] text-zinc-500">{hint}</p>}
  </div>
);

// Every metric here is a deterministic Mongo count / small aggregation. Loading
// this tab makes ZERO OpenAI calls, spawns ZERO jobs, changes ZERO credits, and
// makes ZERO Stripe calls (all reads).
const OverviewTab = ({ active }) => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const res = await axios.get(`${API_URL}/api/admin/beta-program/overview`, { withCredentials: true });
      setData(res.data || {});
    } catch (err) {
      toast.error('Errore nel caricamento della panoramica');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { if (active) fetchData(); }, [active, fetchData]);

  if (loading || !data) {
    return <div className="text-zinc-400 font-mono text-sm py-6">Loading...</div>;
  }

  const t = data.testers || {};
  const a = data.analyses || {};
  const r = data.reports || {};
  const f = data.feedback || {};

  return (
    <div className="space-y-6" data-testid="beta-overview">
      <section>
        <h3 className="text-xs font-mono uppercase tracking-wider text-zinc-500 mb-3">Tester</h3>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <Metric label="Attivi" value={t.active} accent="text-emerald-400" />
          <Metric label="In attesa" value={t.pending} accent="text-amber-400" />
          <Metric label="Revocati" value={t.revoked} accent="text-zinc-300" />
          <Metric label="Account registrati" value={t.registered} accent="text-gold" />
        </div>
      </section>

      <section>
        <h3 className="text-xs font-mono uppercase tracking-wider text-zinc-500 mb-3">Attività beta</h3>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <Metric label="Perizie beta" value={a.beta_total} hint="Analisi degli account beta" />
          <Metric label="PDF illeggibili" value={a.unreadable_total} accent="text-red-400" />
          <Metric label="Report pronti" value={r.ready_total} accent="text-emerald-400" />
          <Metric label="Verifica richiesta" value={r.verification_required_total} accent="text-amber-400" />
          <Metric label="Conferme richieste" value={r.confirmation_required_total} />
          <Metric label="Conferme completate" value={r.confirmation_completed_total} accent="text-emerald-400" />
          <Metric label="Report riutilizzati" value={r.reused_total} />
          <Metric label="Rigenerazioni forzate" value={r.forced_rerun_total} />
          <Metric label="Servizio occupato" value={r.service_busy_total} accent="text-amber-400" />
          <Metric label="Servizio non disp." value={r.service_unavailable_total} accent="text-red-400" />
          <Metric
            label="Durata media (s)"
            value={r.avg_duration_seconds != null ? r.avg_duration_seconds : '—'}
            hint="Solo report pronti"
          />
        </div>
      </section>

      <section>
        <h3 className="text-xs font-mono uppercase tracking-wider text-zinc-500 mb-3">Feedback dei tester</h3>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <Metric label="Totali" value={f.total} accent="text-gold" />
          <Metric label="Nuovi" value={f.new} accent="text-amber-400" />
          <Metric label="Accettati" value={f.accepted} accent="text-emerald-400" />
          <Metric label="Alta/Bloccante" value={f.high_priority} accent="text-red-400" />
        </div>
        <p className="mt-2 text-[11px] text-zinc-500">
          Le metriche di feedback derivano solo da dichiarazioni esplicite dei tester (verbatim).
        </p>
      </section>
    </div>
  );
};

export default OverviewTab;
