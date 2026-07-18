import React, { useEffect, useState } from 'react';
import { DetailBlock } from './shared';
import { getCorrectnessV2DecisionModel } from '../../lib/api/perizia';

// Vista admin ADDITIVE block (Part 21): a read-only preview of the customer
// decision model + persisted confirmations + append-only audit for a job.
// Self-fetches the admin-only route; silently hides when not authorized or on
// error, so it never disturbs the existing admin panel.
const AdminDecisionModelPreview = ({ analysisId, jobId }) => {
  const [data, setData] = useState(null);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    if (!analysisId || !jobId || typeof getCorrectnessV2DecisionModel !== 'function') return undefined;
    const controller = new AbortController();
    let active = true;
    Promise.resolve()
      .then(() => getCorrectnessV2DecisionModel(analysisId, jobId, { signal: controller.signal }))
      .then((resp) => { if (active) setData(resp?.data || null); })
      .catch(() => { if (active) setFailed(true); });
    return () => { active = false; controller.abort(); };
  }, [analysisId, jobId]);

  if (failed || !data) return null;
  const model = data.decision_model || {};
  const readiness = model.readiness || {};
  const confirmations = Array.isArray(data.confirmations) ? data.confirmations : [];
  const audit = Array.isArray(data.audit) ? data.audit : [];

  return (
    <section data-testid="cv2-admin-decision-model" className="space-y-3">
      <h3 className="text-lg font-semibold text-zinc-100">Decision model (anteprima)</h3>
      <div className="rounded-lg border border-zinc-800 bg-zinc-900/70 p-3 text-sm text-zinc-300">
        <p>Esito: <span className="text-zinc-100">{model.esito?.level}</span> · Readiness:{' '}
          <span className="font-mono text-gold">{readiness.state}</span> ({readiness.label})</p>
        <p className="mt-1">Findings: {(model.findings || []).length} · Conferme: {confirmations.length}</p>
      </div>

      {confirmations.length > 0 && (
        <DetailBlock title={`Conferme utente vs perizia (${confirmations.length})`} testId="cv2-admin-confirmations">
          <ul className="space-y-2">
            {confirmations.map((c) => (
              <li key={c.confirmation_id} className="rounded-md border border-zinc-800 bg-zinc-950 p-2 text-xs text-zinc-300">
                <p><span className="font-mono text-zinc-500">{c.finding_id}</span> — {c.selected_label}</p>
                <p className="text-zinc-500">stato: {c.status} · pagina: {c.page} · source: {c.source}</p>
              </li>
            ))}
          </ul>
        </DetailBlock>
      )}

      {audit.length > 0 && (
        <DetailBlock title={`Audit conferme (${audit.length})`} testId="cv2-admin-confirmation-audit">
          <ul className="space-y-1">
            {audit.map((a) => (
              <li key={a.audit_id} className="font-mono text-[11px] text-zinc-500">
                {a.at} · {a.action} · {a.from_option || '-'} → {a.to_option}
              </li>
            ))}
          </ul>
        </DetailBlock>
      )}

      <DetailBlock title="Decision model (JSON)" testId="cv2-admin-decision-json">
        <pre className="max-h-96 overflow-auto rounded bg-zinc-950 p-2 text-[11px] text-zinc-400">
          {JSON.stringify(model, null, 2)}
        </pre>
      </DetailBlock>
    </section>
  );
};

export default AdminDecisionModelPreview;
export { AdminDecisionModelPreview };
