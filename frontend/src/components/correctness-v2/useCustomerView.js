import { useCallback, useEffect, useRef, useState } from 'react';
import { getCorrectnessV2CustomerView } from '../../lib/api/perizia';

// Single source of truth for the sanitized Correctness V2 customer report.
//
// Both AnalysisResult (to decide whether the legacy report must be hidden) and
// CustomerReportView (to render) read the SAME hook instance so the sanitized
// endpoint is fetched once and the "is a safe V2 customer report available?"
// answer never diverges between the page and the rendered report.
//
// A generic network failure or a 404 (feature disabled / no report) is treated
// as "unavailable" for gating purposes: `available` is false, so the caller
// falls back to the legacy report instead of stacking two reports.
//
// When the backend reports `preparing: true` (a V2 job is running for this
// analysis — e.g. auto-started on upload or by a customer lot selection), the
// hook silently re-polls the sanitized endpoint until the report appears or a
// terminal outcome makes `preparing` false. Silent polls never toggle
// `loading`, so the page never flashes back to a placeholder while waiting.

const PREPARING_POLL_MS = 8000;
const PREPARING_MAX_POLLS = 225; // ~30 minutes of background polling

const isCanceledError = (error) => (
  error?.code === 'ERR_CANCELED' ||
  error?.name === 'CanceledError' ||
  error?.message === 'canceled'
);

export const useCorrectnessV2CustomerView = (analysisId, { enabled = true } = {}) => {
  const active = Boolean(enabled && analysisId);
  const [loading, setLoading] = useState(active);
  const [error, setError] = useState('');
  const [payload, setPayload] = useState(null);
  const [selectedLotId, setSelectedLotId] = useState(null);
  // Remembered lot-selection report: when a customer picks a lot whose report
  // does not exist yet, we keep the selector data so they can navigate back
  // instead of hitting a dead end.
  const [lotSelectionReport, setLotSelectionReport] = useState(null);
  const mountedRef = useRef(false);
  const pollCountRef = useRef(0);

  useEffect(() => {
    mountedRef.current = true;
    return () => { mountedRef.current = false; };
  }, []);

  const load = useCallback((lotId, signal, { silent = false } = {}) => {
    if (!silent) {
      setLoading(true);
      setError('');
    }
    return getCorrectnessV2CustomerView(analysisId, { selected_lot_id: lotId || undefined }, { signal })
      .then((response) => {
        if (!mountedRef.current) return;
        const data = response.data;
        setPayload(data);
        if (data?.available && data?.report?.report_status === 'LOT_SELECTION_REQUIRED') {
          setLotSelectionReport(data.report);
        }
        if (silent) setError('');
      })
      .catch((err) => {
        if (isCanceledError(err) || !mountedRef.current) return;
        if (err?.response?.status === 404) {
          // Feature disabled or no report for this analysis -> unavailable, not error.
          setPayload({ available: false, reason_code: 'CORRECTNESS_V2_DISABLED' });
          return;
        }
        if (silent) return; // Keep the last good payload on background poll errors.
        // Any other failure is a safe "unavailable" for gating (legacy fallback),
        // and surfaces an error message inside the customer surface.
        setError('Impossibile caricare il report cliente.');
        setPayload({ available: false, reason_code: 'ERROR' });
      })
      .finally(() => {
        if (mountedRef.current && !silent) setLoading(false);
      });
  }, [analysisId]);

  useEffect(() => {
    if (!active) {
      setLoading(false);
      return undefined;
    }
    pollCountRef.current = 0;
    const controller = new AbortController();
    load(selectedLotId, controller.signal);
    return () => controller.abort();
  }, [active, load, selectedLotId]);

  const preparing = Boolean(payload && !payload.available && payload.preparing);

  // Background poll while the backend is preparing the report for us.
  useEffect(() => {
    if (!active || !preparing) return undefined;
    if (pollCountRef.current >= PREPARING_MAX_POLLS) return undefined;
    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => {
      pollCountRef.current += 1;
      load(selectedLotId, controller.signal, { silent: true });
    }, PREPARING_POLL_MS);
    return () => {
      window.clearTimeout(timeoutId);
      controller.abort();
    };
  }, [active, preparing, load, selectedLotId, payload]);

  const report = payload && payload.available ? (payload.report || null) : null;
  const available = Boolean(payload && payload.available && payload.report);
  const isLotSelection = report?.report_status === 'LOT_SELECTION_REQUIRED';
  // A lot was selected but its report is not available (yet): keep the user in
  // the customer surface (message + back to lots) instead of a dead end.
  const lotUnavailable = Boolean(
    selectedLotId && payload && !payload.available && lotSelectionReport
  );

  const selectLot = useCallback((lotId) => setSelectedLotId(lotId), []);
  const backToLots = useCallback(() => setSelectedLotId(null), []);

  return {
    loading,
    error,
    payload,
    report,
    available,
    preparing,
    isLotSelection,
    lotUnavailable,
    lotSelectionReport,
    selectedLotId,
    selectLot,
    backToLots,
  };
};

export default useCorrectnessV2CustomerView;
