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
  const mountedRef = useRef(false);

  useEffect(() => {
    mountedRef.current = true;
    return () => { mountedRef.current = false; };
  }, []);

  const load = useCallback((lotId, signal) => {
    setLoading(true);
    setError('');
    return getCorrectnessV2CustomerView(analysisId, { selected_lot_id: lotId || undefined }, { signal })
      .then((response) => {
        if (!mountedRef.current) return;
        setPayload(response.data);
      })
      .catch((err) => {
        if (isCanceledError(err) || !mountedRef.current) return;
        if (err?.response?.status === 404) {
          // Feature disabled or no report for this analysis -> unavailable, not error.
          setPayload({ available: false, reason_code: 'CORRECTNESS_V2_DISABLED' });
          return;
        }
        // Any other failure is a safe "unavailable" for gating (legacy fallback),
        // and surfaces an error message inside the customer surface.
        setError('Impossibile caricare il report cliente.');
        setPayload({ available: false, reason_code: 'ERROR' });
      })
      .finally(() => {
        if (mountedRef.current) setLoading(false);
      });
  }, [analysisId]);

  useEffect(() => {
    if (!active) {
      setLoading(false);
      return undefined;
    }
    const controller = new AbortController();
    load(selectedLotId, controller.signal);
    return () => controller.abort();
  }, [active, load, selectedLotId]);

  const report = payload && payload.available ? (payload.report || null) : null;
  const available = Boolean(payload && payload.available && payload.report);
  const isLotSelection = report?.report_status === 'LOT_SELECTION_REQUIRED';

  const selectLot = useCallback((lotId) => setSelectedLotId(lotId), []);
  const backToLots = useCallback(() => setSelectedLotId(null), []);

  return {
    loading,
    error,
    payload,
    report,
    available,
    isLotSelection,
    selectedLotId,
    selectLot,
    backToLots,
  };
};

export default useCorrectnessV2CustomerView;
