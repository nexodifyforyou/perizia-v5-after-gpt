import { useCallback, useEffect, useRef, useState } from 'react';
import { getCorrectnessV2Workspace } from '../../lib/api/perizia';

// Storico lot workspace state: the customer-safe per-lot overview for an
// analysis. The workspace GET is a pure read with ZERO side effects (it can
// never start a job), so fetching / refreshing / polling it is always safe.
//
// Availability contract:
//   - `resolved`  the first fetch has settled (success OR failure);
//   - `available` the workspace endpoint answered — when false (404 rollout /
//     transient failure) the page falls back to the pre-workspace customer
//     view behavior, never a blank page.
//
// While any lot is RUNNING the hook silently re-polls the workspace so the
// overview cards update in place (no duplicate starts: polling is read-only).

const WORKSPACE_POLL_MS = 8000;
const WORKSPACE_MAX_POLLS = 225; // ~30 minutes of background polling

const isCanceledError = (error) => (
  error?.code === 'ERR_CANCELED' ||
  error?.name === 'CanceledError' ||
  error?.message === 'canceled'
);

export const useLotWorkspace = (analysisId, { enabled = true } = {}) => {
  const active = Boolean(enabled && analysisId);
  const [loading, setLoading] = useState(active);
  const [resolved, setResolved] = useState(!active);
  const [available, setAvailable] = useState(false);
  const [workspace, setWorkspace] = useState(null);
  const mountedRef = useRef(false);
  const pollCountRef = useRef(0);
  const [reloadTick, setReloadTick] = useState(0);

  useEffect(() => {
    mountedRef.current = true;
    return () => { mountedRef.current = false; };
  }, []);

  const load = useCallback((signal, { silent = false } = {}) => {
    if (!silent) setLoading(true);
    return getCorrectnessV2Workspace(analysisId, { signal })
      .then((response) => {
        if (!mountedRef.current) return;
        setWorkspace(response.data || null);
        setAvailable(Boolean(response.data));
      })
      .catch((err) => {
        if (isCanceledError(err) || !mountedRef.current) return;
        if (silent) return; // Keep the last good workspace on background poll errors.
        // 404 (feature not rolled out) or any other failure -> fall back to
        // the pre-workspace customer view; never an error page.
        setWorkspace(null);
        setAvailable(false);
      })
      .finally(() => {
        if (!mountedRef.current) return;
        setResolved(true);
        if (!silent) setLoading(false);
      });
  }, [analysisId]);

  useEffect(() => {
    if (!active) {
      setLoading(false);
      setResolved(true);
      return undefined;
    }
    pollCountRef.current = 0;
    const controller = new AbortController();
    load(controller.signal);
    return () => controller.abort();
  }, [active, load, reloadTick]);

  // A lot is being generated: keep the overview fresh with silent read-only
  // polls (never a duplicate start — the GET has no side effects).
  const preparing = Boolean(
    available && workspace && (
      Number(workspace?.summary?.preparing) > 0 ||
      (Array.isArray(workspace.lots) && workspace.lots.some(
        (lot) => lot?.state === 'RUNNING' || lot?.job_running
      ))
    )
  );

  useEffect(() => {
    if (!active || !preparing) return undefined;
    if (pollCountRef.current >= WORKSPACE_MAX_POLLS) return undefined;
    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => {
      pollCountRef.current += 1;
      load(controller.signal, { silent: true });
    }, WORKSPACE_POLL_MS);
    return () => {
      window.clearTimeout(timeoutId);
      controller.abort();
    };
  }, [active, preparing, load, workspace]);

  // Visible re-fetch (customer retry action).
  const reload = useCallback(() => setReloadTick((tick) => tick + 1), []);

  // Immediate silent re-fetch, e.g. right after an explicit generate/rerun so
  // the lot card flips to "in preparazione" without a loading flash.
  const refresh = useCallback(() => load(undefined, { silent: true }), [load]);

  return { loading, resolved, available, workspace, preparing, reload, refresh };
};

export default useLotWorkspace;
