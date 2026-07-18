import axios from 'axios';

const API_URL = process.env.REACT_APP_BACKEND_URL;

const correctnessV2Base = (analysisId) => `${API_URL}/api/analysis/perizia/${analysisId}/correctness-v2`;

export const startCorrectnessV2 = (analysisId, options = {}, requestConfig = {}) => {
  const body = {};
  if (options.selected_lot_id) body.selected_lot_id = String(options.selected_lot_id);
  if (options.analyze_all) body.analyze_all = true;
  return axios.post(`${correctnessV2Base(analysisId)}/start`, body, {
    withCredentials: true,
    ...requestConfig
  });
};

export const getCorrectnessV2Job = (analysisId, jobId, requestConfig = {}) => {
  return axios.get(`${correctnessV2Base(analysisId)}/jobs/${jobId}`, {
    withCredentials: true,
    ...requestConfig
  });
};

export const getLatestCorrectnessV2Job = (analysisId, requestConfig = {}) => {
  return axios.get(`${correctnessV2Base(analysisId)}/latest`, {
    withCredentials: true,
    ...requestConfig
  });
};

export const getCorrectnessV2CustomerReport = (analysisId, jobId, requestConfig = {}) => {
  return axios.get(`${correctnessV2Base(analysisId)}/jobs/${jobId}/customer-report`, {
    withCredentials: true,
    ...requestConfig
  });
};

export const getCorrectnessV2LotSelectionReport = (analysisId, jobId, requestConfig = {}) => {
  return axios.get(`${correctnessV2Base(analysisId)}/jobs/${jobId}/lot-selection-report`, {
    withCredentials: true,
    ...requestConfig
  });
};

// Sanitized customer-safe report (no admin/debug/quality/artifact data). Gated
// server-side by the feature flag + ownership; admins may read any analysis.
export const getCorrectnessV2CustomerView = (analysisId, options = {}, requestConfig = {}) => {
  const params = {};
  if (options.selected_lot_id) params.selected_lot_id = String(options.selected_lot_id);
  return axios.get(`${correctnessV2Base(analysisId)}/customer-view/latest`, {
    withCredentials: true,
    params,
    ...requestConfig
  });
};

// Customer-safe lot workspace (Storico lot workspace). Pure read with ZERO
// side effects: per-lot state, safe display values and the authoritative
// credit preview. Never starts jobs.
export const getCorrectnessV2Workspace = (analysisId, requestConfig = {}) => {
  return axios.get(`${correctnessV2Base(analysisId)}/workspace`, {
    withCredentials: true,
    ...requestConfig
  });
};

// Explicit per-lot generation/rerun. This POST is the ONLY customer path that
// may create a job. `force=true` is an explicit rerun; with `force=false` the
// server reuses existing reports/jobs and answers 409
// {detail:{reason_code:"LOT_FAILED_RERUN_REQUIRED"}} on a failed lot.
export const generateCorrectnessV2Lot = (analysisId, lotId, force = false, requestConfig = {}) => {
  return axios.post(
    `${correctnessV2Base(analysisId)}/lots/${lotId}/generate`,
    { force: Boolean(force) },
    { withCredentials: true, ...requestConfig }
  );
};

// Authoritative credit preview for a lot generation/rerun. Read-only. The
// frontend renders these values verbatim and NEVER computes credits itself.
export const getCorrectnessV2LotCreditPreview = (analysisId, lotId, requestConfig = {}) => {
  return axios.get(`${correctnessV2Base(analysisId)}/lots/${lotId}/generate/preview`, {
    withCredentials: true,
    ...requestConfig
  });
};

// Submit the customer's money-confirmation answers (human-in-the-loop money-role
// disambiguation). `answers` is {ambiguity_id: option_id}. Server re-runs the
// gate deterministically (no OpenAI) and returns the final sanitized report.
export const submitCorrectnessV2MoneyConfirmation = (
  analysisId, jobId, answers, requestConfig = {}
) => {
  return axios.post(
    `${correctnessV2Base(analysisId)}/customer-view/confirm-money`,
    { job_id: jobId, answers: answers || {} },
    { withCredentials: true, ...requestConfig }
  );
};
