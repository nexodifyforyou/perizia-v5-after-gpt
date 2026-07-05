import axios from 'axios';

const API_URL = process.env.REACT_APP_BACKEND_URL;

export const patchPeriziaHeadline = (analysisId, payload) => {
  return axios.patch(`${API_URL}/api/analysis/perizia/${analysisId}/headline`, payload, {
    withCredentials: true
  });
};

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
