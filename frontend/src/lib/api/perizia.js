import axios from 'axios';

const API_URL = process.env.REACT_APP_BACKEND_URL;

export const patchPeriziaHeadline = (analysisId, payload) => {
  return axios.patch(`${API_URL}/api/analysis/perizia/${analysisId}/headline`, payload, {
    withCredentials: true
  });
};
