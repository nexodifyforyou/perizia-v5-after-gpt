import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import { useCorrectnessV2CustomerView } from './useCustomerView';
import {
  getCorrectnessV2CustomerView,
  submitCorrectnessV2FindingConfirmation,
} from '../../lib/api/perizia';

jest.mock('../../lib/api/perizia', () => ({
  getCorrectnessV2CustomerView: jest.fn(),
  submitCorrectnessV2MoneyConfirmation: jest.fn(),
  submitCorrectnessV2FindingConfirmation: jest.fn(),
}));

const flush = () => act(async () => { await Promise.resolve(); });

function Harness({ onState }) {
  const state = useCorrectnessV2CustomerView('analysis_1', { enabled: true });
  onState(state);
  return null;
}

function reportPayload(decisionExtra = {}) {
  return {
    available: true,
    report: {
      job_id: 'cv2_1',
      report_status: 'REPORT_READY',
      decision_model: { schema_version: 'cv2.customer_decision.v1', sections: {}, findings: [], ...decisionExtra },
    },
  };
}

async function renderHook() {
  let latest = null;
  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);
  await act(async () => {
    root.render(<Harness onState={(s) => { latest = s; }} />);
  });
  await flush();
  return { get: () => latest, unmount: () => act(() => root.unmount()) };
}

beforeEach(() => {
  jest.clearAllMocks();
  getCorrectnessV2CustomerView.mockResolvedValue({ data: reportPayload() });
});

// 21. submitFindingConfirmation posts and swaps the refreshed report in
test('submitFindingConfirmation posts and swaps the report', async () => {
  submitCorrectnessV2FindingConfirmation.mockResolvedValue({
    data: { available: true, report: { job_id: 'cv2_1', report_status: 'REPORT_READY',
      decision_model: { schema_version: 'cv2.customer_decision.v1', sections: { conferme: { items: [{ finding_id: 'occ-1' }] } }, findings: [] } } },
  });
  const h = await renderHook();
  let ok;
  await act(async () => { ok = await h.get().submitFindingConfirmation('occ-1', 'libero'); });
  expect(ok).toBe(true);
  expect(submitCorrectnessV2FindingConfirmation).toHaveBeenCalledWith('analysis_1', 'cv2_1', 'occ-1', 'libero', undefined);
  expect(h.get().report.decision_model.sections.conferme.items[0].finding_id).toBe('occ-1');
  h.unmount();
});

// 22. failure sets the error and keeps the previous report
test('submit failure sets error and keeps the report', async () => {
  submitCorrectnessV2FindingConfirmation.mockRejectedValue({
    response: { data: { detail: { reason_human: 'Conferma non disponibile.' } } },
  });
  const h = await renderHook();
  let ok;
  await act(async () => { ok = await h.get().submitFindingConfirmation('occ-1', 'libero'); });
  expect(ok).toBe(false);
  expect(h.get().findingConfirmError).toBe('Conferma non disponibile.');
  expect(h.get().report).not.toBeNull();
  h.unmount();
});

// 23. no extra customer-view fetch is triggered by confirming (no polling/job)
test('confirming does not re-fetch the customer view', async () => {
  submitCorrectnessV2FindingConfirmation.mockResolvedValue({
    data: { available: true, report: { job_id: 'cv2_1', report_status: 'REPORT_READY',
      decision_model: { schema_version: 'cv2.customer_decision.v1', sections: {}, findings: [] } } },
  });
  const h = await renderHook();
  const before = getCorrectnessV2CustomerView.mock.calls.length;
  await act(async () => { await h.get().submitFindingConfirmation('occ-1', 'libero'); });
  expect(getCorrectnessV2CustomerView.mock.calls.length).toBe(before);
  h.unmount();
});
