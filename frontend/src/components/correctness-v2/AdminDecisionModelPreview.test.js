import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import { AdminDecisionModelPreview } from './AdminDecisionModelPreview';
import { getCorrectnessV2DecisionModel } from '../../lib/api/perizia';

jest.mock('../../lib/api/perizia', () => ({
  getCorrectnessV2DecisionModel: jest.fn(),
}));

const flush = () => act(async () => { await Promise.resolve(); await Promise.resolve(); });

async function mount(ui) {
  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);
  await act(async () => { root.render(ui); });
  await flush();
  return { container, unmount: () => act(() => root.unmount()) };
}

const payload = {
  data: {
    analysis_id: 'analysis_1',
    job_id: 'cv2_1',
    decision_model: {
      esito: { level: 'ambra' },
      readiness: { state: 'CONFIRMATIONS_REQUIRED', label: 'Conferme necessarie' },
      findings: [{ finding_id: 'occ-1' }],
    },
    confirmations: [
      { confirmation_id: 'cnf_1', finding_id: 'occ-1', selected_label: 'Libero', status: 'confermato_utente', page: 3, source: 'USER_CONFIRMED' },
    ],
    audit: [{ audit_id: 'aud_1', at: 't', action: 'created', from_option: null, to_option: 'libero' }],
  },
};

// 25. admin preview renders decision model + readiness (raw enum only in admin)
test('renders decision model readiness and confirmations', async () => {
  getCorrectnessV2DecisionModel.mockResolvedValue(payload);
  const { container, unmount } = await mount(<AdminDecisionModelPreview analysisId="analysis_1" jobId="cv2_1" />);
  const block = container.querySelector('[data-testid="cv2-admin-decision-model"]');
  expect(block).not.toBeNull();
  expect(block.textContent).toContain('CONFIRMATIONS_REQUIRED');
  // 26. original-vs-confirmed diff visible
  expect(block.textContent).toContain('occ-1');
  expect(block.textContent).toContain('Libero');
  unmount();
});

// 27. silently hides when the admin route is not authorized / errors
test('hides on error (non-admin)', async () => {
  getCorrectnessV2DecisionModel.mockRejectedValue({ response: { status: 403 } });
  const { container, unmount } = await mount(<AdminDecisionModelPreview analysisId="analysis_1" jobId="cv2_1" />);
  expect(container.querySelector('[data-testid="cv2-admin-decision-model"]')).toBeNull();
  unmount();
});
