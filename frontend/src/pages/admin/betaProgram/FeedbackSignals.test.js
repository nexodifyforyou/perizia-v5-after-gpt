import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import axios from 'axios';
import FeedbackTab from './FeedbackTab';
import SignalsTab from './SignalsTab';

jest.mock('axios');
jest.mock('sonner', () => ({ toast: { error: jest.fn(), success: jest.fn(), info: jest.fn() } }));

const renderTab = async (Comp, data) => {
  axios.get.mockResolvedValue({ data });
  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);
  await act(async () => { root.render(<Comp active={true} />); });
  await act(async () => { await Promise.resolve(); });
  return { container, cleanup: async () => { await act(async () => root.unmount()); } };
};

describe('FeedbackTab — verbatim + owner separation', () => {
  test('shows the tester statement verbatim and labels the column as verbatim', async () => {
    const { container, cleanup } = await renderTab(FeedbackTab, {
      items: [{
        id: 'fb1', user_email: 't@example.test', created_at: '2026-07-01T00:00:00Z',
        expert_comment: 'Il report ha sbagliato il valore', feedback_type: 'sbagliato',
        priority: 'media', status: 'new', section_label_it: 'Costi e oneri',
      }],
      total: 1, metrics: { total: 1 },
    });
    expect(container.textContent).toContain('Osservazione (verbatim)');
    const verbatim = container.querySelector('[data-testid="beta-fb-verbatim"]');
    expect(verbatim.textContent).toContain('Il report ha sbagliato il valore');
    await cleanup();
  });

  test('detail drawer separates tester statement from owner interpretation', async () => {
    const { container, cleanup } = await renderTab(FeedbackTab, {
      items: [{
        id: 'fb1', user_email: 't@example.test', created_at: '2026-07-01T00:00:00Z',
        expert_comment: 'Testo del tester', feedback_type: 'sbagliato', priority: 'media',
        status: 'new', section_label_it: 'Costi e oneri',
      }],
      total: 1, metrics: { total: 1 },
    });
    container.querySelector('[data-testid="beta-fb-row"]')
      .dispatchEvent(new MouseEvent('click', { bubbles: true }));
    await act(async () => { await Promise.resolve(); });
    const drawer = container.querySelector('[data-testid="beta-fb-drawer"]');
    expect(drawer.textContent).toContain('Dichiarazione del tester (verbatim)');
    expect(drawer.textContent).toContain('Interpretazione owner');
    expect(drawer.querySelector('[data-testid="beta-fb-drawer-verbatim"]').textContent)
      .toContain('Testo del tester');
    // Owner controls exist and are namespaced separately from tester text.
    expect(drawer.querySelector('[data-testid="beta-fb-owner-category"]')).not.toBeNull();
    expect(drawer.querySelector('[data-testid="beta-fb-owner-priority"]')).not.toBeNull();
    expect(drawer.querySelector('[data-testid="beta-fb-owner-note"]')).not.toBeNull();
    await cleanup();
  });
});

describe('SignalsTab — operational, not tester statements', () => {
  test('labels signals as system-recorded and renders event rows', async () => {
    const { container, cleanup } = await renderTab(SignalsTab, {
      items: [{ event_id: 'v2ev_1', event_type: 'REPORT_READY', analysis_id: 'a1',
                lot_id: null, status: 'REPORT_READY', duration_seconds: 12, created_at: '2026-07-01T00:00:00Z' }],
      total: 1,
    });
    expect(container.textContent).toContain('non sono dichiarazioni del tester');
    expect(container.querySelector('[data-testid="beta-signal-row"]')).not.toBeNull();
    expect(container.textContent).toContain('Report pronto');
    await cleanup();
  });
});
