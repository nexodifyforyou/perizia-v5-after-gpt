import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import axios from 'axios';
import AdminBetaProgram from './AdminBetaProgram';

jest.mock('axios');

// Keep the heavy AdminLayout (Sidebar) out of these focused tab tests.
jest.mock('./AdminLayout', () => ({ title, children }) => (
  <div data-testid="admin-layout"><h1>{title}</h1>{children}</div>
));

jest.mock('sonner', () => ({ toast: { error: jest.fn(), success: jest.fn(), info: jest.fn() } }));

const overview = {
  testers: { active: 2, pending: 1, revoked: 0, registered: 2 },
  analyses: { beta_total: 3, unreadable_total: 0 },
  reports: { ready_total: 2, verification_required_total: 0, confirmation_required_total: 0,
             confirmation_completed_total: 0, reused_total: 0, forced_rerun_total: 0,
             service_busy_total: 0, service_unavailable_total: 0, avg_duration_seconds: 12 },
  feedback: { total: 1, new: 1, accepted: 0, high_priority: 0 },
  signals: {},
};

const renderPage = async () => {
  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);
  await act(async () => { root.render(<AdminBetaProgram />); });
  await act(async () => { await Promise.resolve(); });
  return { container, cleanup: () => act(() => root.unmount()) };
};

describe('AdminBetaProgram', () => {
  beforeEach(() => {
    axios.get.mockImplementation((url) => {
      if (url.includes('/overview')) return Promise.resolve({ data: overview });
      if (url.includes('/testers')) return Promise.resolve({ data: { items: [], total: 0, page: 1, page_size: 25 } });
      if (url.includes('/feedback')) return Promise.resolve({ data: { items: [], total: 0, metrics: {} } });
      if (url.includes('/signals')) return Promise.resolve({ data: { items: [], total: 0 } });
      return Promise.resolve({ data: {} });
    });
  });

  test('renders the four tabs', async () => {
    const { container, cleanup } = await renderPage();
    expect(container.querySelector('[data-testid="beta-tab-panoramica"]')).not.toBeNull();
    expect(container.querySelector('[data-testid="beta-tab-tester"]')).not.toBeNull();
    expect(container.querySelector('[data-testid="beta-tab-feedback"]')).not.toBeNull();
    expect(container.querySelector('[data-testid="beta-tab-segnali"]')).not.toBeNull();
    cleanup();
  });

  test('Panoramica loads deterministic overview metrics (no raw Mongo ids)', async () => {
    const { container, cleanup } = await renderPage();
    expect(container.querySelector('[data-testid="beta-overview"]')).not.toBeNull();
    expect(container.textContent).toContain('Attivi');
    // Overview is fed by GET /overview, never by an OpenAI/job call.
    const called = axios.get.mock.calls.map((c) => c[0]);
    expect(called.some((u) => u.includes('/admin/beta-program/overview'))).toBe(true);
    expect(called.every((u) => u.includes('/admin/beta-program/'))).toBe(true);
    cleanup();
  });
});
