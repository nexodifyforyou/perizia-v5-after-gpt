import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import axios from 'axios';
import History from './History';

// Storico (plan §G): each perizia card renders the additive `v2` progress
// summary from the backend history rows — a line like
// "6 lotti · 4 pronti · 1 da verificare · 1 non analizzato" (zero categories
// omitted) — while the legacy SemaforoBadge stays (additive).

jest.mock('axios');

jest.mock('react-router-dom', () => ({
  Link: ({ children, to, ...props }) => <a href={to} {...props}>{children}</a>,
}), { virtual: true });

jest.mock('../context/AuthContext', () => ({
  useAuth: () => ({ user: { name: 'Test User' }, logout: jest.fn() }),
}));

jest.mock('./Dashboard', () => ({
  Sidebar: () => <div data-testid="sidebar" />,
  SemaforoBadge: () => <span data-testid="semaforo-badge" />,
}));

jest.mock('sonner', () => ({
  toast: { error: jest.fn(), success: jest.fn(), info: jest.fn() },
}));

const periziaRows = [
  {
    analysis_id: 'a-multi',
    case_id: 'case-multi',
    case_title: 'Perizia multi-lotto',
    file_name: 'multi.pdf',
    created_at: '2026-07-10T10:00:00Z',
    semaforo_status: 'giallo',
    v2: {
      state: 'LOT_OVERVIEW',
      lot_count: 6,
      ready: 4,
      preparing: 0,
      confirmation_required: 0,
      verification_required: 1,
      failed: 0,
      not_analyzed: 1,
    },
  },
  {
    analysis_id: 'a-legacy',
    case_id: 'case-legacy',
    case_title: 'Perizia storica senza v2',
    file_name: 'legacy.pdf',
    created_at: '2026-01-05T10:00:00Z',
    semaforo_status: 'verde',
    // no v2 field: older rows stay fully functional.
  },
];

const setupAxios = () => {
  axios.get.mockImplementation((url) => {
    if (url.includes('/api/history/perizia')) {
      return Promise.resolve({ data: { analyses: periziaRows } });
    }
    if (url.includes('/api/history/images')) {
      return Promise.resolve({ data: { forensics: [] } });
    }
    if (url.includes('/api/history/assistant')) {
      return Promise.resolve({ data: { conversations: [] } });
    }
    return Promise.reject(new Error(`Unexpected GET ${url}`));
  });
};

let container;
let root;

const renderPage = async () => {
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
  await act(async () => {
    root.render(<History />);
    await Promise.resolve();
  });
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
};

const byTestId = (testId) => container.querySelector(`[data-testid="${testId}"]`);

describe('History — Storico v2 lot summary', () => {
  beforeEach(() => {
    globalThis.IS_REACT_ACT_ENVIRONMENT = true;
    document.body.innerHTML = '';
    jest.clearAllMocks();
    setupAxios();
  });

  afterEach(() => {
    if (root) act(() => root.unmount());
    if (container?.parentNode) container.parentNode.removeChild(container);
    container = null;
    root = null;
  });

  test('renders the v2 progress line with zero categories omitted', async () => {
    await renderPage();

    const summary = byTestId('history-v2-summary-a-multi');
    expect(summary).not.toBeNull();
    expect(summary.textContent).toContain('6 lotti · 4 pronti · 1 da verificare · 1 non analizzato');
    // Zero categories are omitted.
    expect(summary.textContent).not.toContain('in preparazione');
    expect(summary.textContent).not.toContain('non completat');
  });

  test('rows without a v2 summary render normally, without the progress line', async () => {
    await renderPage();

    expect(byTestId('history-item-a-legacy')).not.toBeNull();
    expect(byTestId('history-v2-summary-a-legacy')).toBeNull();
  });

  test('the legacy SemaforoBadge stays (additive) and cards still link to /analysis/:id', async () => {
    await renderPage();

    expect(container.querySelectorAll('[data-testid="semaforo-badge"]').length).toBeGreaterThan(0);
    const link = container.querySelector('a[href="/analysis/a-multi"]');
    expect(link).not.toBeNull();
  });
});
