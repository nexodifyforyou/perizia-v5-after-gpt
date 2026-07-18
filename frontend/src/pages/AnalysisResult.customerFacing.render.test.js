import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import axios from 'axios';
import AnalysisResult from './AnalysisResult';

// Page-level tests for the customer-facing analysis page.
//
// The Correctness V2 surface is the ONLY report surface. These tests drive the
// REAL page + tabs + customer view through a mocked axios layer and assert,
// for every customer state:
//   * the correct copy renders and the page is never blank;
//   * no legacy report body / reveal toggle / print / download control exists;
//   * the network layer fetches /meta exactly once and NEVER the old
//     full-payload route or any /pdf route.

jest.mock('axios');

jest.mock('react-router-dom', () => {
  const ReactMock = require('react');
  return {
    Link: ({ children, to, ...props }) => <a href={to} {...props}>{children}</a>,
    useParams: () => ({ analysisId: 'test-analysis' }),
    useNavigate: () => jest.fn(),
    // Stateful search-params mock so the URL-persisted `?lot=` selection works
    // inside tests exactly like in the real router.
    useSearchParams: () => {
      const [params, setParams] = ReactMock.useState(() => new URLSearchParams(''));
      const set = ReactMock.useCallback((updater) => {
        setParams((prev) => new URLSearchParams(
          typeof updater === 'function' ? updater(prev) : updater
        ));
      }, []);
      return [params, set];
    },
  };
}, { virtual: true });

let mockUser = { name: 'Test User' };
jest.mock('../context/AuthContext', () => ({
  useAuth: () => ({
    user: mockUser,
    logout: jest.fn(),
  }),
}));

jest.mock('./Dashboard', () => ({
  Sidebar: () => <div data-testid="sidebar" />,
}));

jest.mock('../components/TechnicalFeedbackModal', () => () => null);

// The Vista admin panel is exercised elsewhere; here we only assert the tab
// exists (or not). Keep it light so admin-tab tests don't fetch job status.
jest.mock('../components/correctness-v2/CorrectnessV2Panel', () => () => (
  <div data-testid="cv2-admin-panel-mock" />
));

jest.mock('sonner', () => ({
  toast: { error: jest.fn(), success: jest.fn(), info: jest.fn() },
}));

const META_PATH = '/api/analysis/perizia/test-analysis/meta';
const FULL_PAYLOAD_PATH_SUFFIX = '/api/analysis/perizia/test-analysis';
const CUSTOMER_VIEW_MARKER = '/correctness-v2/customer-view/latest';

const metaPayload = {
  analysis_id: 'test-analysis',
  case_id: 'case-1',
  case_title: 'Case Title',
  file_name: 'test.pdf',
  created_at: '2026-04-24T00:00:00Z',
  pages_count: 12,
  document_hash: 'abc123',
};

const readyReport = {
  schema_version: 'cv2.customer_report.v1',
  analysis_id: 'test-analysis',
  job_id: 'cv2_ready',
  report_status: 'REPORT_READY',
  report_status_label: 'Report pronto',
  title: 'Report cliente pronto',
  decision: { level: 'attenzione', label: 'Attenzione', headline: 'Attenzione: verifiche necessarie.', reason: '', drivers: [] },
  money_sections: { valuation_chain: [], auction_terms: [], buyer_side_costs: [], procedure_cancelled_formalities: [], uncertain_money: [] },
};

const setupAxios = (customerViewData) => {
  axios.get.mockImplementation((url) => {
    if (url.endsWith(META_PATH)) {
      return Promise.resolve({ data: metaPayload });
    }
    if (url.includes(CUSTOMER_VIEW_MARKER)) {
      if (customerViewData instanceof Error) return Promise.reject(customerViewData);
      return Promise.resolve({ data: customerViewData });
    }
    return Promise.reject(new Error(`Unexpected GET ${url}`));
  });
  axios.delete.mockResolvedValue({ data: { ok: true } });
};

let container;
let root;

const renderPage = async () => {
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
  await act(async () => {
    root.render(<AnalysisResult />);
    await Promise.resolve();
  });
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
};

const byTestId = (testId) => container.querySelector(`[data-testid="${testId}"]`);
const text = () => container.textContent || '';

// The non-negotiable invariants asserted in EVERY state.
const expectNoLegacySurface = () => {
  expect(byTestId('legacy-report-body')).toBeNull();
  expect(byTestId('legacy-report-reveal')).toBeNull();
  expect(byTestId('print-view-btn')).toBeNull();
  expect(byTestId('download-pdf-btn')).toBeNull();
  // The page must never be blank: the V2 surface (or its placeholder) exists.
  expect(byTestId('cv2-customer-tab-panel')).not.toBeNull();
};

const expectSafeNetwork = () => {
  const urls = axios.get.mock.calls.map(([url]) => url);
  const metaCalls = urls.filter((url) => url.endsWith(META_PATH));
  expect(metaCalls).toHaveLength(1);
  urls.forEach((url) => {
    expect(url.endsWith(FULL_PAYLOAD_PATH_SUFFIX)).toBe(false); // old full payload
    expect(url.includes('/pdf')).toBe(false); // legacy pdf endpoints
    expect(url.includes('/html')).toBe(false);
  });
};

describe('AnalysisResult customer-facing page (V2-only surface)', () => {
  beforeEach(() => {
    globalThis.IS_REACT_ACT_ENVIRONMENT = true;
    document.body.innerHTML = '';
    jest.clearAllMocks();
    mockUser = { name: 'Test User' };
  });

  afterEach(() => {
    if (root) act(() => root.unmount());
    if (container?.parentNode) container.parentNode.removeChild(container);
    container = null;
    root = null;
  });

  test('safe V2 report: customer report body renders, no legacy anywhere', async () => {
    setupAxios({ available: true, report: readyReport });
    await renderPage();

    expect(byTestId('cv2-customer-report')).not.toBeNull();
    expect(text()).toContain('Report cliente pronto');
    expectNoLegacySurface();
    expectSafeNetwork();
  });

  test('preparing: preparing copy renders, no legacy, no blank page', async () => {
    setupAxios({ available: false, preparing: true, reason_code: 'PREPARING' });
    await renderPage();

    expect(byTestId('cv2-customer-preparing')).not.toBeNull();
    expect(text()).toContain('Report cliente in preparazione');
    expectNoLegacySurface();
    expectSafeNetwork();
  });

  test('VERIFICATION_REQUIRED: verification copy renders, no internal codes', async () => {
    setupAxios({ available: false, preparing: false, reason_code: 'VERIFICATION_REQUIRED' });
    await renderPage();

    expect(byTestId('cv2-customer-verification-required')).not.toBeNull();
    expect(text()).toContain('Report cliente non disponibile: verifica tecnica richiesta.');
    expect(text()).not.toContain('CONTRACT_VALIDATION_FAILED');
    expectNoLegacySurface();
    expectSafeNetwork();
  });

  test('SERVICE_BUSY: busy copy renders, no internal codes', async () => {
    setupAxios({ available: false, preparing: false, reason_code: 'SERVICE_BUSY' });
    await renderPage();

    expect(byTestId('cv2-customer-service-busy')).not.toBeNull();
    expect(text()).toContain(
      "Il servizio è momentaneamente occupato e non disponibile. Riprova tra qualche minuto oppure contatta l'amministratore."
    );
    expect(text()).not.toContain('OPENAI_QUOTA_EXHAUSTED');
    expectNoLegacySurface();
    expectSafeNetwork();
  });

  test('SERVICE_UNAVAILABLE: safe message and retry render, no blank page', async () => {
    setupAxios({ available: false, preparing: false, reason_code: 'SERVICE_UNAVAILABLE' });
    await renderPage();

    expect(byTestId('cv2-customer-service-unavailable')).not.toBeNull();
    expect(text()).toContain('Il servizio non è al momento disponibile. Riprova più tardi.');
    expect(byTestId('cv2-customer-retry')).not.toBeNull();
    expectNoLegacySurface();
    expectSafeNetwork();
  });

  test('NO_REPORT (historical analysis): exact copy renders, no legacy fallback', async () => {
    setupAxios({ available: false, preparing: false, reason_code: 'NO_REPORT' });
    await renderPage();

    expect(byTestId('cv2-customer-unavailable')).not.toBeNull();
    expect(text()).toContain('Il nuovo report cliente non è ancora disponibile per questa analisi.');
    expectNoLegacySurface();
    expectSafeNetwork();
  });

  test('customer-view fetch failure: service-unavailable state, never a blank page', async () => {
    setupAxios(new Error('network down'));
    await renderPage();

    expect(byTestId('cv2-customer-service-unavailable')).not.toBeNull();
    expectNoLegacySurface();
    expectSafeNetwork();
  });

  test('normal customer: no Vista admin tab, no admin panel', async () => {
    setupAxios({ available: true, report: readyReport });
    await renderPage();

    expect(byTestId('cv2-tab-admin')).toBeNull();
    expect(byTestId('cv2-admin-panel-mock')).toBeNull();
    expectNoLegacySurface();
  });

  test('exact admin: Vista admin tab present, still no legacy reveal control', async () => {
    mockUser = { name: 'Owner', is_master_admin: true, correctness_v2_admin_view: true };
    setupAxios({ available: true, report: readyReport });
    await renderPage();

    expect(byTestId('cv2-tab-admin')).not.toBeNull();
    expect(byTestId('legacy-report-reveal')).toBeNull();
    expect(byTestId('legacy-report-body')).toBeNull();
    expectSafeNetwork();
  });

  test('page shell renders metadata only (title, case, pages, date)', async () => {
    setupAxios({ available: false, preparing: false, reason_code: 'NO_REPORT' });
    await renderPage();

    expect(text()).toContain('Case Title');
    expect(text()).toContain('Case: case-1');
    expect(text()).toContain('12 pagine');
    expect(text()).toContain('Torna allo storico');
    expect(byTestId('delete-analysis-btn')).not.toBeNull();
  });
});
