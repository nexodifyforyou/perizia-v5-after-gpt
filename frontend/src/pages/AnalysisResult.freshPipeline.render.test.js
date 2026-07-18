import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import axios from 'axios';
import AnalysisResult from './AnalysisResult';

// Page-shell acceptance for the V2-only analysis page: the surviving shell
// controls (back link, delete flow, technical feedback gating) keep working,
// the shell reads ONLY the /meta endpoint, and the multi-lot selection flow
// stays entirely inside the Correctness V2 surface.

jest.mock('axios');

const mockNavigate = jest.fn();
jest.mock('react-router-dom', () => {
  const ReactMock = require('react');
  return {
    Link: ({ children, to, ...props }) => <a href={to} {...props}>{children}</a>,
    useParams: () => ({ analysisId: 'test-analysis' }),
    useNavigate: () => mockNavigate,
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

jest.mock('../components/TechnicalFeedbackModal', () => ({ open }) => (
  open ? <div data-testid="technical-feedback-modal" /> : null
));
jest.mock('../components/correctness-v2/CorrectnessV2Panel', () => () => null);

jest.mock('sonner', () => ({
  toast: { error: jest.fn(), success: jest.fn(), info: jest.fn() },
}));

const META_PATH = '/api/analysis/perizia/test-analysis/meta';
const CUSTOMER_VIEW_MARKER = '/correctness-v2/customer-view/latest';

const metaPayload = {
  analysis_id: 'test-analysis',
  case_id: 'case-1',
  case_title: 'Perizia multi-lotto',
  file_name: 'perizia.pdf',
  created_at: '2026-04-24T00:00:00Z',
  pages_count: 33,
  document_hash: 'abc123',
};

const lotSelectionReport = {
  schema_version: 'cv2.customer_report.v1',
  analysis_id: 'test-analysis',
  report_status: 'LOT_SELECTION_REQUIRED',
  report_status_label: 'Selezione del lotto richiesta',
  title: 'Selezione del lotto richiesta',
  decision: { level: 'da_verificare', label: 'Da verificare', headline: 'Da verificare', reason: '', drivers: [] },
  lot_selection: {
    message: 'Selezionare un lotto.',
    lots: [
      { lot_id: '1', label: 'Lotto 1', address: 'Via Uno', property_type: 'Appartamento', money_summary: [] },
      { lot_id: '2', label: 'Lotto 2', address: 'Via Due', property_type: 'Magazzino', money_summary: [] },
    ],
  },
};

const lotOneReport = {
  schema_version: 'cv2.customer_report.v1',
  analysis_id: 'test-analysis',
  job_id: 'cv2_lot1',
  report_status: 'REPORT_READY',
  report_status_label: 'Report pronto',
  title: 'Report Lotto 1',
  decision: { level: 'attenzione', label: 'Attenzione', headline: 'Attenzione.', reason: '', drivers: [] },
  lot_structure: { selected_lot: '1' },
  money_sections: { valuation_chain: [], auction_terms: [], buyer_side_costs: [], procedure_cancelled_formalities: [], uncertain_money: [] },
};

const setupAxios = () => {
  axios.get.mockImplementation((url, config) => {
    if (url.endsWith(META_PATH)) return Promise.resolve({ data: metaPayload });
    if (url.includes(CUSTOMER_VIEW_MARKER)) {
      const lotId = config?.params?.selected_lot_id;
      if (lotId === '1') return Promise.resolve({ data: { available: true, report: lotOneReport } });
      return Promise.resolve({ data: { available: true, report: lotSelectionReport } });
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

const click = async (testId) => {
  const node = byTestId(testId);
  if (!node) throw new Error(`Missing node with test id ${testId}`);
  await act(async () => {
    node.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    await Promise.resolve();
  });
  await act(async () => {
    await Promise.resolve();
  });
};

describe('AnalysisResult page shell + V2 lot flow', () => {
  beforeEach(() => {
    globalThis.IS_REACT_ACT_ENVIRONMENT = true;
    document.body.innerHTML = '';
    jest.clearAllMocks();
    mockUser = { name: 'Test User' };
    setupAxios();
  });

  afterEach(() => {
    if (root) act(() => root.unmount());
    if (container?.parentNode) container.parentNode.removeChild(container);
    container = null;
    root = null;
  });

  test('multi-lot flow lives inside the V2 surface (selector -> lot report)', async () => {
    await renderPage();

    expect(byTestId('cv2-customer-lot-selector')).not.toBeNull();
    expect(byTestId('legacy-report-body')).toBeNull();

    await click('cv2-customer-lot-view-1');
    expect(byTestId('cv2-customer-report')).not.toBeNull();
    expect(container.textContent).toContain('Report Lotto 1');
    expect(byTestId('legacy-report-body')).toBeNull();
  });

  test('delete flow: confirmation modal, DELETE call, navigate to history', async () => {
    await renderPage();

    await click('delete-analysis-btn');
    expect(container.textContent).toContain('Sei sicuro di voler eliminare questa analisi?');

    const buttons = Array.from(container.querySelectorAll('button'));
    const confirm = buttons.filter((b) => b.textContent.includes('Elimina')).pop();
    await act(async () => {
      confirm.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await Promise.resolve();
    });

    expect(axios.delete).toHaveBeenCalledWith(
      expect.stringContaining('/api/analysis/perizia/test-analysis'),
      expect.objectContaining({ withCredentials: true })
    );
    expect(mockNavigate).toHaveBeenCalledWith('/history');
  });

  test('technical feedback control: hidden for normal users, shown for master admin', async () => {
    await renderPage();
    expect(byTestId('share-technical-feedback-btn')).toBeNull();

    act(() => root.unmount());
    container.parentNode.removeChild(container);

    mockUser = { name: 'Owner', is_master_admin: true };
    await renderPage();
    expect(byTestId('share-technical-feedback-btn')).not.toBeNull();

    await click('share-technical-feedback-btn');
    expect(byTestId('technical-feedback-modal')).not.toBeNull();
  });

  test('the shell fetches /meta once and never the old full-payload route', async () => {
    await renderPage();

    const urls = axios.get.mock.calls.map(([url]) => url);
    expect(urls.filter((url) => url.endsWith(META_PATH))).toHaveLength(1);
    urls.forEach((url) => {
      expect(url.endsWith('/api/analysis/perizia/test-analysis')).toBe(false);
      expect(url.includes('/pdf')).toBe(false);
    });
  });
});
