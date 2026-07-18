import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import axios from 'axios';
import AnalysisResult from './AnalysisResult';
import {
  getCorrectnessV2CustomerView,
  getCorrectnessV2Workspace,
  generateCorrectnessV2Lot,
  getCorrectnessV2LotCreditPreview,
} from '../lib/api/perizia';

// Page-level tests for the Storico lot workspace:
//   * a multi-lot analysis lands on the LOT OVERVIEW, never a lot report;
//   * opening a lot sets `?lot=` and shows the stored report (no generation);
//   * "Torna ai lotti" is a pure URL change: ZERO API calls, ZERO jobs;
//   * a `?lot=` deep link (refresh / back / forward) opens that lot directly;
//   * explicit generate goes through the confirmation modal with the backend
//     credit preview;
//   * when the workspace endpoint is unavailable the page falls back to the
//     pre-workspace customer-view behavior.

jest.mock('axios');

jest.mock('../lib/api/perizia', () => ({
  getCorrectnessV2CustomerView: jest.fn(),
  getCorrectnessV2Workspace: jest.fn(),
  generateCorrectnessV2Lot: jest.fn(),
  getCorrectnessV2LotCreditPreview: jest.fn(),
  submitCorrectnessV2MoneyConfirmation: jest.fn(),
}));

let mockInitialSearch = '';
jest.mock('react-router-dom', () => {
  const ReactMock = require('react');
  return {
    Link: ({ children, to, ...props }) => <a href={to} {...props}>{children}</a>,
    useParams: () => ({ analysisId: 'test-analysis' }),
    useNavigate: () => jest.fn(),
    // Stateful search-params mock so `?lot=` behaves like the real router.
    useSearchParams: () => {
      const [params, setParams] = ReactMock.useState(() => new URLSearchParams(mockInitialSearch));
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
  useAuth: () => ({ user: mockUser, logout: jest.fn() }),
}));

jest.mock('./Dashboard', () => ({
  Sidebar: () => <div data-testid="sidebar" />,
}));

jest.mock('../components/TechnicalFeedbackModal', () => () => null);
jest.mock('../components/correctness-v2/CorrectnessV2Panel', () => () => null);

jest.mock('sonner', () => ({
  toast: { error: jest.fn(), success: jest.fn(), info: jest.fn() },
}));

const metaPayload = {
  analysis_id: 'test-analysis',
  case_id: 'case-1',
  case_title: 'Perizia multi-lotto',
  file_name: 'perizia.pdf',
  created_at: '2026-04-24T00:00:00Z',
  pages_count: 33,
  document_hash: 'abc123',
};

const creditPreview = {
  can_start: true,
  will_consume_credit: false,
  credits_required: 0,
  available_credits: 12,
  already_paid_at_upload: true,
  exempt: false,
  reason: null,
};

const workspacePayload = {
  analysis_id: 'test-analysis',
  multi_lot: true,
  lot_count: 2,
  analysis_state: 'LOT_OVERVIEW',
  summary: { lot_count: 2, ready: 1, preparing: 0, confirmation_required: 0, verification_required: 0, failed: 0, not_analyzed: 1 },
  lots: [
    {
      lot_id: '1', label: 'Lotto 1', address: 'Via Uno', property_type: 'Appartamento',
      final_value: '€ 38.110,20', state: 'REPORT_READY', has_safe_report: true,
      job_running: false, last_attempt_failed: false,
      latest_report_at: '2026-07-10T10:00:00Z', report_version: 1,
      actions: ['open_report', 'rerun'],
    },
    {
      lot_id: '2', label: 'Lotto 2', address: 'Via Due', property_type: 'Magazzino',
      state: 'NOT_ANALYZED', has_safe_report: false, job_running: false,
      last_attempt_failed: false, latest_report_at: null, report_version: null,
      actions: ['generate'],
    },
  ],
  credit_preview: creditPreview,
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

const setupMocks = ({ workspaceAvailable = true } = {}) => {
  axios.get.mockImplementation((url) => {
    if (url.endsWith('/api/analysis/perizia/test-analysis/meta')) {
      return Promise.resolve({ data: metaPayload });
    }
    return Promise.reject(new Error(`Unexpected GET ${url}`));
  });
  axios.delete.mockResolvedValue({ data: { ok: true } });

  if (workspaceAvailable) {
    getCorrectnessV2Workspace.mockResolvedValue({ data: workspacePayload });
  } else {
    getCorrectnessV2Workspace.mockRejectedValue({ response: { status: 404 } });
  }
  getCorrectnessV2CustomerView.mockImplementation((analysisId, options = {}) => {
    if (options?.selected_lot_id === '1') {
      return Promise.resolve({ data: { available: true, report: lotOneReport } });
    }
    return Promise.resolve({ data: { available: true, report: lotSelectionReport } });
  });
  getCorrectnessV2LotCreditPreview.mockResolvedValue({ data: { ...creditPreview, lot_state: 'NOT_ANALYZED', can_start: true } });
  generateCorrectnessV2Lot.mockResolvedValue({ data: { spawned: true, state: 'RUNNING', preparing: true, job_id: 'cv2_new' } });
};

let container;
let root;

const flush = async () => {
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();
  });
};

const renderPage = async () => {
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
  await act(async () => {
    root.render(<AnalysisResult />);
    await Promise.resolve();
  });
  await flush();
};

const byTestId = (testId) => container.querySelector(`[data-testid="${testId}"]`);
const text = () => container.textContent || '';

const click = async (testId) => {
  const node = byTestId(testId);
  if (!node) throw new Error(`Missing node with test id ${testId}`);
  await act(async () => {
    node.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    await Promise.resolve();
  });
  await flush();
};

const apiCallCounts = () => ({
  customerView: getCorrectnessV2CustomerView.mock.calls.length,
  workspace: getCorrectnessV2Workspace.mock.calls.length,
  generate: generateCorrectnessV2Lot.mock.calls.length,
  preview: getCorrectnessV2LotCreditPreview.mock.calls.length,
});

describe('AnalysisResult — Storico lot workspace', () => {
  beforeEach(() => {
    globalThis.IS_REACT_ACT_ENVIRONMENT = true;
    document.body.innerHTML = '';
    jest.clearAllMocks();
    mockUser = { name: 'Test User' };
    mockInitialSearch = '';
    setupMocks();
  });

  afterEach(() => {
    if (root) act(() => root.unmount());
    if (container?.parentNode) container.parentNode.removeChild(container);
    container = null;
    root = null;
  });

  test('a multi-lot analysis lands on the lot overview, never a lot report', async () => {
    await renderPage();

    expect(byTestId('cv2-lot-workspace')).not.toBeNull();
    expect(byTestId('cv2-customer-report')).toBeNull();
    expect(text()).toContain('2 lotti · 1 pronto · 1 non analizzato');
    // Landing on the overview creates nothing.
    expect(generateCorrectnessV2Lot).not.toHaveBeenCalled();
    // "Torna allo storico" stays available.
    expect(text()).toContain('Torna allo storico');
  });

  test('"Apri report" opens the stored lot report via ?lot= with zero generation', async () => {
    await renderPage();

    await click('cv2-lot-open-1');

    expect(byTestId('cv2-customer-report')).not.toBeNull();
    expect(text()).toContain('Report Lotto 1');
    expect(text()).toContain('Torna ai lotti');
    // The report came from the side-effect-free customer-view GET for lot 1.
    const lastCall = getCorrectnessV2CustomerView.mock.calls.at(-1);
    expect(lastCall[1]).toEqual({ selected_lot_id: '1' });
    expect(generateCorrectnessV2Lot).not.toHaveBeenCalled();
  });

  test('"Torna ai lotti" returns to the overview with ZERO API/job calls', async () => {
    await renderPage();
    await click('cv2-lot-open-1');
    expect(byTestId('cv2-customer-report')).not.toBeNull();

    const before = apiCallCounts();
    await click('cv2-customer-back-to-lots');

    expect(byTestId('cv2-lot-workspace')).not.toBeNull();
    expect(byTestId('cv2-customer-report')).toBeNull();
    expect(apiCallCounts()).toEqual(before);
  });

  test('a ?lot= deep link (refresh / back / forward) opens that lot directly', async () => {
    mockInitialSearch = 'lot=1';
    await renderPage();

    expect(byTestId('cv2-customer-report')).not.toBeNull();
    expect(text()).toContain('Report Lotto 1');
    expect(byTestId('cv2-lot-workspace')).toBeNull();
    expect(generateCorrectnessV2Lot).not.toHaveBeenCalled();
  });

  test('explicit generate: confirmation modal with backend credit preview, one POST', async () => {
    await renderPage();

    await click('cv2-lot-generate-2');
    expect(text()).toContain('Generare il report del lotto?');
    expect(byTestId('cv2-lot-credit-preview').textContent).toContain("0 crediti · già incluso nell'analisi");
    expect(byTestId('cv2-lot-credit-preview').textContent).toContain('Crediti disponibili: 12');
    expect(generateCorrectnessV2Lot).not.toHaveBeenCalled();

    await click('cv2-lot-generate-confirm');
    expect(generateCorrectnessV2Lot).toHaveBeenCalledTimes(1);
    expect(generateCorrectnessV2Lot).toHaveBeenCalledWith('test-analysis', '2', false);
  });

  test('workspace unavailable (rollout 404): falls back to the customer-view flow', async () => {
    setupMocks({ workspaceAvailable: false });
    await renderPage();

    expect(byTestId('cv2-lot-workspace')).toBeNull();
    // The pre-workspace surface renders (lot selector from the customer view).
    expect(byTestId('cv2-customer-lot-selector')).not.toBeNull();
    expect(generateCorrectnessV2Lot).not.toHaveBeenCalled();
  });
});
