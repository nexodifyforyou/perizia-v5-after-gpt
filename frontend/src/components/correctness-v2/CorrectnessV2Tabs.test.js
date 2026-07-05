import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import CorrectnessV2Tabs from './CorrectnessV2Tabs';
import {
  getCorrectnessV2CustomerView,
  getLatestCorrectnessV2Job,
} from '../../lib/api/perizia';

jest.mock('../../lib/api/perizia', () => ({
  getCorrectnessV2CustomerView: jest.fn(),
  getCorrectnessV2CustomerReport: jest.fn(),
  getCorrectnessV2Job: jest.fn(),
  getLatestCorrectnessV2Job: jest.fn(),
  startCorrectnessV2: jest.fn(),
}));

let container;
let root;

const flush = async () => {
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();
  });
};

const render = async (props = {}) => {
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
  await act(async () => {
    root.render(<CorrectnessV2Tabs analysisId="analysis_generic" {...props} />);
  });
  await flush();
};

const click = async (selector) => {
  const node = container.querySelector(selector);
  if (!node) throw new Error(`Missing ${selector}`);
  await act(async () => {
    node.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    await Promise.resolve();
  });
  await flush();
};

const text = () => container.textContent || '';

describe('CorrectnessV2Tabs', () => {
  beforeEach(() => {
    globalThis.IS_REACT_ACT_ENVIRONMENT = true;
    document.body.innerHTML = '';
    jest.clearAllMocks();
    getCorrectnessV2CustomerView.mockResolvedValue({ data: { available: false } });
    getLatestCorrectnessV2Job.mockRejectedValue({ response: { status: 404 } });
  });

  afterEach(() => {
    if (root) act(() => root.unmount());
    if (container?.parentNode) container.parentNode.removeChild(container);
    container = null;
    root = null;
  });

  test('shows only the customer tab when the admin tab is not permitted', async () => {
    // Normal customer / other admin: sees Report cliente, never Vista admin.
    await render({ canSeeAdminTab: false });
    expect(container.querySelector('[data-testid="cv2-tab-customer"]')).toBeTruthy();
    expect(container.querySelector('[data-testid="cv2-tab-admin"]')).toBeNull();
    expect(container.querySelector('[data-testid="cv2-customer-tab-panel"]')).toBeTruthy();
    // No run/rerun control or quality table is reachable from the customer tab.
    expect(container.querySelector('[data-testid="run-correctness-v2-button"]')).toBeNull();
    expect(container.querySelector('[data-testid="cv2-quality-control"]')).toBeNull();
    // Customer view fetches the sanitized endpoint, never the admin latest-job.
    expect(getCorrectnessV2CustomerView).toHaveBeenCalled();
    expect(getLatestCorrectnessV2Job).not.toHaveBeenCalled();
  });

  test('reveals the admin tab and switches to the full panel when permitted', async () => {
    // Exact-email admin: sees both tabs, default is still Report cliente.
    await render({ canSeeAdminTab: true });
    expect(container.querySelector('[data-testid="cv2-tab-admin"]')).toBeTruthy();
    expect(container.querySelector('[data-testid="cv2-customer-tab-panel"]')).toBeTruthy();
    // Run controls live only inside the admin tab, never on the customer tab.
    expect(container.querySelector('[data-testid="run-correctness-v2-button"]')).toBeNull();

    await click('[data-testid="cv2-tab-admin"]');

    expect(container.querySelector('[data-testid="cv2-admin-tab-panel"]')).toBeTruthy();
    expect(container.querySelector('[data-testid="run-correctness-v2-button"]')).toBeTruthy();
    expect(getLatestCorrectnessV2Job).toHaveBeenCalled();
  });

  test('uses the supplied customerState instead of self-fetching', async () => {
    const customerState = {
      loading: false,
      error: '',
      payload: { available: true },
      report: {
        report_status: 'REPORT_READY',
        title: 'Report cliente Torino',
        decision: { level: 'da_verificare', label: 'Da verificare', headline: 'Da verificare', drivers: [] },
      },
      available: true,
      isLotSelection: false,
      selectedLotId: null,
      selectLot: jest.fn(),
      backToLots: jest.fn(),
    };
    await render({ canSeeAdminTab: false, customerState });
    expect(container.querySelector('[data-testid="cv2-customer-report"]')).toBeTruthy();
    expect(text()).toContain('Report cliente Torino');
    // The shared state was passed in, so the endpoint is not re-fetched here.
    expect(getCorrectnessV2CustomerView).not.toHaveBeenCalled();
  });
});
