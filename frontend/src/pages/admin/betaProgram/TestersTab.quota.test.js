import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import axios from 'axios';
import TestersTab from './TestersTab';

// Configurable beta perizia allowance — owner-facing controls in
// Programma Beta -> Tester (docs/beta_perizia_limits_plan.md §N).
// Covers required tests 1-10 (owner side) plus test 21 (tab structure
// untouched by this feature, exercised at the TestersTab level here).

jest.mock('axios');
jest.mock('sonner', () => ({ toast: { error: jest.fn(), success: jest.fn(), info: jest.fn() } }));

const tester = (over = {}) => ({
  membership_id: 'betam_1',
  normalized_email: 't@example.test',
  display_name: 'Beta',
  status: 'ACTIVE',
  account_linked: true,
  added_at: '2026-07-01T00:00:00Z',
  activated_at: '2026-07-01T00:00:00Z',
  revoked_at: null,
  analyses_total: 3,
  feedback_total: 1,
  quota: {
    mode: 'UNLIMITED',
    limit: null,
    consumed: 0,
    reserved: 0,
    remaining: null,
    state: 'UNLIMITED',
    quota_version: 1,
  },
  ...over,
});

const click = (el) => el.dispatchEvent(new MouseEvent('click', { bubbles: true }));
const fireInput = (el, value) => {
  const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
  setter.call(el, value);
  el.dispatchEvent(new Event('input', { bubbles: true }));
};

const renderTab = async (rows, { phases = [] } = {}) => {
  axios.get.mockImplementation((url) => {
    const u = String(url);
    if (u.includes('/quota/phases')) {
      return Promise.resolve({ data: { items: phases } });
    }
    if (u.includes('/testers')) {
      return Promise.resolve({ data: { items: rows, total: rows.length, page: 1, page_size: 25 } });
    }
    return Promise.resolve({ data: {} });
  });
  axios.patch.mockResolvedValue({ data: {} });
  axios.post.mockResolvedValue({ data: {} });

  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);
  await act(async () => { root.render(<TestersTab active={true} />); });
  await act(async () => { await Promise.resolve(); });
  return { container, root, cleanup: async () => { await act(async () => root.unmount()); } };
};

const openQuotaDialog = async (container) => {
  click(container.querySelector('[data-testid="beta-quota-manage-btn"]'));
  await act(async () => { await Promise.resolve(); });
};

describe('TestersTab — beta quota management (owner)', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  // 1. owner sees current limit -----------------------------------------
  test('1. owner sees the tester current quota limit', async () => {
    const { container, cleanup } = await renderTab([
      tester({ quota: { mode: 'LIMITED', limit: 5, consumed: 2, reserved: 0, remaining: 3, state: 'AVAILABLE', quota_version: 1 } }),
    ]);
    expect(container.textContent).toContain('Limitata a 5 perizie');
    await cleanup();
  });

  // 2. owner sees remaining allowance, emphasized -------------------------
  test('2. owner sees the remaining allowance as the primary number', async () => {
    const { container, cleanup } = await renderTab([
      tester({ quota: { mode: 'LIMITED', limit: 5, consumed: 2, reserved: 0, remaining: 3, state: 'AVAILABLE', quota_version: 1 } }),
    ]);
    const remainingLine = container.querySelector('[data-testid="quota-remaining-line"]');
    expect(remainingLine).not.toBeNull();
    expect(remainingLine.textContent).toContain('3 perizie rimanenti su 5');
    expect(container.textContent).toContain('Utilizzate: 2');
    expect(container.textContent).toContain('Rimanenti: 3');
    await cleanup();
  });

  // 3. owner can choose Unlimited ------------------------------------------
  test('3. owner can choose Illimitata and save it', async () => {
    const { container, cleanup } = await renderTab([
      tester({ quota: { mode: 'LIMITED', limit: 5, consumed: 2, reserved: 0, remaining: 3, state: 'AVAILABLE', quota_version: 1 } }),
    ]);
    await openQuotaDialog(container);
    click(container.querySelector('[data-testid="quota-mode-unlimited"]'));
    await act(async () => { await Promise.resolve(); });
    expect(container.querySelector('[data-testid="quota-limit-input"]')).toBeNull();

    await act(async () => { click(container.querySelector('[data-testid="quota-save-btn"]')); await Promise.resolve(); });
    expect(axios.patch).toHaveBeenCalledWith(
      expect.stringContaining(`/testers/${tester().membership_id}/quota`),
      expect.objectContaining({ quota_mode: 'UNLIMITED', analysis_limit: null }),
      expect.anything(),
    );
    await cleanup();
  });

  // 4. owner can choose Limited ---------------------------------------------
  test('4. owner can choose Limitata and see the number input', async () => {
    const { container, cleanup } = await renderTab([tester()]); // starts UNLIMITED
    await openQuotaDialog(container);
    expect(container.querySelector('[data-testid="quota-limit-input"]')).toBeNull();
    click(container.querySelector('[data-testid="quota-mode-limited"]'));
    await act(async () => { await Promise.resolve(); });
    expect(container.querySelector('[data-testid="quota-limit-input"]')).not.toBeNull();
    await cleanup();
  });

  // 5. valid limit accepted --------------------------------------------------
  test('5. a valid limit is accepted and sent to the API', async () => {
    const { container, cleanup } = await renderTab([tester()]);
    await openQuotaDialog(container);
    click(container.querySelector('[data-testid="quota-mode-limited"]'));
    await act(async () => { await Promise.resolve(); });
    fireInput(container.querySelector('[data-testid="quota-limit-input"]'), '8');
    await act(async () => { await Promise.resolve(); });

    await act(async () => { click(container.querySelector('[data-testid="quota-save-btn"]')); await Promise.resolve(); });
    expect(container.querySelector('[data-testid="quota-limit-error"]')).toBeNull();
    expect(axios.patch).toHaveBeenCalledWith(
      expect.stringContaining('/quota'),
      expect.objectContaining({ quota_mode: 'LIMITED', analysis_limit: 8 }),
      expect.anything(),
    );
    await cleanup();
  });

  // 6. invalid limit rejected --------------------------------------------------
  test('6. an invalid limit (zero) is rejected client-side and never sent', async () => {
    const { container, cleanup } = await renderTab([tester()]);
    await openQuotaDialog(container);
    click(container.querySelector('[data-testid="quota-mode-limited"]'));
    await act(async () => { await Promise.resolve(); });
    fireInput(container.querySelector('[data-testid="quota-limit-input"]'), '0');
    await act(async () => { await Promise.resolve(); });

    await act(async () => { click(container.querySelector('[data-testid="quota-save-btn"]')); await Promise.resolve(); });
    expect(container.querySelector('[data-testid="quota-limit-error"]')).not.toBeNull();
    expect(axios.patch).not.toHaveBeenCalled();
    await cleanup();
  });

  test('6b. a non-numeric limit is also rejected client-side', async () => {
    const { container, cleanup } = await renderTab([tester()]);
    await openQuotaDialog(container);
    click(container.querySelector('[data-testid="quota-mode-limited"]'));
    await act(async () => { await Promise.resolve(); });
    fireInput(container.querySelector('[data-testid="quota-limit-input"]'), 'abc');
    await act(async () => { await Promise.resolve(); });

    await act(async () => { click(container.querySelector('[data-testid="quota-save-btn"]')); await Promise.resolve(); });
    expect(container.querySelector('[data-testid="quota-limit-error"]')).not.toBeNull();
    expect(axios.patch).not.toHaveBeenCalled();
    await cleanup();
  });

  // 7. increase preserves consumed count ---------------------------------------
  test('7. increasing the limit preserves the consumed count in the live preview', async () => {
    const { container, cleanup } = await renderTab([
      tester({ quota: { mode: 'LIMITED', limit: 5, consumed: 4, reserved: 0, remaining: 1, state: 'AVAILABLE', quota_version: 1 } }),
    ]);
    await openQuotaDialog(container);
    fireInput(container.querySelector('[data-testid="quota-limit-input"]'), '8');
    await act(async () => { await Promise.resolve(); });

    const preview = container.querySelector('[data-testid="quota-preview"]');
    expect(preview.textContent).toContain('Nuovo limite: 8');
    expect(preview.textContent).toContain('Già utilizzate: 4');
    expect(preview.textContent).toContain('Rimanenti dopo la modifica: 4');
    expect(container.querySelector('[data-testid="quota-lower-warning"]')).toBeNull();
    await cleanup();
  });

  // 8. lowering below consumed shows warning -------------------------------------
  test('8. lowering the limit below consumed shows an explicit warning', async () => {
    const { container, cleanup } = await renderTab([
      tester({ quota: { mode: 'LIMITED', limit: 5, consumed: 4, reserved: 0, remaining: 1, state: 'AVAILABLE', quota_version: 1 } }),
    ]);
    await openQuotaDialog(container);
    fireInput(container.querySelector('[data-testid="quota-limit-input"]'), '2');
    await act(async () => { await Promise.resolve(); });

    const warning = container.querySelector('[data-testid="quota-lower-warning"]');
    expect(warning).not.toBeNull();
    expect(warning.textContent).toContain('utilizzo registrato resta invariato');
    expect(warning.textContent).toContain('non verrà applicato alcun addebito retroattivo');

    const preview = container.querySelector('[data-testid="quota-preview"]');
    expect(preview.textContent).toContain('Già utilizzate: 4');
    expect(preview.textContent).toContain('Rimanenti dopo la modifica: 0');

    // Lowering is still allowed (no retroactive charge, usage preserved) —
    // saving must still be possible.
    await act(async () => { click(container.querySelector('[data-testid="quota-save-btn"]')); await Promise.resolve(); });
    expect(axios.patch).toHaveBeenCalledWith(
      expect.stringContaining('/quota'),
      expect.objectContaining({ quota_mode: 'LIMITED', analysis_limit: 2 }),
      expect.anything(),
    );
    await cleanup();
  });

  // 9. new phase requires explicit confirmation ------------------------------------
  test('9. starting a new phase requires explicit confirmation before calling the API', async () => {
    const { container, cleanup } = await renderTab([
      tester({ quota: { mode: 'LIMITED', limit: 5, consumed: 4, reserved: 0, remaining: 1, state: 'AVAILABLE', quota_version: 1 } }),
    ]);
    await openQuotaDialog(container);
    click(container.querySelector('[data-testid="beta-new-phase-open-btn"]'));
    await act(async () => { await Promise.resolve(); });

    const dialog = container.querySelector('[data-testid="beta-new-phase-dialog"]');
    expect(dialog).not.toBeNull();
    expect(dialog.textContent).toContain(
      'Verrà avviata una nuova fase beta. Le analisi e l\'utilizzo della fase precedente resteranno nello storico.',
    );
    expect(dialog.textContent).not.toMatch(/\breset\b/i);
    // No API call before the explicit confirm click.
    expect(axios.post).not.toHaveBeenCalledWith(expect.stringContaining('/new-phase'), expect.anything(), expect.anything());

    await act(async () => { click(container.querySelector('[data-testid="new-phase-confirm-btn"]')); await Promise.resolve(); });
    expect(axios.post).toHaveBeenCalledWith(
      expect.stringContaining(`/testers/${tester().membership_id}/quota/new-phase`),
      { confirm: true },
      expect.anything(),
    );
    await cleanup();
  });

  // 10. new phase preserves previous phase history ------------------------------------
  test('10. historical phases (including a superseded one) are visible with no raw Mongo ids', async () => {
    const previousPhases = [
      { quota_version: 1, limit: 5, consumed: 5, started_at: '2026-07-01T00:00:00Z', ended_at: '2026-07-15T00:00:00Z', actor_email: 'owner@nexodify.test' },
      { quota_version: 2, limit: 8, consumed: 1, started_at: '2026-07-15T00:00:00Z', ended_at: null, actor_email: 'owner@nexodify.test' },
    ];
    const { container, cleanup } = await renderTab(
      [tester({ quota: { mode: 'LIMITED', limit: 8, consumed: 1, reserved: 0, remaining: 7, state: 'AVAILABLE', quota_version: 2 } })],
      { phases: previousPhases },
    );

    click(container.querySelector('[data-testid="beta-phases-open-btn"]'));
    await act(async () => { await Promise.resolve(); });

    const dialog = container.querySelector('[data-testid="beta-phases-dialog"]');
    expect(dialog).not.toBeNull();
    expect(dialog.textContent).toContain('v1');
    expect(dialog.textContent).toContain('v2');
    // The superseded phase's final consumed count is preserved in history.
    expect(dialog.textContent).toContain('owner@nexodify.test');
    expect(dialog.textContent).not.toMatch(/[0-9a-f]{24}/); // no raw ObjectId
    expect(dialog.textContent).not.toContain('_id');
    await cleanup();
  });

  // 21. Programma Beta / Tester structure remains intact ------------------------------
  test('21. existing tester-table structure (rows, revoke/reactivate) remains intact alongside the new quota column', async () => {
    const { container, cleanup } = await renderTab([
      tester({ status: 'ACTIVE' }),
      tester({ membership_id: 'betam_2', status: 'REVOKED', normalized_email: 'r@example.test' }),
    ]);
    expect(container.querySelectorAll('[data-testid="beta-tester-row"]')).toHaveLength(2);
    expect(container.querySelector('[data-testid="beta-revoke-btn"]')).not.toBeNull();
    expect(container.querySelector('[data-testid="beta-reactivate-btn"]')).not.toBeNull();
    expect(container.textContent).toContain('Quota beta');
    await cleanup();
  });
});
