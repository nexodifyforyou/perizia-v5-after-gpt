import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import axios from 'axios';
import Billing from './Billing';

// Configurable beta perizia allowance — tester-facing display in Billing
// (docs/beta_perizia_limits_plan.md §O). Covers required tests 11-20 (tester
// side) plus test 22 (mobile layout stays usable).

jest.mock('axios');

jest.mock('react-router-dom', () => ({
  Link: ({ children, to, ...props }) => <a href={to} {...props}>{children}</a>,
  useSearchParams: () => [new URLSearchParams(''), jest.fn()],
}), { virtual: true });

jest.mock('./Dashboard', () => ({ Sidebar: () => <div data-testid="sidebar" /> }));
jest.mock('sonner', () => ({ toast: { error: jest.fn(), success: jest.fn(), info: jest.fn(), warning: jest.fn() } }));

let mockAccountState;
const mockRefreshUser = jest.fn();
const mockLogout = jest.fn();
jest.mock('../context/AuthContext', () => ({
  useAuth: () => ({
    user: { name: 'Test', email: 't@x.it' },
    logout: mockLogout,
    refreshUser: mockRefreshUser,
    accountState: mockAccountState,
  }),
}));

const REAL_BALANCE = 53;

const baseAccountState = (overrides = {}) => ({
  isMasterAdmin: false,
  planId: 'free',
  planLabel: 'Programma Beta',
  periziaCredits: { monthlyRemaining: 0, extraRemaining: REAL_BALANCE, totalAvailable: REAL_BALANCE },
  subscription: {},
  betaProgram: { active: true, quota: { mode: 'UNLIMITED', state: 'UNLIMITED', limit: null, consumed: 0, reserved: 0, remaining: null } },
  ...overrides,
});

const limitedQuota = (over = {}) => ({
  mode: 'LIMITED',
  state: 'AVAILABLE',
  limit: 5,
  consumed: 2,
  reserved: 0,
  remaining: 3,
  ...over,
});

const mockAxiosResponses = () => {
  axios.get.mockImplementation((url) => {
    const u = String(url);
    if (u.includes('/api/plans')) return Promise.resolve({ data: { plans: [] } });
    if (u.includes('/api/billing/ledger')) return Promise.resolve({ data: { entries: [], total: 0 } });
    return Promise.resolve({ data: {} });
  });
};

const renderBilling = async (accountState) => {
  mockAccountState = accountState;
  mockAxiosResponses();
  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);
  await act(async () => { root.render(<Billing />); });
  await act(async () => { await Promise.resolve(); });
  return { container, root, cleanup: () => act(() => root.unmount()) };
};

const rerenderBilling = async (root, accountState) => {
  mockAccountState = accountState;
  await act(async () => { root.render(<Billing />); });
  await act(async () => { await Promise.resolve(); });
};

describe('Billing — beta quota (tester side)', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  // 11. tester sees X available out of Y -----------------------------------
  test('11. tester sees "X perizie beta disponibili su Y" when LIMITED and available', async () => {
    const { container, cleanup } = await renderBilling(baseAccountState({
      betaProgram: { active: true, quota: limitedQuota() },
    }));
    expect(container.querySelector('[data-testid="billing-beta-banner"]').textContent).toContain('3 perizie beta disponibili su 5');
    expect(container.textContent).toContain('Accesso Beta');
    await cleanup();
  });

  // 12. tester sees zero when exhausted -------------------------------------
  test('12. tester sees "0 perizie beta disponibili su Y" when EXHAUSTED', async () => {
    const { container, cleanup } = await renderBilling(baseAccountState({
      betaProgram: { active: true, quota: limitedQuota({ state: 'EXHAUSTED', consumed: 5, remaining: 0 }) },
    }));
    expect(container.querySelector('[data-testid="billing-beta-banner"]').textContent).toContain('0 perizie beta disponibili su 5');
    await cleanup();
  });

  // 13. tester does not see fake 9,999 --------------------------------------
  test('13. tester never sees a fake 9999/placeholder value, in any quota state', async () => {
    for (const quota of [
      { mode: 'UNLIMITED', state: 'UNLIMITED', limit: null, consumed: 0, reserved: 0, remaining: null },
      limitedQuota(),
      limitedQuota({ state: 'EXHAUSTED', consumed: 5, remaining: 0 }),
    ]) {
      const { container, cleanup } = await renderBilling(baseAccountState({ betaProgram: { active: true, quota } }));
      expect(container.textContent).not.toContain('9999');
      cleanup();
    }
  });

  // 14. tester cannot access limit controls ---------------------------------
  test('14. tester never sees owner-only quota-management controls', async () => {
    const { container, cleanup } = await renderBilling(baseAccountState({
      betaProgram: { active: true, quota: limitedQuota() },
    }));
    expect(container.textContent).not.toContain('Gestisci limite');
    expect(container.textContent).not.toContain('Avvia nuova fase beta');
    expect(container.textContent).not.toContain('Numero massimo di perizie');
    expect(container.querySelector('[data-testid="quota-mode-limited"]')).toBeNull();
    expect(container.querySelector('[data-testid="quota-save-btn"]')).toBeNull();
    await cleanup();
  });

  // 15. exhausted beta state is clear ---------------------------------------
  test('15. EXHAUSTED beta state is unambiguous: badge, zero-of-limit, and reassurance copy', async () => {
    const { container, cleanup } = await renderBilling(baseAccountState({
      betaProgram: { active: true, quota: limitedQuota({ state: 'EXHAUSTED', consumed: 5, remaining: 0 }) },
    }));
    const banner = container.querySelector('[data-testid="billing-beta-banner"]');
    expect(banner.textContent).toContain('Limite beta raggiunto');
    expect(banner.textContent).toContain('0 perizie beta disponibili su 5');
    expect(banner.textContent).toContain('Gli eventuali crediti acquistati restano disponibili.');
    await cleanup();
  });

  // 16. BETA_LIMIT_REACHED message renders -----------------------------------
  test('16. the exact BETA_LIMIT_REACHED customer-safe message text is present in the codebase copy', () => {
    // NewAnalysis.js renders this exact string when the backend returns the
    // BETA_LIMIT_REACHED reason code on an upload attempt (verified by source
    // inspection here — the upload flow itself lives outside Billing.js).
    // eslint-disable-next-line global-require
    const fs = require('fs');
    const path = require('path');
    const source = fs.readFileSync(path.join(__dirname, 'NewAnalysis.js'), 'utf8');
    expect(source).toContain('BETA_LIMIT_REACHED');
    expect(source).toContain(
      "Hai completato le analisi previste per questa fase beta. I report già generati restano disponibili. Contatta l'amministratore del programma per estendere il test oppure utilizza il tuo piano disponibile.",
    );
  });

  // 17. real credits remain visible separately -------------------------------
  test('17. the real paid balance ("Crediti preservati") stays visible separately from the beta quota text', async () => {
    const { container, cleanup } = await renderBilling(baseAccountState({
      betaProgram: { active: true, quota: limitedQuota() },
      periziaCredits: { monthlyRemaining: 0, extraRemaining: REAL_BALANCE, totalAvailable: REAL_BALANCE },
    }));
    const total = container.querySelector('[data-testid="billing-total-credits"]');
    expect(total.textContent).toContain('Crediti preservati');
    expect(total.textContent).toContain(String(REAL_BALANCE));
    // The beta quota banner and the real-balance box are two distinct elements.
    expect(container.querySelector('[data-testid="billing-beta-banner"]')).not.toBe(total);
    await cleanup();
  });

  // 18. purchased balance not shown as consumed ------------------------------
  test('18. the purchased balance is independent of the beta quota consumed count', async () => {
    const { container, cleanup } = await renderBilling(baseAccountState({
      betaProgram: { active: true, quota: limitedQuota({ consumed: 4, remaining: 1 }) },
      periziaCredits: { monthlyRemaining: 0, extraRemaining: REAL_BALANCE, totalAvailable: REAL_BALANCE },
    }));
    const total = container.querySelector('[data-testid="billing-total-credits"]');
    // The real balance shows the untouched purchased number, never the beta
    // "consumed" count (4) and never a number derived from it.
    expect(total.textContent).toContain(String(REAL_BALANCE));
    expect(total.textContent).not.toContain('4');
    await cleanup();
  });

  // 19. increased limit updates after refresh --------------------------------
  test('19. after the owner increases the limit, the tester sees the new number on refresh (no remount)', async () => {
    const { container, root, cleanup } = await renderBilling(baseAccountState({
      betaProgram: { active: true, quota: limitedQuota({ limit: 5, consumed: 2, remaining: 3 }) },
    }));
    expect(container.querySelector('[data-testid="billing-beta-banner"]').textContent).toContain('3 perizie beta disponibili su 5');

    await rerenderBilling(root, baseAccountState({
      betaProgram: { active: true, quota: limitedQuota({ limit: 8, consumed: 2, remaining: 6 }) },
    }));
    expect(container.querySelector('[data-testid="billing-beta-banner"]').textContent).toContain('6 perizie beta disponibili su 8');
    await cleanup();
  });

  // 20. no logout required ----------------------------------------------------
  test('20. the quota refresh above requires no logout call', async () => {
    const { root, cleanup } = await renderBilling(baseAccountState({
      betaProgram: { active: true, quota: limitedQuota({ limit: 5, consumed: 2, remaining: 3 }) },
    }));
    await rerenderBilling(root, baseAccountState({
      betaProgram: { active: true, quota: limitedQuota({ limit: 8, consumed: 2, remaining: 6 }) },
    }));
    expect(mockLogout).not.toHaveBeenCalled();
    await cleanup();
  });

  // In-progress reservation line ------------------------------------------
  test('reserved > 0 shows the "analisi in elaborazione" line, pluralised', async () => {
    const { container, cleanup } = await renderBilling(baseAccountState({
      betaProgram: { active: true, quota: limitedQuota({ reserved: 1 }) },
    }));
    expect(container.querySelector('[data-testid="billing-beta-in-progress"]').textContent).toContain('1 analisi in elaborazione');
    cleanup();

    const { container: c2, cleanup: cleanup2 } = await renderBilling(baseAccountState({
      betaProgram: { active: true, quota: limitedQuota({ reserved: 3 }) },
    }));
    expect(c2.querySelector('[data-testid="billing-beta-in-progress"]').textContent).toContain('3 analisi in elaborazione');
    cleanup2();
  });

  // EXHAUSTED restores normal plan controls -----------------------------------
  test('EXHAUSTED restores normal plan controls (plans grid, recharge CTA) even while still an active beta member', async () => {
    const { container, cleanup } = await renderBilling(baseAccountState({
      betaProgram: { active: true, quota: limitedQuota({ state: 'EXHAUSTED', consumed: 5, remaining: 0 }) },
    }));
    expect(container.textContent).toContain('Piani Disponibili');
    expect(container.textContent).toContain('Ricarica crediti');
    await cleanup();
  });

  // 22. mobile layout remains usable --------------------------------------------
  test('22. the beta banner and credit boxes use responsive (mobile-first) layout classes', async () => {
    const { container, cleanup } = await renderBilling(baseAccountState({
      betaProgram: { active: true, quota: limitedQuota() },
    }));
    const banner = container.querySelector('[data-testid="billing-beta-banner"]');
    expect(banner.querySelector('.flex.flex-wrap')).not.toBeNull();
    // The stats grid stacks to a single column on narrow viewports
    // (grid-cols-1) and only widens at md/xl breakpoints — no fixed desktop
    // width is imposed on the page body.
    const statsGrid = container.querySelector('.grid.grid-cols-1.md\\:grid-cols-2.xl\\:grid-cols-4');
    expect(statsGrid).not.toBeNull();
    await cleanup();
  });
});
