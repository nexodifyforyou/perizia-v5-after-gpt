import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import axios from 'axios';
import Billing from './Billing';

jest.mock('axios');

jest.mock('react-router-dom', () => ({
  Link: ({ children, to, ...props }) => <a href={to} {...props}>{children}</a>,
  useSearchParams: () => [new URLSearchParams(''), jest.fn()],
}), { virtual: true });

jest.mock('./Dashboard', () => ({ Sidebar: () => <div data-testid="sidebar" /> }));
jest.mock('sonner', () => ({ toast: { error: jest.fn(), success: jest.fn(), info: jest.fn(), warning: jest.fn() } }));

let mockAccountState;
const mockRefreshUser = jest.fn();
jest.mock('../context/AuthContext', () => ({
  useAuth: () => ({
    user: { name: 'Test', email: 't@x.it' },
    logout: jest.fn(),
    refreshUser: mockRefreshUser,
    accountState: mockAccountState,
  }),
}));

const baseAccountState = (overrides = {}) => ({
  isMasterAdmin: false,
  planId: 'free',
  planLabel: 'Accesso iniziale',
  periziaCredits: { monthlyRemaining: 0, extraRemaining: 8, totalAvailable: 8 },
  subscription: {},
  betaProgram: { active: false },
  ...overrides,
});

// A non-trivial, distinctive real purchased balance used across the
// "byte-identical" checks below — chosen so it can never be confused with the
// old fake 9999 placeholder.
const REAL_BALANCE = 53;

const PLAN_FIXTURES = [
  {
    plan_id: 'starter',
    name_it: 'Pacchetto 8 crediti',
    plan_type_label_it: 'Pacchetto extra',
    price: 19,
    price_suffix_it: 'una tantum',
    features_it: ['8 crediti extra'],
  },
  {
    plan_id: 'solo',
    name_it: 'Solo',
    plan_type_label_it: 'Abbonamento mensile',
    price: 29,
    price_suffix_it: '/mese',
    features_it: ['28 crediti mensili'],
  },
];

const mockAxiosResponses = ({ plans = [] } = {}) => {
  axios.get.mockImplementation((url) => {
    const u = String(url);
    if (u.includes('/api/plans')) {
      return Promise.resolve({ data: { plans } });
    }
    if (u.includes('/api/billing/ledger')) {
      return Promise.resolve({ data: { entries: [], total: 0 } });
    }
    return Promise.resolve({ data: {} });
  });
};

const renderBilling = async (accountState, { plans = [] } = {}) => {
  mockAccountState = accountState;
  mockAxiosResponses({ plans });
  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);
  await act(async () => { root.render(<Billing />); });
  await act(async () => { await Promise.resolve(); });
  return { container, root, cleanup: () => act(() => root.unmount()) };
};

// Re-renders the SAME root with a new accountState, simulating a live
// entitlement refresh (no logout, no remount) — this is how the real app
// picks up activation/revocation after `refreshUser()`.
const rerenderBilling = async (root, accountState) => {
  mockAccountState = accountState;
  await act(async () => { root.render(<Billing />); });
  await act(async () => { await Promise.resolve(); });
};

describe('Billing — beta program presentation', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  test('ACTIVE beta: shows banner, hides purchase/recharge CTAs, labels balance "Crediti preservati"', async () => {
    mockAccountState = baseAccountState({
      planLabel: 'Programma Beta',
      betaProgram: { active: true, displayName: 'Geom. Beta' },
    });
    const { container, cleanup } = await renderBilling(mockAccountState);
    expect(container.querySelector('[data-testid="billing-beta-banner"]')).not.toBeNull();
    expect(container.textContent).toContain('Programma Beta attivo');
    // Purchase/recharge CTAs and the plans grid are hidden.
    expect(container.textContent).not.toContain('Piani Disponibili');
    expect(container.textContent).not.toContain('Ricarica crediti');
    // Real balance shown as "Crediti preservati", never a fake unlimited number.
    const total = container.querySelector('[data-testid="billing-total-credits"]');
    expect(total).not.toBeNull();
    expect(total.textContent).toContain('Crediti preservati');
    expect(total.textContent).toContain('8');
    expect(container.textContent).not.toContain('9999');
    cleanup();
  });

  test('INACTIVE (normal): no banner, purchase CTAs and plans are present', async () => {
    mockAccountState = baseAccountState();
    const { container, cleanup } = await renderBilling(mockAccountState);
    expect(container.querySelector('[data-testid="billing-beta-banner"]')).toBeNull();
    expect(container.textContent).toContain('Piani Disponibili');
    expect(container.textContent).toContain('Ricarica crediti');
    const total = container.querySelector('[data-testid="billing-total-credits"]');
    expect(total.textContent).toContain('Totale crediti disponibili');
    cleanup();
  });

  // 1. ACTIVE beta hides package/recharge CTAs -----------------------------
  test('1. ACTIVE beta hides package/recharge CTAs even when real plans (incl. a pack) are loaded', async () => {
    const accountState = baseAccountState({
      betaProgram: { active: true },
    });
    const { container, cleanup } = await renderBilling(accountState, { plans: PLAN_FIXTURES });
    expect(container.textContent).not.toContain('Acquista pacchetto');
    expect(container.textContent).not.toContain('Acquista ora');
    expect(container.textContent).not.toContain('Attiva piano');
    expect(container.textContent).not.toContain('Ricarica crediti');
    expect(container.querySelector('[data-testid="subscribe-starter-btn"]')).toBeNull();
    expect(container.querySelector('[data-testid="subscribe-solo-btn"]')).toBeNull();
    cleanup();
  });

  // 2. ACTIVE beta shows the real preserved balance, never 9999 ------------
  test('2. ACTIVE beta shows the real preserved balance and never a 9999 placeholder', async () => {
    const accountState = baseAccountState({
      betaProgram: { active: true },
      periziaCredits: { monthlyRemaining: 0, extraRemaining: REAL_BALANCE, totalAvailable: REAL_BALANCE },
    });
    const { container, cleanup } = await renderBilling(accountState);
    const total = container.querySelector('[data-testid="billing-total-credits"]');
    expect(total.textContent).toContain('Crediti preservati');
    expect(total.textContent).toContain(String(REAL_BALANCE));
    expect(container.textContent).not.toContain('9999');
    cleanup();
  });

  // 3. ACTIVE beta with NO subscription: no subscription-management CTA ---
  test('3. ACTIVE beta with no paid subscription shows no subscription-management controls', async () => {
    const accountState = baseAccountState({
      betaProgram: { active: true },
      subscription: {},
    });
    const { container, cleanup } = await renderBilling(accountState);
    expect(container.textContent).not.toContain('Gestisci abbonamento');
    expect(container.textContent).not.toContain('Cancella a fine periodo');
    expect(container.textContent).not.toContain('Mantieni attivo il rinnovo');
    expect(container.textContent).toContain('Nessun abbonamento ricorrente attivo');
    cleanup();
  });

  // 4. ACTIVE beta WITH a paid subscription retains subscription management
  test('4. ACTIVE beta with an active paid subscription retains "Gestisci abbonamento" controls', async () => {
    const accountState = baseAccountState({
      planId: 'solo',
      betaProgram: { active: true },
      subscription: {
        status: 'active',
        stripeSubscriptionId: 'sub_live_123',
        currentPlanId: 'solo',
        currentPeriodEnd: '2026-08-20T00:00:00Z',
        cancelAtPeriodEnd: false,
      },
    });
    const { container, cleanup } = await renderBilling(accountState);
    // Beta CTAs still hidden even though the user is paying.
    expect(container.textContent).not.toContain('Piani Disponibili');
    expect(container.textContent).not.toContain('Ricarica crediti');
    // But subscription management must be retained — never trap a paying user.
    expect(container.textContent).toContain('Gestisci abbonamento');
    expect(container.textContent).toContain('Cancella a fine periodo');
    expect(container.textContent).toContain('Abbonamento ricorrente attivo');
    cleanup();
  });

  // 5. REVOKED immediately restores normal purchase/recharge controls -----
  test('5. REVOKED (on next authenticated refresh, no remount) restores normal purchase/recharge controls', async () => {
    const activeState = baseAccountState({
      betaProgram: { active: true },
      periziaCredits: { monthlyRemaining: 0, extraRemaining: REAL_BALANCE, totalAvailable: REAL_BALANCE },
    });
    const { container, root, cleanup } = await renderBilling(activeState, { plans: PLAN_FIXTURES });
    expect(container.querySelector('[data-testid="billing-beta-banner"]')).not.toBeNull();
    expect(container.textContent).not.toContain('Ricarica crediti');

    const revokedState = baseAccountState({
      betaProgram: { active: false },
      periziaCredits: { monthlyRemaining: 0, extraRemaining: REAL_BALANCE, totalAvailable: REAL_BALANCE },
    });
    await rerenderBilling(root, revokedState);

    expect(container.querySelector('[data-testid="billing-beta-banner"]')).toBeNull();
    expect(container.textContent).toContain('Ricarica crediti');
    expect(container.textContent).toContain('Piani Disponibili');
    const total = container.querySelector('[data-testid="billing-total-credits"]');
    expect(total.textContent).toContain('Totale crediti disponibili');
    expect(total.textContent).toContain(String(REAL_BALANCE));
    cleanup();
  });

  // 6. Purchased balance byte-identical before / during / after -----------
  test('6. Purchased balance is byte-identical before activation, during beta, and after revocation', async () => {
    const before = baseAccountState({
      betaProgram: { active: false },
      periziaCredits: { monthlyRemaining: 0, extraRemaining: REAL_BALANCE, totalAvailable: REAL_BALANCE },
    });
    const { container: c1, cleanup: cleanup1 } = await renderBilling(before);
    const beforeText = c1.querySelector('[data-testid="billing-total-credits"]').textContent;
    cleanup1();

    const during = baseAccountState({
      betaProgram: { active: true },
      periziaCredits: { monthlyRemaining: 0, extraRemaining: REAL_BALANCE, totalAvailable: REAL_BALANCE },
    });
    const { container: c2, cleanup: cleanup2 } = await renderBilling(during);
    const duringNumber = c2.querySelector('[data-testid="billing-total-credits"]').textContent;
    cleanup2();

    const after = baseAccountState({
      betaProgram: { active: false },
      periziaCredits: { monthlyRemaining: 0, extraRemaining: REAL_BALANCE, totalAvailable: REAL_BALANCE },
    });
    const { container: c3, cleanup: cleanup3 } = await renderBilling(after);
    const afterText = c3.querySelector('[data-testid="billing-total-credits"]').textContent;
    cleanup3();

    // Same underlying number every time (label differs, digits don't).
    expect(beforeText).toContain(String(REAL_BALANCE));
    expect(duringNumber).toContain(String(REAL_BALANCE));
    expect(afterText).toContain(String(REAL_BALANCE));
    expect(beforeText).not.toContain('9999');
    expect(duringNumber).not.toContain('9999');
    expect(afterText).not.toContain('9999');
    // before/after use the identical "Totale crediti disponibili" presentation.
    expect(beforeText).toEqual(afterText);
  });

  // 7. Activation/revocation makes ZERO Stripe calls -----------------------
  test('7. Activation and revocation lifecycle makes zero Stripe/checkout network calls', async () => {
    const normal = baseAccountState({ betaProgram: { active: false } });
    const { root, cleanup } = await renderBilling(normal, { plans: PLAN_FIXTURES });

    await rerenderBilling(root, baseAccountState({ betaProgram: { active: true } }));
    await rerenderBilling(root, baseAccountState({ betaProgram: { active: false } }));
    await rerenderBilling(root, baseAccountState({ betaProgram: { active: true } }));

    // No POST calls at all (checkout create / subscription actions are only
    // ever triggered by explicit button clicks, none of which happened here).
    expect(axios.post).not.toHaveBeenCalled();
    // Every GET call made during the whole lifecycle stays on plans/ledger —
    // never touches a checkout/stripe endpoint.
    const getUrls = axios.get.mock.calls.map((call) => String(call[0]));
    expect(getUrls.length).toBeGreaterThan(0);
    getUrls.forEach((url) => {
      expect(url).not.toMatch(/checkout|stripe/i);
    });
    cleanup();
  });

  // 8. Evidence check (not a unit test) — see report: `git diff` inspection
  // of the branch shows no Stripe product/price/checkout/webhook code changed.

  // Full isolated lifecycle: normal -> ACTIVE -> REVOKED -> ACTIVE again ---
  test('lifecycle: normal -> ACTIVE -> REVOKED -> ACTIVE again records controls and balance at every state', async () => {
    const observe = (container) => ({
      hasBanner: Boolean(container.querySelector('[data-testid="billing-beta-banner"]')),
      hasPurchaseCtas: container.textContent.includes('Piani Disponibili') || container.textContent.includes('Ricarica crediti'),
      balanceLabel: container.querySelector('[data-testid="billing-total-credits"]').textContent.includes('Crediti preservati')
        ? 'Crediti preservati'
        : 'Totale crediti disponibili',
      balanceText: container.querySelector('[data-testid="billing-total-credits"]').textContent,
    });

    const withBeta = (active) => baseAccountState({
      betaProgram: { active },
      periziaCredits: { monthlyRemaining: 0, extraRemaining: REAL_BALANCE, totalAvailable: REAL_BALANCE },
    });

    const lifecycle = [];

    const { container, root, cleanup } = await renderBilling(withBeta(false), { plans: PLAN_FIXTURES });
    lifecycle.push({ state: 'normal', ...observe(container) });

    await rerenderBilling(root, withBeta(true));
    lifecycle.push({ state: 'ACTIVE', ...observe(container) });

    await rerenderBilling(root, withBeta(false));
    lifecycle.push({ state: 'REVOKED', ...observe(container) });

    await rerenderBilling(root, withBeta(true));
    lifecycle.push({ state: 'ACTIVE again', ...observe(container) });

    expect(lifecycle).toEqual([
      { state: 'normal', hasBanner: false, hasPurchaseCtas: true, balanceLabel: 'Totale crediti disponibili', balanceText: expect.stringContaining(String(REAL_BALANCE)) },
      { state: 'ACTIVE', hasBanner: true, hasPurchaseCtas: false, balanceLabel: 'Crediti preservati', balanceText: expect.stringContaining(String(REAL_BALANCE)) },
      { state: 'REVOKED', hasBanner: false, hasPurchaseCtas: true, balanceLabel: 'Totale crediti disponibili', balanceText: expect.stringContaining(String(REAL_BALANCE)) },
      { state: 'ACTIVE again', hasBanner: true, hasPurchaseCtas: false, balanceLabel: 'Crediti preservati', balanceText: expect.stringContaining(String(REAL_BALANCE)) },
    ]);

    // The balance number itself never changes across the lifecycle, and is
    // never the old fake 9999 placeholder.
    lifecycle.forEach((entry) => {
      expect(entry.balanceText).not.toContain('9999');
    });

    cleanup();
  });
});
