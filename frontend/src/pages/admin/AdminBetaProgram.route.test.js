import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import axios from 'axios';
import App from '../../App';

// This suite drives the ACTUAL route table declared in App.js (real Route
// paths, real guard components — ProtectedRoute/AdminRoute/OwnerRoute — real
// AdminBetaProgram) for the old /admin/beta-feedback path, across all five
// actor types. Everything that isn't the routing shell under test is stubbed
// so failures point at the guard/redirect logic, not at unrelated page
// internals (those have their own suites).
//
// 'react-router-dom' itself cannot be resolved by this project's Jest config
// (its package "exports" map has no CJS "require" condition CRA's Jest
// understands — `import { BrowserRouter } from 'react-router-dom'` fails
// with "Cannot find module" even in isolation, before any change made here).
// Every existing test that touches a react-router-dom-consuming page already
// works around this by mocking the package outright. This suite does the
// same, but the mock is a real (if minimal) router — it matches Route
// "path" props against the live URL and lets Navigate really change it — so
// App.js's own Routes/Route/Navigate/useLocation usage is genuinely
// exercised rather than replaced by test-only routing logic.
jest.mock('react-router-dom', () => {
  const ReactActual = require('react');
  const LocationContext = ReactActual.createContext(null);

  const readLocation = () => ({
    pathname: global.window.location.pathname,
    search: global.window.location.search,
    hash: global.window.location.hash,
    state: null,
  });

  // Location lives in real React state on BrowserRouter and is threaded
  // through context, so a Navigate's setState propagates to every
  // useLocation() consumer within the SAME commit/act flush — no manual
  // pub-sub, so no ordering race between who subscribes first.
  const BrowserRouter = ({ children }) => {
    const [loc, setLoc] = ReactActual.useState(readLocation);
    const navigate = ReactActual.useCallback((to) => {
      global.window.history.pushState(null, '', to);
      setLoc(readLocation());
    }, []);
    const value = ReactActual.useMemo(() => ({ loc, navigate }), [loc, navigate]);
    return ReactActual.createElement(LocationContext.Provider, { value }, children);
  };

  const useLocation = () => ReactActual.useContext(LocationContext).loc;
  const useInternalNavigate = () => ReactActual.useContext(LocationContext).navigate;

  const Routes = ({ children }) => {
    const loc = useLocation();
    const items = ReactActual.Children.toArray(children);
    const exact = items.find((child) => child.props.path === loc.pathname);
    const wildcard = items.find((child) => child.props.path === '*');
    const match = exact || wildcard;
    return match ? match.props.element : null;
  };

  const Route = () => null;

  const Navigate = ({ to }) => {
    const navigate = useInternalNavigate();
    ReactActual.useEffect(() => { navigate(to); }, [to, navigate]);
    return null;
  };

  return { BrowserRouter, Routes, Route, Navigate, useLocation };
}, { virtual: true });

jest.mock('axios');
jest.mock('sonner', () => ({ toast: { error: jest.fn(), success: jest.fn(), info: jest.fn() } }));
jest.mock('../../components/ui/sonner', () => ({ Toaster: () => null }));

let mockAuth;
jest.mock('../../context/AuthContext', () => ({
  AuthProvider: ({ children }) => children,
  useAuth: () => mockAuth,
}));

// Every page other than AdminBetaProgram is a trivial stand-in: this suite
// only asserts on where the router lands and whether beta data was fetched,
// never on those pages' own internals.
jest.mock('../Landing', () => () => <div data-testid="page-landing" />);
jest.mock('../Pacchetti', () => () => <div data-testid="page-pacchetti" />);
jest.mock('../Supporto', () => () => <div data-testid="page-supporto" />);
jest.mock('../Termini', () => () => <div data-testid="page-termini" />);
jest.mock('../Privacy', () => () => <div data-testid="page-privacy" />);
jest.mock('../Dashboard', () => ({
  __esModule: true,
  default: () => <div data-testid="page-dashboard" />,
  Sidebar: () => <div data-testid="sidebar" />,
}));
jest.mock('../NewAnalysis', () => () => <div data-testid="page-new-analysis" />);
jest.mock('../AnalysisResult', () => () => <div data-testid="page-analysis-result" />);
jest.mock('../AnalysisPrintView', () => () => <div data-testid="page-analysis-print" />);
jest.mock('../ImageForensics', () => () => <div data-testid="page-forensics" />);
jest.mock('../Assistant', () => () => <div data-testid="page-assistant" />);
jest.mock('../History', () => () => <div data-testid="page-history" />);
jest.mock('../Billing', () => () => <div data-testid="page-billing" />);
jest.mock('../Profile', () => () => <div data-testid="page-profile" />);
jest.mock('../AuthCallback', () => () => <div data-testid="page-auth-callback" />);
jest.mock('./AdminOverview', () => () => <div data-testid="page-admin-overview" />);
jest.mock('./AdminUsers', () => () => <div data-testid="page-admin-users" />);
jest.mock('./AdminUserDetail', () => () => <div data-testid="page-admin-user-detail" />);
jest.mock('./AdminPerizie', () => () => <div data-testid="page-admin-perizie" />);
jest.mock('./AdminImages', () => () => <div data-testid="page-admin-images" />);
jest.mock('./AdminAssistant', () => () => <div data-testid="page-admin-assistant" />);
jest.mock('./AdminTransactions', () => () => <div data-testid="page-admin-transactions" />);
jest.mock('../BetaDashboard', () => () => <div data-testid="page-beta-dashboard" />);
// AdminBetaProgram itself is deliberately NOT mocked — it's the destination
// under test, and must be proven to actually land on the Feedback tab.

const overview = {
  testers: { active: 2, pending: 1, revoked: 0, registered: 2 },
  analyses: { beta_total: 3, unreadable_total: 0 },
  reports: { ready_total: 2, verification_required_total: 0, confirmation_required_total: 0,
             confirmation_completed_total: 0, reused_total: 0, forced_rerun_total: 0,
             service_busy_total: 0, service_unavailable_total: 0, avg_duration_seconds: 12 },
  feedback: { total: 1, new: 1, accepted: 0, high_priority: 0 },
  signals: {},
};

const betaEndpointStub = (url) => {
  if (url.includes('/overview')) return Promise.resolve({ data: overview });
  if (url.includes('/testers')) return Promise.resolve({ data: { items: [], total: 0, page: 1, page_size: 25 } });
  if (url.includes('/feedback')) return Promise.resolve({ data: { items: [], total: 0, metrics: {} } });
  if (url.includes('/signals')) return Promise.resolve({ data: { items: [], total: 0 } });
  return Promise.resolve({ data: {} });
};

const baseFeatureAccess = { canUseAssistant: false, canUseImageForensics: false };

const OWNER = { email: 'owner@nexodify.it', is_master_admin: true, correctness_v2_admin_view: true };
const BETA_TESTER = { email: 'tester@example.it', is_beta_partner: true, is_master_admin: false, correctness_v2_admin_view: false };
const NORMAL_CUSTOMER = { email: 'customer@example.it', is_master_admin: false, correctness_v2_admin_view: false };
const NON_OWNER_ADMIN = { email: 'admin@nexodify.it', is_master_admin: true, correctness_v2_admin_view: false };

const renderAtOldRoute = async () => {
  window.history.pushState(null, '', '/admin/beta-feedback');
  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);
  await act(async () => { root.render(<App />); });
  // Each guard hop (old route -> canonical route, or old route -> denial
  // target) is itself a Navigate whose effect fires only after the previous
  // commit — flush a few microtask ticks so every hop settles.
  for (let i = 0; i < 5; i += 1) {
    // eslint-disable-next-line no-await-in-loop
    await act(async () => { await Promise.resolve(); });
  }
  return { container, cleanup: () => act(() => root.unmount()) };
};

describe('Old route /admin/beta-feedback — authorization + redirect (all actors)', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    axios.get.mockImplementation(betaEndpointStub);
  });

  test('exact owner: redirected to /admin/beta-program?tab=feedback and lands on the Feedback tab', async () => {
    mockAuth = { user: OWNER, loading: false, featureAccess: baseFeatureAccess };
    const { container, cleanup } = await renderAtOldRoute();

    expect(window.location.pathname).toBe('/admin/beta-program');
    expect(window.location.search).toBe('?tab=feedback');

    const feedbackTrigger = container.querySelector('[data-testid="beta-tab-feedback"]');
    const overviewTrigger = container.querySelector('[data-testid="beta-tab-panoramica"]');
    expect(feedbackTrigger).not.toBeNull();
    expect(feedbackTrigger.getAttribute('data-state')).toBe('active');
    expect(overviewTrigger.getAttribute('data-state')).toBe('inactive');
    expect(container.querySelector('[data-testid="beta-feedback"]')).not.toBeNull();

    // The owner IS authorized, so the Feedback tab's own data load happens.
    const calledUrls = axios.get.mock.calls.map((c) => c[0]);
    expect(calledUrls.some((u) => u.includes('/admin/beta-program/feedback'))).toBe(true);

    cleanup();
  });

  test('beta tester: denied (redirected to /dashboard), no beta API call issued', async () => {
    mockAuth = { user: BETA_TESTER, loading: false, featureAccess: baseFeatureAccess };
    const { container, cleanup } = await renderAtOldRoute();

    expect(window.location.pathname).toBe('/dashboard');
    expect(container.querySelector('[data-testid="page-dashboard"]')).not.toBeNull();
    expect(container.querySelector('[data-testid="beta-feedback"]')).toBeNull();
    expect(axios.get).not.toHaveBeenCalled();

    cleanup();
  });

  test('normal customer: denied (redirected to /dashboard), no beta API call issued', async () => {
    mockAuth = { user: NORMAL_CUSTOMER, loading: false, featureAccess: baseFeatureAccess };
    const { container, cleanup } = await renderAtOldRoute();

    expect(window.location.pathname).toBe('/dashboard');
    expect(container.querySelector('[data-testid="page-dashboard"]')).not.toBeNull();
    expect(container.querySelector('[data-testid="beta-feedback"]')).toBeNull();
    expect(axios.get).not.toHaveBeenCalled();

    cleanup();
  });

  test('non-owner admin: denied (redirected to /dashboard), no beta API call issued', async () => {
    mockAuth = { user: NON_OWNER_ADMIN, loading: false, featureAccess: baseFeatureAccess };
    const { container, cleanup } = await renderAtOldRoute();

    expect(window.location.pathname).toBe('/dashboard');
    expect(container.querySelector('[data-testid="page-dashboard"]')).not.toBeNull();
    expect(container.querySelector('[data-testid="beta-feedback"]')).toBeNull();
    expect(axios.get).not.toHaveBeenCalled();

    cleanup();
  });

  test('unauthenticated user: established login/auth redirect (to "/"), no beta API call issued', async () => {
    mockAuth = { user: null, loading: false, featureAccess: baseFeatureAccess };
    const { container, cleanup } = await renderAtOldRoute();

    expect(window.location.pathname).toBe('/');
    expect(container.querySelector('[data-testid="page-landing"]')).not.toBeNull();
    expect(container.querySelector('[data-testid="beta-feedback"]')).toBeNull();
    expect(axios.get).not.toHaveBeenCalled();

    cleanup();
  });
});
