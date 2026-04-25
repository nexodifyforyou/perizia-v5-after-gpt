import fs from 'fs';
import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import AnalysisResult from './AnalysisResult';

jest.mock('react-router-dom', () => ({
  Link: ({ children, to, ...props }) => <a href={to} {...props}>{children}</a>,
  useParams: () => ({ analysisId: 'fresh-analysis' }),
  useLocation: () => ({ search: '?debug=1' }),
  useNavigate: () => jest.fn(),
}), { virtual: true });

jest.mock('../context/AuthContext', () => ({
  useAuth: () => ({
    user: { name: 'Fresh Pipeline Test User' },
    logout: jest.fn(),
  }),
}));

jest.mock('./Dashboard', () => ({
  Sidebar: () => <div data-testid="sidebar" />,
  SemaforoBadge: ({ status }) => <div data-testid="semaforo-badge">{status}</div>,
}));

jest.mock('../components/HeadlineVerifyModal', () => () => null);

jest.mock('../components/ui/button', () => ({
  Button: ({ children, ...props }) => <button {...props}>{children}</button>,
}));

jest.mock('../components/ui/tabs', () => {
  const React = require('react');
  const TabsContext = React.createContext({ value: '', onValueChange: () => {} });
  return {
    Tabs: ({ value, onValueChange, children }) => (
      <TabsContext.Provider value={{ value, onValueChange }}>
        <div>{children}</div>
      </TabsContext.Provider>
    ),
    TabsList: ({ children, ...props }) => <div {...props}>{children}</div>,
    TabsTrigger: ({ value, children, ...props }) => {
      const ctx = React.useContext(TabsContext);
      return (
        <button type="button" onClick={() => ctx.onValueChange(value)} {...props}>
          {children}
        </button>
      );
    },
    TabsContent: ({ value, children, ...props }) => {
      const ctx = React.useContext(TabsContext);
      if (ctx.value !== value) return null;
      return <div {...props}>{children}</div>;
    },
  };
});

jest.mock('sonner', () => ({
  toast: {
    error: jest.fn(),
    success: jest.fn(),
    info: jest.fn(),
  },
}));

const BANNED_PHRASES = [
  'TBD',
  'NOT_SPECIFIED',
  'NON SPECIFICATO IN PERIZIA',
  'step3_candidates',
  'Deterministic candidate-based cost',
  'no_packet',
  'unresolved_explained',
  'explanation_fallback_reason',
  'raw',
  'debug',
  'candidate',
  'INTERNAL DIRTY',
];

const FIXTURES = [
  {
    name: 'fresh multilot',
    path: '/tmp/fresh_multilot_final.json',
    summary: 'Agibilità assente / non rilasciata.',
    canonicalIssue: 'Opponibilità occupazione: NON VERIFICABILE.',
    ambiguity: 'Le frasi raccolte sulla opponibilità occupazione non coincidono tra loro e non permettono una chiusura sicura.',
    verifyNext: 'Verificare titolo di occupazione, data del contratto, registrazione e opponibilità verso la procedura.',
    scopeLabels: ['Perizia Multi-Lotto (3 lotti)', 'Lotto 1', 'Lotto 2', 'Lotto 3', 'Bene 1'],
    pagePatterns: [/p\. 46|Pag\. 46/, /p\. 70|Pag\. 70/],
  },
  {
    name: 'fresh multibene',
    path: '/tmp/fresh_multibene_final.json',
    summary: 'Immobile occupato.',
    canonicalIssue: 'Opponibilità occupazione: NON VERIFICABILE.',
    ambiguity: 'Ambiguità: La perizia dice che il Bene 1 è "occupato da ... debitore".',
    verifyNext: "Per verificare l'opponibilità serve controllare nelle pagine della sezione del Bene 1 se è indicato un titolo di occupazione o un contratto registrato",
    scopeLabels: ['Composizione Lotto / Lot Composition', 'Bene 1', 'Bene 2', 'Bene 3', 'Bene 4'],
    pagePatterns: [/p\. 21|Pag\. 21/, /p\. 13|Pag\. 13/],
  },
];

const loadFreshPayload = (path) => JSON.parse(fs.readFileSync(path, 'utf8'));

let container;
let root;

const renderAnalysisResult = async (payload) => {
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
  window.__DEBUG_ANALYSIS_PAYLOAD__ = payload;
  await act(async () => {
    root.render(<AnalysisResult />);
    await Promise.resolve();
  });
};

const clickTab = async (testId) => {
  const node = container.querySelector(`[data-testid="${testId}"]`);
  if (!node) throw new Error(`Missing tab ${testId}`);
  await act(async () => {
    node.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    await Promise.resolve();
  });
};

const fullText = () => container.textContent || '';

const expectBannedPhrasesAbsent = () => {
  const text = fullText().toLowerCase();
  for (const phrase of BANNED_PHRASES) {
    expect(text).not.toContain(phrase.toLowerCase());
  }
};

describe('AnalysisResult fresh canonical pipeline render acceptance', () => {
  beforeEach(() => {
    globalThis.IS_REACT_ACT_ENVIRONMENT = true;
    document.body.innerHTML = '';
  });

  afterEach(() => {
    if (root) {
      act(() => {
        root.unmount();
      });
    }
    if (container?.parentNode) {
      container.parentNode.removeChild(container);
    }
    container = null;
    root = null;
    delete window.__DEBUG_ANALYSIS_PAYLOAD__;
  });

  test.each(FIXTURES)('$name renders fresh canonical truth without internal leakage', async ({
    path,
    summary,
    canonicalIssue,
    ambiguity,
    verifyNext,
    scopeLabels,
    pagePatterns,
  }) => {
    const payload = loadFreshPayload(path);

    expect(payload.analysis_id).toMatch(/^analysis_/);
    expect(payload.status).toBe('COMPLETED');
    expect(Array.isArray(payload.result.issues)).toBe(true);
    expect(payload.result.issues.length).toBeGreaterThan(0);

    await renderAnalysisResult(payload);

    expect(fullText()).toContain(summary);
    for (const scopeLabel of scopeLabels) {
      expect(fullText()).toContain(scopeLabel);
    }
    expectBannedPhrasesAbsent();

    await clickTab('tab-legal');
    expect(fullText()).toContain(canonicalIssue);
    expect(fullText()).toContain(verifyNext);
    expectBannedPhrasesAbsent();

    await clickTab('tab-flags');
    expect(fullText()).toContain(canonicalIssue);
    expect(fullText()).toContain(ambiguity);
    expect(fullText()).toContain(verifyNext);
    for (const pagePattern of pagePatterns) {
      expect(fullText()).toMatch(pagePattern);
    }
    expectBannedPhrasesAbsent();
  });
});
