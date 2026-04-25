import fs from 'fs';
import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import AnalysisResult from './AnalysisResult';

jest.mock('react-router-dom', () => ({
  Link: ({ children, to, ...props }) => <a href={to} {...props}>{children}</a>,
  useParams: () => ({ analysisId: 'live-analysis' }),
  useLocation: () => ({ search: '?debug=1' }),
  useNavigate: () => jest.fn(),
}), { virtual: true });

jest.mock('../context/AuthContext', () => ({
  useAuth: () => ({
    user: { name: 'Test User' },
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

jest.mock('../components/EvidenceDisplay', () => ({
  EvidenceBadge: ({ evidence }) => {
    const pages = [...new Set((Array.isArray(evidence) ? evidence : []).map((item) => item?.page).filter(Boolean))];
    if (!pages.length) return null;
    return <span>{`p. ${pages.join(', ')}`}</span>;
  },
  EvidenceDetail: ({ evidence }) => {
    const pages = [...new Set((Array.isArray(evidence) ? evidence : []).map((item) => item?.page).filter(Boolean))];
    if (!pages.length) return null;
    return <div>{`Evidenza: p. ${pages.join(', ')}`}</div>;
  },
  DataValueWithEvidence: ({ label, value, evidence }) => {
    const pages = [...new Set((Array.isArray(evidence) ? evidence : []).map((item) => item?.page).filter(Boolean))];
    return (
      <div>
        <span>{label}</span>
        <span>{value}</span>
        {pages.length ? <span>{`p. ${pages.join(', ')}`}</span> : null}
      </div>
    );
  },
}));

jest.mock('sonner', () => ({
  toast: {
    error: jest.fn(),
    success: jest.fn(),
    info: jest.fn(),
  },
}));

const BANNED_PHRASES = [
  'NON SPECIFICATO IN PERIZIA',
  'step3_candidates',
  'no_packet',
  'unresolved_explained',
  'explanation_fallback_reason',
  'INTERNAL DIRTY',
];

const loadFixture = (analysisId) => JSON.parse(
  fs.readFileSync(`/tmp/periziascan_live_payloads/${analysisId}.json`, 'utf8')
);

const withEmptyIssues = (payload) => {
  const clone = JSON.parse(JSON.stringify(payload));
  clone.result.issues = [];
  return clone;
};

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

describe('AnalysisResult legacy grouped fallback render path', () => {
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

  test('renders legacy legal and red-flag content when canonical issues are empty', async () => {
    const payload = withEmptyIssues(loadFixture('analysis_956b0ab279c3'));

    expect(payload.result.issues).toEqual([]);
    expect(payload.result.section_9_legal_killers.top_items.length).toBeGreaterThan(0);
    expect(payload.result.section_11_red_flags.length).toBeGreaterThan(0);
    expect(payload.result.red_flags_operativi.length).toBeGreaterThan(0);

    await renderAnalysisResult(payload);
    expect(fullText()).toContain('Panoramica');
    expectBannedPhrasesAbsent();

    await clickTab('tab-legal');
    expect(fullText()).toContain('Formalità cancellabile: ipoteca');
    expect(fullText()).toContain('Background legale da non promuovere a rischio prioritario cliente');
    expect(fullText()).toContain('p. 10');
    expectBannedPhrasesAbsent();

    await clickTab('tab-flags');
    expect(fullText()).toContain('Tecnico / Compliance');
    expect(fullText()).toContain('Regolarità urbanistica: PRESENTI DIFFORMITA.');
    expect(fullText()).toContain('Costi espliciti a carico dell\'acquirente: € 3.600,00.');
    expect(fullText()).toContain('p. 8');
    expectBannedPhrasesAbsent();
  });
});
