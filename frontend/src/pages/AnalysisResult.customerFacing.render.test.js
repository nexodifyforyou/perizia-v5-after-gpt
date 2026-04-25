import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import AnalysisResult from './AnalysisResult';

jest.mock('react-router-dom', () => ({
  Link: ({ children, to, ...props }) => <a href={to} {...props}>{children}</a>,
  useParams: () => ({ analysisId: 'test-analysis' }),
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
  EvidenceBadge: () => <span data-testid="evidence-badge" />,
  EvidenceDetail: () => null,
  DataValueWithEvidence: ({ label, value }) => (
    <div>
      <span>{label}</span>
      <span>{value}</span>
    </div>
  ),
}));

jest.mock('sonner', () => ({
  toast: {
    error: jest.fn(),
    success: jest.fn(),
    info: jest.fn(),
  },
}));

let container;
let root;

const renderAnalysisResult = async () => {
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
  await act(async () => {
    root.render(<AnalysisResult />);
    await Promise.resolve();
  });
};

const clickByTestId = async (testId) => {
  const node = container.querySelector(`[data-testid="${testId}"]`);
  if (!node) throw new Error(`Missing node with test id ${testId}`);
  await act(async () => {
    node.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    await Promise.resolve();
  });
};

const expectTextPresent = (text) => {
  expect(container.textContent.includes(text)).toBe(true);
};

const expectTextAbsent = (text) => {
  expect(container.textContent.includes(text)).toBe(false);
};

describe('AnalysisResult customer-facing render', () => {
  beforeEach(() => {
    globalThis.IS_REACT_ACT_ENVIRONMENT = true;
    document.body.innerHTML = '';
    window.__DEBUG_ANALYSIS_PAYLOAD__ = {
      analysis_id: 'test-analysis',
      case_id: 'case-1',
      case_title: 'Case Title',
      file_name: 'test.pdf',
      created_at: '2026-04-24T00:00:00Z',
      pages_count: 12,
      result: {
        report_header: {
          procedure: { value: 'Proc. 1' },
          tribunale: { value: 'Milano' },
          address: { value: 'Via Roma 1' },
          lotto: { value: 'Lotto Unico' },
        },
        section_1_semaforo_generale: {
          status: 'AMBER',
          status_it: 'ATTENZIONE',
          status_en: 'WARNING',
          status_label: 'INTERNAL DIRTY summary',
          reason_it: 'INTERNAL DIRTY explanation',
          reason_en: 'INTERNAL DIRTY action',
          top_blockers: ['Blocco A'],
          semaforo_complessivo: {
            evidence: [{ page: 3, quote: 'evidenza semaforo' }],
          },
        },
        section_2_decisione_rapida: {
          summary_it: 'INTERNAL DIRTY action',
          summary_en: 'Dirty EN',
        },
        decision_rapida_client: {
          summary_it: 'INTERNAL DIRTY action',
          summary_en: 'Dirty EN',
        },
        summary_for_client: {
          summary_it: 'INTERNAL DIRTY explanation',
          summary_en: 'Dirty summary en',
          disclaimer_it: 'Disclaimer',
          disclaimer_en: 'Disclaimer EN',
        },
        summary_for_client_bundle: {
          decision_summary_it: 'Clean bundle summary',
          decision_summary_en: 'Clean bundle summary en',
          top_issue_it: 'Clean top issue',
          next_step_it: 'Clean next step',
        },
        section_9_legal_killers: {
          top_items: [
            {
              headline_it: 'Clean legal title',
              killer: 'Immobile occupato',
              explanation_it: 'Clean explanation_it',
              verify_next_it: 'Clean verify_next_it',
              reason_it: 'Immobile occupato step3_candidates dirty text',
              action: 'Deterministic candidate-based cost dirty text',
              status: 'AMBER',
              status_it: 'ATTENZIONE',
              evidence: [{ page: 9, quote: 'legal quote' }],
            },
          ],
        },
        section_11_red_flags: [
          {
            headline_it: 'Clean flag title',
            flag_it: 'Legacy flag title',
            action_it: 'Clean action_it',
            explanation_it: 'Clean flag explanation',
            verify_next_it: 'Clean flag verify',
            explanation: 'INTERNAL DIRTY explanation',
            detail: 'step3_candidates dirty text',
            reason_it: 'Deterministic candidate-based cost dirty text',
            severity: 'AMBER',
            evidence: [{ page: 10, quote: 'flag quote' }],
          },
        ],
        red_flags_operativi: [
          {
            headline_it: 'Clean flag title',
            flag_it: 'Legacy flag title',
            action_it: 'Clean action_it',
            explanation_it: 'Clean flag explanation',
            verify_next_it: 'Clean flag verify',
            explanation: 'INTERNAL DIRTY explanation',
            detail: 'step3_candidates dirty text',
            reason_it: 'Deterministic candidate-based cost dirty text',
            severity: 'AMBER',
            evidence: [{ page: 10, quote: 'flag quote' }],
          },
        ],
        qa_pass: { status: 'PASS' },
        field_states: {},
        lots: [],
        panoramica_contract: {},
        section_3_money_box: { items: [] },
      },
    };
  });

  afterEach(() => {
    if (root) {
      act(() => {
        root.unmount();
      });
    }
    if (container && container.parentNode) {
      container.parentNode.removeChild(container);
    }
    container = null;
    root = null;
    delete window.__DEBUG_ANALYSIS_PAYLOAD__;
  });

  test('renders clean customer text and suppresses dirty legacy strings across overview, legal, and flags tabs', async () => {
    await renderAnalysisResult();

    expectTextPresent('Clean bundle summary');
    expectTextAbsent('INTERNAL DIRTY summary');
    expectTextAbsent('INTERNAL DIRTY action');
    expectTextAbsent('INTERNAL DIRTY explanation');

    await clickByTestId('tab-legal');
    expectTextPresent('Clean explanation_it');
    expectTextAbsent('step3_candidates dirty text');
    expectTextAbsent('Deterministic candidate-based cost dirty text');

    await clickByTestId('tab-flags');
    expectTextPresent('Clean action_it');
    expectTextAbsent('INTERNAL DIRTY explanation');
    expectTextAbsent('step3_candidates dirty text');
    expectTextAbsent('Deterministic candidate-based cost dirty text');
  });
});
