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

const loadFixture = (analysisId) => JSON.parse(
  fs.readFileSync(`/tmp/periziascan_live_payloads/${analysisId}.json`, 'utf8')
);

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

describe('AnalysisResult canonical issues render path', () => {
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

  test('via_cristoforo switches from legacy formalita text to canonical cost issues', async () => {
    await renderAnalysisResult(loadFixture('analysis_956b0ab279c3'));
    await clickTab('tab-flags');

    expect(fullText()).toContain("Costi espliciti a carico dell'acquirente: € 3.600,00.");
    expect(fullText()).toContain('Spese condominiali arretrate: non trovato.');
    expect(fullText()).toContain("Verifica l'importo totale dei costi e il perimetro delle spese prima dell'offerta.");
    expect(fullText()).toContain('p. 4');
    expect(fullText()).not.toContain('Formalità cancellabile: ipoteca');
  });

  test('via_del_mare renders blocked canonical explanation, verify-next, and ambiguity text', async () => {
    await renderAnalysisResult(loadFixture('analysis_28bf95cdee5c'));
    await clickTab('tab-flags');

    expect(fullText()).toContain('Documento non leggibile o estrazione bloccata.');
    expect(fullText()).toContain('Documento parzialmente leggibile in automatico');
    expect(fullText()).toContain('Verifica manuale obbligatoria sul documento originale.');
    expect(fullText()).toContain('Ambiguità: Manca una base testuale affidabile e anchor-bound per decidere oltre.');
  });

  test('multilot preserves canonical titles, verify-next guidance, evidence, and bene scope', async () => {
    await renderAnalysisResult(loadFixture('analysis_a7f41b222261'));
    await clickTab('tab-legal');
    await clickTab('tab-flags');

    expect(fullText()).toContain('Agibilità assente / non rilasciata.');
    expect(fullText()).toContain('Opponibilità occupazione: NON VERIFICABILE.');
    expect(fullText()).toContain('Verifica titoli edilizi, agibilità/abitabilità e costi necessari per la regolarizzazione.');
    expect(fullText()).toContain("Verificare nell'avviso di vendita o nel prospetto finale il prezzo base d'asta del lotto.");
    expect(fullText()).toContain('Bene 3');
    expect(fullText()).toContain('Bene 1');
    expect(fullText()).toContain('p. 46');
    expect(fullText()).toContain('p. 70');
  });
});
