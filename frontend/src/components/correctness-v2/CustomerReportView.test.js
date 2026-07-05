import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import CustomerReportView from './CustomerReportView';
import { getCorrectnessV2CustomerView } from '../../lib/api/perizia';

jest.mock('../../lib/api/perizia', () => ({
  getCorrectnessV2CustomerView: jest.fn(),
}));

const sanitizedReport = {
  schema_version: 'cv2.customer_report.v1',
  analysis_id: 'analysis_generic',
  job_id: 'cv2_ready',
  report_status: 'REPORT_READY',
  report_status_label: 'Report pronto',
  title: 'Appartamento in Torino',
  subtitle: 'Lotto 1',
  decision: {
    level: 'attenzione',
    label: 'Attenzione',
    headline: 'Attenzione: sono presenti criticità che richiedono verifiche.',
    reason: 'Motivo principale: immobile occupato; condizioni strutturali critiche.',
    drivers: ['immobile occupato', 'condizioni strutturali critiche'],
  },
  case_identity: { tribunale: 'Tribunale di Torino', address: 'Via Esempio 6', property_type: 'Appartamento' },
  lot_structure: { selected_lot: 'Lotto 1', bene_count: 1 },
  executive_summary: [{ text: 'Valore di vendita giudiziaria indicato in perizia.', evidence_pages: [4] }],
  key_facts: [{ label: 'Valore di vendita giudiziaria', value_display: 'EUR 38.110,20', evidence_pages: [4] }],
  risk_sections: [
    { section_id: 'criticita', title: 'Criticità', items: [{ area: 'struttura', summary: 'Immobile collabente.', evidence_pages: [12] }] },
  ],
  money_sections: {
    valuation_chain: [{ label: 'Valore di vendita giudiziaria', amount_display: 'EUR 38.110,20', evidence_pages: [4] }],
    auction_terms: [],
    buyer_side_costs: [
      { label: 'Costi di cancellazione', amount_display: 'EUR 294,00', included_in_valuation: true, evidence_pages: [19] },
    ],
    procedure_cancelled_formalities: [{ label: 'Ipoteca cancellata', amount_display: 'EUR 150.000,00', evidence_pages: [10] }],
    uncertain_money: [],
  },
  beni_sections: [{ bene_id: '1', title: 'Bene principale: appartamento', accessories: [], risks: [], checklist: [] }],
  occupancy_section: { status: 'occupato', status_label: 'Occupato da inquilino', evidence_pages: [6] },
  compliance_section: [{ area: 'Agibilità', status_label: 'Da verificare', evidence_pages: [8] }],
  formalities_section: [{ type_label: 'Ipoteca', status_label: 'Cancellata', description: 'Cancellata dalla procedura.', evidence_pages: [10] }],
  buyer_checklist: [{ text: 'Verificare la situazione locativa.', evidence_pages: [6] }],
  customer_evidence_index: [
    { page: 8, topic: 'conformità urbanistica', perizia_excerpt: 'immobile conforme al PRGC' },
  ],
  disclaimer: 'Documento informativo, non sostituisce la perizia.',
};

const lotSelectionReport = {
  schema_version: 'cv2.customer_report.v1',
  analysis_id: 'analysis_generic',
  report_status: 'LOT_SELECTION_REQUIRED',
  report_status_label: 'Selezione del lotto richiesta',
  title: 'Selezione del lotto richiesta',
  decision: { level: 'da_verificare', label: 'Da verificare', headline: 'Da verificare', reason: '', drivers: [] },
  lot_selection: {
    message: 'Selezionare un lotto.',
    lots: [
      { lot_id: '1', label: 'Lotto 1 - Bene N° 1', address: 'Montecatini', property_type: 'Fabbricato', money_summary: [{ label: 'Prezzo base', amount_display: 'EUR 64.198,00' }] },
      { lot_id: '2', label: 'Lotto 2 - Bene N° 2', address: 'Pieve a Nievole', property_type: 'Magazzino', money_summary: [] },
    ],
  },
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

const render = async (props = {}) => {
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
  await act(async () => {
    root.render(<CustomerReportView analysisId="analysis_generic" {...props} />);
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

describe('CustomerReportView', () => {
  beforeEach(() => {
    globalThis.IS_REACT_ACT_ENVIRONMENT = true;
    document.body.innerHTML = '';
    jest.clearAllMocks();
  });

  afterEach(() => {
    if (root) act(() => root.unmount());
    if (container?.parentNode) container.parentNode.removeChild(container);
    container = null;
    root = null;
  });

  test('renders the sanitized report with a decision box, money summary and evidence', async () => {
    getCorrectnessV2CustomerView.mockResolvedValue({ data: { available: true, report: sanitizedReport } });
    await render();

    expect(container.querySelector('[data-testid="cv2-customer-view"]')).toBeTruthy();
    expect(container.querySelector('[data-testid="cv2-customer-decision"]').textContent).toContain('Attenzione');
    expect(text()).toContain('immobile occupato');
    expect(container.querySelector('[data-testid="cv2-customer-money"]').textContent).toContain('EUR 38.110,20');
    expect(text()).toContain('Già incluso nel valore finale');
    expect(container.querySelector('[data-testid="cv2-customer-evidence"]').textContent).toContain('immobile conforme al PRGC');
    expect(text()).toContain('Documento informativo');
  });

  test('never renders admin/debug internals in the customer view', async () => {
    getCorrectnessV2CustomerView.mockResolvedValue({ data: { available: true, report: sanitizedReport } });
    await render();

    const body = text();
    expect(body).not.toContain('REPORT_READY');
    expect(body).not.toContain('Run Correctness V2');
    expect(body).not.toContain('Controllo qualità');
    expect(body).not.toContain('Debug evidenze');
    expect(body).not.toContain('step3:');
    expect(container.querySelector('[data-testid="run-correctness-v2-button"]')).toBeNull();
    expect(container.querySelector('[data-testid="cv2-quality-control"]')).toBeNull();
    expect(container.querySelector('[data-testid="cv2-evidence-admin-debug"]')).toBeNull();
  });

  test('shows an unavailable message when no customer report exists', async () => {
    getCorrectnessV2CustomerView.mockResolvedValue({ data: { available: false, reason_code: 'NO_CUSTOMER_REPORT' } });
    await render();

    expect(container.querySelector('[data-testid="cv2-customer-unavailable"]')).toBeTruthy();
    expect(text()).toContain('non è ancora disponibile');
  });

  test('lot selection: choosing a lot refetches the customer view with the selected lot', async () => {
    getCorrectnessV2CustomerView
      .mockResolvedValueOnce({ data: { available: true, report: lotSelectionReport } })
      .mockResolvedValueOnce({ data: { available: true, report: { ...sanitizedReport, subtitle: 'Lotto 1 selezionato' } } });

    await render();

    expect(container.querySelector('[data-testid="cv2-customer-lot-selector"]')).toBeTruthy();
    expect(text()).toContain('Vedi report lotto');

    await click('[data-testid="cv2-customer-lot-view-1"]');

    const lastCall = getCorrectnessV2CustomerView.mock.calls.at(-1);
    expect(lastCall[1]).toEqual({ selected_lot_id: '1' });
    expect(container.querySelector('[data-testid="cv2-customer-report"]')).toBeTruthy();
    expect(text()).toContain('Torna alla scelta del lotto');
  });

  test('treats a 404 (feature disabled) as unavailable, not an error', async () => {
    getCorrectnessV2CustomerView.mockRejectedValue({ response: { status: 404 } });
    await render();
    expect(container.querySelector('[data-testid="cv2-customer-unavailable"]')).toBeTruthy();
  });
});
