import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import CorrectnessV2Panel from './CorrectnessV2Panel';
import {
  getCorrectnessV2CustomerReport,
  getLatestCorrectnessV2Job,
  startCorrectnessV2,
} from '../../lib/api/perizia';

jest.mock('../../lib/api/perizia', () => ({
  getCorrectnessV2CustomerReport: jest.fn(),
  getCorrectnessV2Job: jest.fn(),
  getLatestCorrectnessV2Job: jest.fn(),
  startCorrectnessV2: jest.fn(),
}));

const readyJob = {
  job_id: 'cv2_ready',
  analysis_id: 'analysis_generic',
  status: 'REPORT_READY',
  current_stage: 'step3:report_ready',
  customer_report_generated: true,
  safe_to_show_customer: true,
  artifacts_saved: { customer_report: '/admin-only/path/customer_report.json' },
};

const reportReady = {
  schema_version: 'cv2.customer_report.v1',
  analysis_id: 'analysis_generic',
  job_id: 'cv2_ready',
  report_status: 'REPORT_READY',
  title: 'Report generico',
  subtitle: 'Lotto Unico',
  case_identity: { property_type: 'Appartamento', address: 'Via di esempio' },
  lot_structure: { multi_lot: false, selected_lot: 'Lotto Unico', bene_count: 2 },
  beni_sections: [
    { bene_id: '1', title: 'Bene 1', risks: [], checklist: [], note: 'Nessuna segnalazione specifica.' },
    { bene_id: '2', title: 'Bene 2', risks: [], checklist: [], note: 'Nessuna segnalazione specifica.' },
  ],
  executive_summary: [{ text: 'Valore di vendita giudiziaria indicato in perizia.', evidence_pages: [4] }],
  key_facts: [{ label: 'Valore di vendita giudiziaria', value: 120000, value_display: 'EUR 120.000,00', evidence_pages: [4] }],
  risk_sections: [
    { section_id: 'da_verificare', title: 'Aspetti da verificare', items: [{ area: 'agibilita', status_label: 'Da verificare', summary: 'Documentazione non reperita.', evidence_pages: [8] }] },
  ],
  money_sections: {
    valuation_chain: [{ label: 'Valore di vendita giudiziaria', amount: 120000, amount_display: 'EUR 120.000,00', evidence_pages: [4] }],
    auction_terms: [],
    buyer_side_costs: [{ label: 'Spese condominiali buyer-side', amount: 2500, amount_display: 'EUR 2.500,00', evidence_pages: [9] }],
    procedure_cancelled_formalities: [{ label: 'Ipoteca cancellata dalla procedura', amount: 800, amount_display: 'EUR 800,00', evidence_pages: [10] }],
    uncertain_money: [{ label: 'Importo senza ruolo certo', amount: 3000, amount_display: 'EUR 3.000,00', status: 'da_verificare', evidence_pages: [11] }],
  },
  buyer_checklist: [{ action: 'Verificare amministratore', detail: 'Confermare spese condominiali', evidence_pages: [9] }],
  manual_review_flags: [{ kind: 'uncertain_money', detail: 'Importo senza ruolo certo', evidence_pages: [11] }],
  evidence_index: [{ page: 4, referenced_by: ['valuation_chain'] }],
  disclaimer: 'Disclaimer generico.',
};

const lotSelectionJob = {
  job_id: 'cv2_lots',
  analysis_id: 'analysis_generic',
  status: 'LOT_SELECTION_REQUIRED',
  current_stage: 'step3:lot_selection_required',
  customer_report_generated: true,
  safe_to_show_customer: true,
  reason_code: 'LOT_SELECTION_REQUIRED',
  available_lots: [
    { lot_id: '1', label: 'Lotto 1', address: 'Via Lotto 1', property_type: 'Appartamento', key_money: [{ label: 'Valore lotto', amount_display: 'EUR 100.000,00' }], confidence: 'alta' },
    { lot_id: '2', label: 'Lotto 2', address: 'Via Lotto 2', property_type: 'Garage', key_money: [{ label: 'Valore lotto', amount_display: 'EUR 20.000,00' }], confidence: 'media' },
  ],
  artifacts_saved: { customer_report: '/admin-only/path/customer_report.json' },
};

const lotSelectionReport = {
  ...reportReady,
  job_id: 'cv2_lots',
  report_status: 'LOT_SELECTION_REQUIRED',
  title: 'Selezione del lotto richiesta',
  subtitle: 'La perizia contiene 2 lotti distinti',
  case_identity: {},
  lot_structure: { multi_lot: true, lot_count: 2, lot_ids: ['1', '2'], selected_lot: null },
  key_facts: [],
  risk_sections: [],
  money_sections: {
    valuation_chain: [],
    auction_terms: [],
    buyer_side_costs: [],
    procedure_cancelled_formalities: [],
    uncertain_money: [],
  },
  lot_selection: {
    message: 'Rilevati 2 lotti distinti.',
    lots: lotSelectionJob.available_lots.map((lot) => ({ ...lot, money_summary: lot.key_money, evidence_pages: [1] })),
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

const renderPanel = async (props = {}) => {
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
  await act(async () => {
    root.render(<CorrectnessV2Panel analysisId="analysis_generic" isAdmin {...props} />);
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
};

const text = () => container.textContent || '';

describe('CorrectnessV2Panel', () => {
  beforeEach(() => {
    globalThis.IS_REACT_ACT_ENVIRONMENT = true;
    document.body.innerHTML = '';
    jest.clearAllMocks();
  });

  afterEach(() => {
    if (root) {
      act(() => {
        root.unmount();
      });
    }
    if (container?.parentNode) container.parentNode.removeChild(container);
    container = null;
    root = null;
  });

  test('does not render the admin preview for non-admin users', async () => {
    await renderPanel({ isAdmin: false });
    expect(text()).toBe('');
    expect(getLatestCorrectnessV2Job).not.toHaveBeenCalled();
  });

  test('renders REPORT_READY customer_report money sections separately', async () => {
    getLatestCorrectnessV2Job.mockResolvedValue({ data: readyJob });
    getCorrectnessV2CustomerReport.mockResolvedValue({ data: reportReady });

    await renderPanel();

    expect(text()).toContain('REPORT_READY');
    expect(text()).toContain('Valore di vendita giudiziaria');
    expect(container.querySelector('[data-testid="cv2-money-section-buyer_side_costs"]').textContent).toContain('Spese condominiali buyer-side');
    expect(container.querySelector('[data-testid="cv2-money-section-procedure_cancelled_formalities"]').textContent).toContain('Ipoteca cancellata dalla procedura');
    expect(container.querySelector('[data-testid="cv2-money-section-uncertain_money"]').textContent).toContain('Importo senza ruolo certo');
  });

  test('renders LOT_SELECTION_REQUIRED and starts selected lot exactly once', async () => {
    getLatestCorrectnessV2Job.mockResolvedValue({ data: lotSelectionJob });
    getCorrectnessV2CustomerReport
      .mockResolvedValueOnce({ data: lotSelectionReport })
      .mockResolvedValueOnce({ data: reportReady });
    startCorrectnessV2.mockResolvedValue({ data: readyJob });

    await renderPanel();
    expect(container.querySelector('[data-testid="cv2-lot-selector"]').textContent).toContain('Lotto 1');

    await click('[data-testid="cv2-lot-analyze-1"]');
    await flush();

    expect(startCorrectnessV2).toHaveBeenCalledTimes(1);
    expect(startCorrectnessV2).toHaveBeenCalledWith(
      'analysis_generic',
      { selected_lot_id: '1' },
      expect.objectContaining({ signal: expect.any(AbortSignal) })
    );
  });

  test('guards against duplicate run clicks while a start request is in flight', async () => {
    getLatestCorrectnessV2Job.mockRejectedValue({ response: { status: 404 } });
    startCorrectnessV2.mockReturnValue(new Promise(() => {}));

    await renderPanel();
    await act(async () => {
      const button = container.querySelector('[data-testid="run-correctness-v2-button"]');
      button.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      button.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(startCorrectnessV2).toHaveBeenCalledTimes(1);
  });

  test('validation failures show manual review state and no fake report sections', async () => {
    getLatestCorrectnessV2Job.mockResolvedValue({
      data: {
        job_id: 'cv2_failed',
        analysis_id: 'analysis_generic',
        status: 'CONTRACT_VALIDATION_FAILED',
        reason_code: 'CONTRACT_VALIDATION_FAILED',
        reason_human: 'La validazione deterministica ha rifiutato il foglio di lavoro.',
        customer_report_generated: false,
        safe_to_show_customer: false,
        artifacts_saved: { customer_report: '/admin-only/path/customer_report.json' },
      },
    });
    getCorrectnessV2CustomerReport.mockResolvedValue({
      data: {
        ...reportReady,
        job_id: 'cv2_failed',
        report_status: 'CONTRACT_VALIDATION_FAILED',
        title: 'Report non disponibile: verifica non superata',
        subtitle: 'La validazione deterministica ha rifiutato il foglio di lavoro.',
        key_facts: [],
        money_sections: {
          valuation_chain: [],
          auction_terms: [],
          buyer_side_costs: [],
          procedure_cancelled_formalities: [],
          uncertain_money: [],
        },
        manual_review_flags: [{ kind: 'status', detail: 'La validazione deterministica ha rifiutato il foglio di lavoro.' }],
      },
    });

    await renderPanel();

    expect(container.querySelector('[data-testid="cv2-manual-review"]').textContent).toContain('Report non disponibile');
    expect(text()).not.toContain("Prezzo base d'asta");
    expect(container.querySelector('[data-testid="cv2-report"]')).toBeNull();
  });

  test('disables Run Correctness V2 while a latest job is still running', async () => {
    getLatestCorrectnessV2Job.mockResolvedValue({
      data: {
        job_id: 'cv2_running',
        analysis_id: 'analysis_generic',
        status: 'RUNNING',
        current_stage: 'step2:analyst',
        customer_report_generated: false,
        safe_to_show_customer: false,
        artifacts_saved: {},
      },
    });

    await renderPanel();

    expect(text()).toContain('RUNNING');
    expect(container.querySelector('[data-testid="run-correctness-v2-button"]').disabled).toBe(true);
  });
});
