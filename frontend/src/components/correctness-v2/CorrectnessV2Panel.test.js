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
    buyer_side_costs: [
      { label: 'Spese condominiali buyer-side', amount: 2500, amount_display: 'EUR 2.500,00', evidence_pages: [9] },
      {
        label: 'Costi di cancellazione formalità',
        amount: 294,
        amount_display: 'EUR 294,00',
        included_in_valuation: true,
        notes: 'Già considerato nella catena di valore.',
        evidence_pages: [19],
      },
    ],
    procedure_cancelled_formalities: [{ label: 'Ipoteca cancellata dalla procedura', amount: 800, amount_display: 'EUR 800,00', evidence_pages: [10] }],
    market_comparatives: [
      {
        label: 'Comparativo 1 - valore OMI medio',
        amount: 870,
        amount_display: 'EUR 870,00',
        status: 'comparativo',
        status_label: 'Comparativo di mercato (dato di contesto)',
        evidence_pages: [16],
      },
    ],
    context_values: [
      {
        label: 'Rendita catastale',
        amount: 472.56,
        amount_display: 'EUR 472,56',
        status: 'contesto',
        status_label: 'Dato economico di contesto (non è un costo)',
        evidence_pages: [2],
      },
    ],
    uncertain_money: [{ label: 'Importo senza ruolo certo', amount: 3000, amount_display: 'EUR 3.000,00', status: 'da_verificare', evidence_pages: [11] }],
  },
  occupancy_section: {
    title: 'Stato di occupazione',
    status: 'occupato',
    status_label: 'Occupato',
    title_info: 'Contratto di locazione registrato in data certa.',
    registration_dates: ['01/01/2020'],
    expiry_dates: ['31/12/2024'],
    risks: ['La scadenza contrattuale risulta superata.'],
    evidence_pages: [3],
  },
  compliance_section: [
    { area: 'urbanistica', classification: 'conforming', status_label: 'conforme secondo la perizia', evidence_pages: [8] },
    { area: 'edilizia', classification: 'regularizable', status_label: 'regolarizzabile secondo la perizia', cost: 2500, cost_display: 'EUR 2.500,00', evidence_pages: [8] },
  ],
  formalities_section: [
    {
      type: 'ipoteca',
      description: 'Ipoteca volontaria iscritta.',
      status_label: 'Formalità rilevata; cancellazione indicata a cura della procedura',
      cancelled_by_procedure: true,
      buyer_burden: false,
      amount: 150000,
      amount_display: 'EUR 150.000,00',
      amount_note: 'Importo della formalità iscritta: non è un debito a carico dell\'acquirente salvo diversa indicazione della perizia.',
      evidence_pages: [5],
    },
  ],
  surfaces_section: [
    { label: 'Superficie commerciale', value: '46,95', evidence_pages: [2] },
    { label: 'Rendita catastale', value: '472,56', evidence_pages: [2] },
  ],
  buyer_checklist: [{ action: 'Verificare amministratore', detail: 'Confermare spese condominiali', evidence_pages: [9] }],
  manual_review_flags: [{ kind: 'uncertain_money', kind_label: 'Importo da verificare', detail: 'Importo senza ruolo certo', evidence_pages: [11] }],
  evidence_index: [{ page: 4, referenced_by: ['valuation_chain'] }],
  customer_evidence_index: [
    {
      page: 8,
      topic: 'Conformità urbanistica',
      report_section: 'Conformità e documenti tecnici',
      perizia_excerpt: 'L\'immobile risulta conforme.',
      excerpt_truncated: false,
      coverage_status: 'covered',
    },
    {
      page: 19,
      topic: 'Costi di cancellazione formalità',
      report_section: 'Valori e costi',
      perizia_excerpt: 'Spese di cancellazione delle trascrizioni ed iscrizioni a carico dell\'acquirente: €. 294,00',
      excerpt_truncated: false,
      coverage_status: 'covered',
    },
    {
      page: 13,
      topic: 'APE / prestazione energetica',
      report_section: 'Conformità e documenti tecnici',
      perizia_excerpt: null,
      note: 'Estratto non disponibile automaticamente; verificare pagina 13.',
      coverage_status: 'excerpt_missing',
    },
  ],
  admin_evidence_index: [
    {
      page: 8,
      raw_keys: ['technical_compliance[2]:urbanistica', 'risk_classification[5]:edilizia'],
      artifact_source: 'verified_report_contract.json',
    },
  ],
  quality_control: {
    title: 'Controllo qualità pagina per pagina',
    coverage_status: 'PASS',
    quality_status: 'PASS',
    customer_readiness: 'READY',
    satisfaction_score: 97,
    satisfaction_status: 'CUSTOMER_READY',
    blocking_issue_count: 0,
    warning_count: 0,
    columns: ['Pagina', 'Dato rilevante nella perizia', 'Presente nel report', 'Esito', 'Note'],
    rows: [
      { pagina: 2, dato: 'Valore di mercato: € 100.000,00', presente: true, esito: 'Coperto', note: '' },
      { pagina: 5, dato: 'Ipoteca', presente: true, esito: 'Coperto', note: '' },
      { pagina: 8, dato: 'Agibilità / abitabilità', presente: true, esito: 'Da verificare', note: 'Reso come punto da verificare.' },
    ],
    page_summary: [],
  },
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

  test('renders the quality section with the page-by-page audit table', async () => {
    getLatestCorrectnessV2Job.mockResolvedValue({ data: readyJob });
    getCorrectnessV2CustomerReport.mockResolvedValue({ data: reportReady });

    await renderPanel();

    const quality = container.querySelector('[data-testid="cv2-quality-control"]');
    expect(quality).not.toBeNull();
    expect(quality.textContent).toContain('Controllo qualità pagina per pagina');
    const table = container.querySelector('[data-testid="cv2-quality-table"]');
    expect(table.textContent).toContain('Dato rilevante nella perizia');
    expect(table.textContent).toContain('Valore di mercato: € 100.000,00');
    expect(table.textContent).toContain('Coperto');
    expect(table.textContent).toContain('Da verificare');
  });

  test('customer-facing sections use Italian labels and hide raw internal prefixes', async () => {
    getLatestCorrectnessV2Job.mockResolvedValue({ data: readyJob });
    getCorrectnessV2CustomerReport.mockResolvedValue({ data: reportReady });

    await renderPanel();

    expect(text()).toContain('Dati principali');
    expect(text()).toContain('Struttura lotto / beni');
    expect(text()).toContain('Sintesi esecutiva');
    expect(text()).toContain('Catena di valore');
    expect(text()).toContain("Costi a carico dell'acquirente");
    expect(text()).toContain('Checklist acquirente');
    expect(text()).toContain('Punti da verificare');
    // Raw internal machine kinds must not be shown as flag prefixes.
    const flags = container.querySelector('[data-testid="cv2-manual-review-flags"]');
    expect(flags.textContent).toContain('Importo da verificare');
    expect(flags.textContent).not.toContain('uncertain_money');
    // Grid keys are translated.
    expect(text()).toContain('Tipologia');
    expect(text()).not.toContain('property type');
  });

  test('renders occupancy, compliance, formalities and surfaces sections', async () => {
    getLatestCorrectnessV2Job.mockResolvedValue({ data: readyJob });
    getCorrectnessV2CustomerReport.mockResolvedValue({ data: reportReady });

    await renderPanel();

    expect(container.querySelector('[data-testid="cv2-occupancy-section"]').textContent)
      .toContain('Contratto di locazione registrato');
    const compliance = container.querySelector('[data-testid="cv2-compliance-section"]');
    expect(compliance.textContent).toContain('conforme secondo la perizia');
    expect(compliance.textContent).toContain('regolarizzabile secondo la perizia');
    const formalities = container.querySelector('[data-testid="cv2-formalities-section"]');
    expect(formalities.textContent).toContain('cancellazione indicata a cura della procedura');
    expect(formalities.textContent).toContain('non è un debito a carico');
    const surfaces = container.querySelector('[data-testid="cv2-surfaces-section"]');
    expect(surfaces.textContent).toContain('Superficie commerciale');
    expect(surfaces.textContent).toContain('Rendita catastale');
  });

  test('failed coverage never renders a clean report', async () => {
    getLatestCorrectnessV2Job.mockResolvedValue({ data: readyJob });
    getCorrectnessV2CustomerReport.mockResolvedValue({
      data: {
        ...reportReady,
        quality_control: {
          ...reportReady.quality_control,
          coverage_status: 'FAIL',
          quality_status: 'FAIL',
          blocking_issue_count: 2,
          rows: [
            { pagina: 4, dato: 'Valore di vendita giudiziaria: € 120.000,00', presente: false, esito: 'Mancante', note: 'Omissione critica.' },
          ],
        },
      },
    });

    await renderPanel();

    // The clean report body must NOT render...
    expect(container.querySelector('[data-testid="cv2-report"]')).toBeNull();
    // ...and the failure + audit table must be visible instead.
    expect(container.querySelector('[data-testid="cv2-coverage-failed"]')).not.toBeNull();
    const quality = container.querySelector('[data-testid="cv2-quality-control"]');
    expect(quality.textContent).toContain('Mancante');
  });

  test('renders comparatives and context values separately from importi da verificare', async () => {
    getLatestCorrectnessV2Job.mockResolvedValue({ data: readyJob });
    getCorrectnessV2CustomerReport.mockResolvedValue({ data: reportReady });

    await renderPanel();

    const comparatives = container.querySelector('[data-testid="cv2-money-section-market_comparatives"]');
    expect(comparatives).not.toBeNull();
    expect(comparatives.textContent).toContain('Comparativi di mercato');
    expect(comparatives.textContent).toContain('Comparativo 1 - valore OMI medio');
    const context = container.querySelector('[data-testid="cv2-money-section-context_values"]');
    expect(context.textContent).toContain('Dati economici di contesto');
    expect(context.textContent).toContain('Rendita catastale');
    const uncertain = container.querySelector('[data-testid="cv2-money-section-uncertain_money"]');
    expect(uncertain.textContent).toContain('Importo senza ruolo certo');
    expect(uncertain.textContent).not.toContain('Comparativo 1');
    expect(uncertain.textContent).not.toContain('Rendita catastale');
  });

  test('buyer-side cost already in the valuation chain shows the inclusion badge', async () => {
    getLatestCorrectnessV2Job.mockResolvedValue({ data: readyJob });
    getCorrectnessV2CustomerReport.mockResolvedValue({ data: reportReady });

    await renderPanel();

    const buyer = container.querySelector('[data-testid="cv2-money-section-buyer_side_costs"]');
    expect(buyer.textContent).toContain('Costi di cancellazione formalità');
    expect(buyer.textContent).toContain('EUR 294,00');
    expect(buyer.textContent).toContain('Già nella catena di valore');
    expect(buyer.textContent).toContain('Già considerato nella catena di valore.');
  });

  test('customer evidence index shows page, topic and verbatim excerpt without raw keys', async () => {
    getLatestCorrectnessV2Job.mockResolvedValue({ data: readyJob });
    getCorrectnessV2CustomerReport.mockResolvedValue({ data: reportReady });

    await renderPanel();

    const evidence = container.querySelector('[data-testid="cv2-evidence-index"]');
    expect(evidence).not.toBeNull();
    expect(evidence.textContent).toContain('p. 8');
    expect(evidence.textContent).toContain('Conformità urbanistica');
    expect(evidence.textContent).toContain('Estratto perizia');
    expect(evidence.textContent).toContain('L\'immobile risulta conforme.');
    expect(evidence.textContent).toContain('294,00');
    // Missing excerpt entries say so explicitly instead of inventing text.
    expect(evidence.textContent).toContain('Estratto non disponibile automaticamente; verificare pagina 13.');
    // Raw internal keys never render in the customer evidence list.
    expect(evidence.textContent).not.toContain('technical_compliance[');
    expect(evidence.textContent).not.toContain('risk_classification[');
    // ...they stay in the collapsed admin debug block only.
    const adminDebug = container.querySelector('[data-testid="cv2-evidence-admin-debug"]');
    expect(adminDebug).not.toBeNull();
    expect(adminDebug.hasAttribute('open')).toBe(false);
    expect(adminDebug.textContent).toContain('technical_compliance[2]');
  });

  test('renders bene principale with accessories when no explicit beni exist', async () => {
    getLatestCorrectnessV2Job.mockResolvedValue({ data: readyJob });
    getCorrectnessV2CustomerReport.mockResolvedValue({
      data: {
        ...reportReady,
        lot_structure: { multi_lot: false, selected_lot: '1', bene_count: 1, detected_bene_count: 0 },
        beni_sections: [
          {
            bene_id: 'principale',
            title: 'Bene principale: appartamento',
            is_main_property: true,
            property_type: 'appartamento',
            address: 'Via di esempio 1',
            accessories: [
              { label: 'soffitta', evidence_pages: [2], note: 'Accessorio/pertinenza del bene principale secondo la perizia.' },
            ],
            risks: [],
            checklist: [],
            note: null,
          },
        ],
      },
    });

    await renderPanel();

    expect(text()).toContain('Bene principale: appartamento');
    expect(text()).toContain('Numero beni');
    expect(text()).not.toContain('Nessuna sezione beni separata');
    const accessories = container.querySelector('[data-testid="cv2-bene-accessories-principale"]');
    expect(accessories).not.toBeNull();
    expect(accessories.textContent).toContain('soffitta');
    expect(accessories.textContent).toContain('p. 2');
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
