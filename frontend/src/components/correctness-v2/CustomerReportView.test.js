import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import CustomerReportView from './CustomerReportView';
import {
  getCorrectnessV2CustomerView,
  submitCorrectnessV2MoneyConfirmation,
} from '../../lib/api/perizia';

jest.mock('../../lib/api/perizia', () => ({
  getCorrectnessV2CustomerView: jest.fn(),
  submitCorrectnessV2MoneyConfirmation: jest.fn(),
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

const moneyConfirmationReport = {
  schema_version: 'cv2.customer_report.v1',
  analysis_id: 'analysis_generic',
  job_id: 'cv2_money',
  report_status: 'MONEY_CONFIRMATION_REQUIRED',
  report_status_label: 'Conferma importi richiesta',
  title: 'Conferma richiesta su alcuni importi',
  decision: { level: 'da_verificare', label: 'Da verificare', headline: 'Da verificare', reason: '', drivers: [] },
  money_sections: { valuation_chain: [{ label: 'Valore di mercato', amount_display: 'EUR 452.494,00', evidence_pages: [12] }], auction_terms: [], buyer_side_costs: [], procedure_cancelled_formalities: [], uncertain_money: [] },
  money_confirmation: {
    message: 'Servono alcune conferme sugli importi.',
    ambiguities: [
      {
        ambiguity_id: 'p12:valore_vendita:3',
        amount_display: 'EUR 452.494,00',
        page: 12,
        evidence_pages: [12],
        excerpt: 'Valore di vendita giudiziaria: EUR 452.494,00',
        question: 'Per l\'importo EUR 452.494,00 (pag. 12) abbiamo trovato due possibili interpretazioni. Quale è corretta?',
        options: [
          { option_id: 'judicial_sale_value', label: 'valore di vendita giudiziaria' },
          { option_id: 'market_value', label: 'valore di mercato' },
        ],
      },
    ],
  },
};

const notReadableReport = {
  schema_version: 'cv2.customer_report.v1',
  analysis_id: 'analysis_generic',
  job_id: 'cv2_unreadable',
  report_status: 'DOCUMENT_NOT_READABLE',
  report_status_label: 'Documento non leggibile',
  title: 'Perizia non leggibile',
  decision: {
    level: 'non_leggibile',
    label: 'Documento non leggibile',
    headline: 'Non riusciamo a leggere il contenuto della perizia.',
    reason: '',
    drivers: [],
  },
  // Deliberately carries normal-body fields too, to prove the not-readable
  // branch takes precedence and never renders the standard report.
  money_sections: { valuation_chain: [{ label: 'Valore di mercato', amount_display: 'EUR 999.999,00', evidence_pages: [1] }], auction_terms: [], buyer_side_costs: [], procedure_cancelled_formalities: [], uncertain_money: [] },
};

const documentNotReadableReport = {
  schema_version: 'cv2.customer_report.v1',
  analysis_id: 'analysis_generic',
  job_id: 'cv2_unreadable',
  report_status: 'DOCUMENT_NOT_READABLE',
  report_status_label: 'Perizia non leggibile',
  title: 'Perizia non leggibile',
  decision: {
    level: 'non_leggibile',
    label: 'Non leggibile',
    headline: 'Non è stato possibile leggere la perizia caricata.',
    reason: 'Caricare un PDF leggibile con testo selezionabile e riprovare.',
    drivers: [],
  },
  // A DOCUMENT_NOT_READABLE report intentionally carries no perizia facts.
  money_sections: { valuation_chain: [], auction_terms: [], buyer_side_costs: [], procedure_cancelled_formalities: [], uncertain_money: [] },
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

  test('NO_REPORT: historical analysis without a V2 report shows the exact copy', async () => {
    getCorrectnessV2CustomerView.mockResolvedValue({ data: { available: false, reason_code: 'NO_REPORT' } });
    await render();

    expect(container.querySelector('[data-testid="cv2-customer-unavailable"]')).toBeTruthy();
    expect(text()).toContain('Il nuovo report cliente non è ancora disponibile per questa analisi.');
    expect(container.querySelector('[data-testid="legacy-report-body"]')).toBeNull();
  });

  test('VERIFICATION_REQUIRED: shows the verification copy, never internal codes', async () => {
    getCorrectnessV2CustomerView.mockResolvedValue({
      data: { available: false, preparing: false, reason_code: 'VERIFICATION_REQUIRED' },
    });
    await render();

    expect(container.querySelector('[data-testid="cv2-customer-verification-required"]')).toBeTruthy();
    expect(text()).toContain('Report cliente non disponibile: verifica tecnica richiesta.');
    expect(text()).not.toContain('CONTRACT_VALIDATION_FAILED');
    expect(text()).not.toContain('NEEDS_MANUAL_REVIEW');
    expect(container.querySelector('[data-testid="legacy-report-body"]')).toBeNull();
  });

  test('SERVICE_BUSY: shows the busy copy, never internal codes', async () => {
    getCorrectnessV2CustomerView.mockResolvedValue({
      data: { available: false, preparing: false, reason_code: 'SERVICE_BUSY' },
    });
    await render();

    expect(container.querySelector('[data-testid="cv2-customer-service-busy"]')).toBeTruthy();
    expect(text()).toContain(
      "Il servizio è momentaneamente occupato e non disponibile. Riprova tra qualche minuto oppure contatta l'amministratore."
    );
    expect(text()).not.toContain('OPENAI_QUOTA_EXHAUSTED');
    expect(container.querySelector('[data-testid="legacy-report-body"]')).toBeNull();
  });

  test('SERVICE_UNAVAILABLE: safe message plus a working retry action', async () => {
    getCorrectnessV2CustomerView
      .mockResolvedValueOnce({
        data: { available: false, preparing: false, reason_code: 'SERVICE_UNAVAILABLE' },
      })
      .mockResolvedValueOnce({ data: { available: true, report: sanitizedReport } });
    await render();

    expect(container.querySelector('[data-testid="cv2-customer-service-unavailable"]')).toBeTruthy();
    expect(text()).toContain('Il servizio non è al momento disponibile. Riprova più tardi.');
    expect(container.querySelector('[data-testid="legacy-report-body"]')).toBeNull();

    await click('[data-testid="cv2-customer-retry"]');
    expect(getCorrectnessV2CustomerView).toHaveBeenCalledTimes(2);
    expect(container.querySelector('[data-testid="cv2-customer-report"]')).toBeTruthy();
  });

  test('a fetch failure (non-404) degrades to the service-unavailable state', async () => {
    getCorrectnessV2CustomerView.mockRejectedValue(new Error('network down'));
    await render();

    expect(container.querySelector('[data-testid="cv2-customer-service-unavailable"]')).toBeTruthy();
    expect(container.querySelector('[data-testid="legacy-report-body"]')).toBeNull();
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

  test('shows a preparing state (not unavailable) while the backend generates the report', async () => {
    getCorrectnessV2CustomerView.mockResolvedValue({
      data: { available: false, preparing: true, reason_code: 'PREPARING' },
    });
    await render();
    expect(container.querySelector('[data-testid="cv2-customer-preparing"]')).toBeTruthy();
    expect(container.querySelector('[data-testid="cv2-customer-unavailable"]')).toBeNull();
    expect(text()).toContain('in preparazione');
    // Reassuring processing-time expectation so waiting feels normal.
    expect(text()).toContain('Di solito bastano 1–2 minuti');
    expect(text()).toContain('Puoi lasciare questa pagina aperta');
  });

  test('selecting a lot with no report keeps the customer flow (pending box + back to lots)', async () => {
    getCorrectnessV2CustomerView
      .mockResolvedValueOnce({ data: { available: true, report: lotSelectionReport } })
      .mockResolvedValueOnce({ data: { available: false, preparing: true, reason_code: 'PREPARING' } })
      .mockResolvedValueOnce({ data: { available: true, report: lotSelectionReport } });

    await render();
    await click('[data-testid="cv2-customer-lot-view-2"]');

    expect(container.querySelector('[data-testid="cv2-customer-lot-pending"]')).toBeTruthy();
    expect(text()).toContain('in preparazione');
    expect(text()).toContain('Di solito bastano 1–2 minuti');

    await click('[data-testid="cv2-customer-back-to-lots"]');
    expect(container.querySelector('[data-testid="cv2-customer-lot-selector"]')).toBeTruthy();
  });

  test('evidence: curated preview by default, full list behind an explicit toggle', async () => {
    const manyEvidence = Array.from({ length: 20 }, (_, i) => ({
      page: i + 1,
      topic: `Tema ${i + 1}`,
      report_section: `Sezione ${i % 4}`,
      perizia_excerpt: `Estratto numero ${i + 1}.`,
    }));
    getCorrectnessV2CustomerView.mockResolvedValue({
      data: { available: true, report: { ...sanitizedReport, customer_evidence_index: manyEvidence } },
    });
    await render();

    const evidenceSection = container.querySelector('[data-testid="cv2-customer-evidence"]');
    expect(evidenceSection).toBeTruthy();
    // Preview is curated: far fewer items than the full index.
    expect(evidenceSection.textContent).not.toContain('Estratto numero 20');
    const toggle = container.querySelector('[data-testid="cv2-customer-evidence-toggle"]');
    expect(toggle.textContent).toContain('Mostra tutte le evidenze (20)');

    await click('[data-testid="cv2-customer-evidence-toggle"]');
    expect(container.querySelector('[data-testid="cv2-customer-evidence"]').textContent).toContain('Estratto numero 20');
  });

  test('evidence without an excerpt shows a graceful fallback line', async () => {
    getCorrectnessV2CustomerView.mockResolvedValue({
      data: {
        available: true,
        report: {
          ...sanitizedReport,
          customer_evidence_index: [{ page: 7, topic: 'Documentazione', perizia_excerpt: null }],
        },
      },
    });
    await render();
    expect(container.querySelector('[data-testid="cv2-customer-evidence"]').textContent)
      .toContain('Estratto non disponibile automaticamente');
  });

  test('identity facts render once: duplicated key facts are suppressed', async () => {
    getCorrectnessV2CustomerView.mockResolvedValue({
      data: {
        available: true,
        report: {
          ...sanitizedReport,
          key_facts: [
            { label: 'Tribunale', value: 'Tribunale di Torino' },
            { label: 'Valore di vendita giudiziaria', value_display: 'EUR 38.110,20' },
            { label: 'Superficie commerciale', value_display: '95 mq' },
          ],
        },
      },
    });
    await render();
    const body = text();
    // The duplicated identity fact appears exactly once (identity grid only).
    expect(body.split('Tribunale di Torino').length - 1).toBe(1);
    // The money amount renders in the money section, not duplicated as a key fact card.
    const summary = container.querySelector('[data-testid="cv2-customer-summary"]');
    expect(summary.textContent).not.toContain('EUR 38.110,20');
    // A genuinely new fact is kept.
    expect(summary.textContent).toContain('95 mq');
  });

  test('compliance cards show a normalized status badge and support excerpt when available', async () => {
    getCorrectnessV2CustomerView.mockResolvedValue({
      data: {
        available: true,
        report: {
          ...sanitizedReport,
          compliance_section: [
            { area: 'Conformità urbanistica', classification: 'conforming', status_label: 'conforme secondo la perizia', evidence_pages: [8] },
            { area: 'Regolarità edilizia', classification: 'regularizable', cost_display: '€ 20.000,00', evidence_pages: [34] },
          ],
          customer_evidence_index: [
            { page: 8, topic: 'conformità urbanistica', report_section: 'Conformità e documenti tecnici', perizia_excerpt: 'immobile conforme al PRGC' },
          ],
        },
      },
    });
    await render();
    const compliance = container.querySelector('[data-testid="cv2-customer-compliance"]');
    expect(compliance.textContent).toContain('conforme secondo la perizia');
    expect(compliance.textContent).toContain('Regolarizzabile');
    expect(compliance.textContent).toContain('€ 20.000,00');
    // Verbatim support line pulled from the evidence index by page+topic match.
    expect(compliance.textContent).toContain('immobile conforme al PRGC');
  });

  test('checklist is capped with the rest behind a collapse', async () => {
    const checklist = Array.from({ length: 12 }, (_, i) => ({
      action: 'Verificare',
      detail: `punto numero ${i + 1}`,
    }));
    getCorrectnessV2CustomerView.mockResolvedValue({
      data: { available: true, report: { ...sanitizedReport, buyer_checklist: checklist } },
    });
    await render();
    const section = container.querySelector('[data-testid="cv2-customer-checklist"]');
    expect(section.textContent).toContain('punto numero 8');
    expect(section.textContent).toContain('Altre verifiche (4)');
  });

  test('sections with no data are omitted instead of rendering empty clutter', async () => {
    getCorrectnessV2CustomerView.mockResolvedValue({
      data: {
        available: true,
        report: {
          ...sanitizedReport,
          occupancy_section: {},
          formalities_section: [],
          money_sections: { valuation_chain: [], auction_terms: [], buyer_side_costs: [], procedure_cancelled_formalities: [], uncertain_money: [] },
          compliance_section: [],
          buyer_checklist: [],
          customer_evidence_index: [],
        },
      },
    });
    await render();
    expect(container.querySelector('[data-testid="cv2-customer-occupancy"]')).toBeNull();
    expect(container.querySelector('[data-testid="cv2-customer-money"]')).toBeNull();
    expect(container.querySelector('[data-testid="cv2-customer-costs"]')).toBeNull();
    expect(container.querySelector('[data-testid="cv2-customer-formalities"]')).toBeNull();
    expect(container.querySelector('[data-testid="cv2-customer-compliance"]')).toBeNull();
    expect(container.querySelector('[data-testid="cv2-customer-checklist"]')).toBeNull();
    expect(container.querySelector('[data-testid="cv2-customer-evidence"]')).toBeNull();
  });

  test('money confirmation: shows the amount, excerpt + page and candidate options', async () => {
    getCorrectnessV2CustomerView.mockResolvedValue({ data: { available: true, report: moneyConfirmationReport } });
    await render();
    const card = container.querySelector('[data-testid="cv2-customer-money-confirmation"]');
    expect(card).not.toBeNull();
    expect(card.textContent).toContain('EUR 452.494,00');
    expect(card.textContent).toContain('pag. 12');
    expect(card.textContent).toContain('Valore di vendita giudiziaria: EUR 452.494,00');
    expect(container.querySelector('[data-testid="cv2-customer-money-option-market_value"]')).not.toBeNull();
    expect(container.querySelector('[data-testid="cv2-customer-money-option-judicial_sale_value"]')).not.toBeNull();
    // Submit is disabled until an option is chosen.
    expect(container.querySelector('[data-testid="cv2-customer-money-submit"]').disabled).toBe(true);
    // The closest-guess report body is still shown beneath the prompt.
    expect(container.querySelector('[data-testid="cv2-customer-report"]')).not.toBeNull();
  });

  test('DOCUMENT_NOT_READABLE renders the "PDF leggibile" guidance, not the normal report body', async () => {
    getCorrectnessV2CustomerView.mockResolvedValue({ data: { available: true, report: documentNotReadableReport } });
    await render();
    const card = container.querySelector('[data-testid="cv2-customer-not-readable"]');
    expect(card).not.toBeNull();
    expect(card.textContent).toContain("Per completare l'analisi ci serve un PDF leggibile");
    expect(card.textContent).toContain('Come ottenere un PDF leggibile');
    // Obvious re-upload call-to-action.
    expect(container.querySelector('[data-testid="cv2-customer-not-readable-cta"]')).not.toBeNull();
    // The dedicated branch takes precedence: the normal report body and the
    // lot/money branches never render for a non-readable document.
    expect(container.querySelector('[data-testid="cv2-customer-report"]')).toBeNull();
    expect(container.querySelector('[data-testid="cv2-customer-lot-selector"]')).toBeNull();
    // Informational sky tone, NOT the amber "da verificare" tone.
    const decision = container.querySelector('[data-testid="cv2-customer-decision"]');
    expect(decision.className).toContain('sky');
    expect(decision.className).not.toContain('amber');
  });

  test('money confirmation: choosing an option and submitting finalizes the report', async () => {
    getCorrectnessV2CustomerView.mockResolvedValue({ data: { available: true, report: moneyConfirmationReport } });
    submitCorrectnessV2MoneyConfirmation.mockResolvedValue({ data: { available: true, report: sanitizedReport } });
    await render();
    await click('[data-testid="cv2-customer-money-option-market_value"] input');
    expect(container.querySelector('[data-testid="cv2-customer-money-submit"]').disabled).toBe(false);
    await click('[data-testid="cv2-customer-money-submit"]');
    expect(submitCorrectnessV2MoneyConfirmation).toHaveBeenCalledWith(
      'analysis_generic', 'cv2_money', { 'p12:valore_vendita:3': 'market_value' },
    );
    // The finalized report replaces the confirmation prompt.
    expect(container.querySelector('[data-testid="cv2-customer-money-confirmation"]')).toBeNull();
    expect(text()).toContain('Appartamento in Torino');
  });

  test('DOCUMENT_NOT_READABLE: shows the "PDF leggibile" guidance and not the normal report body', async () => {
    getCorrectnessV2CustomerView.mockResolvedValue({ data: { available: true, report: notReadableReport } });
    await render();

    const notReadable = container.querySelector('[data-testid="cv2-customer-not-readable"]');
    expect(notReadable).not.toBeNull();
    expect(notReadable.textContent).toContain('Per completare l\'analisi ci serve un PDF leggibile');
    expect(container.querySelector('[data-testid="cv2-customer-not-readable-cta"]').textContent)
      .toContain('Carica di nuovo la perizia');
    // Precedence: the standard report body and any money amount must not leak through.
    expect(container.querySelector('[data-testid="cv2-customer-report"]')).toBeNull();
    expect(text()).not.toContain('EUR 999.999,00');
    // Informational tone, not the amber "da verificare" fallback.
    expect(text()).not.toContain('Da verificare');
  });

  test('preparing state sets a reassuring processing-time expectation', async () => {
    getCorrectnessV2CustomerView.mockResolvedValue({
      data: { available: false, preparing: true, reason_code: 'PREPARING' },
    });
    await render();
    expect(container.querySelector('[data-testid="cv2-customer-preparing"]')).toBeTruthy();
    expect(text()).toContain('1–2 minuti');
  });
});
