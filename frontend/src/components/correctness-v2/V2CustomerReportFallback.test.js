import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import { V2CustomerReportFallback } from './CustomerReportView';
import CustomerDecisionReport from './CustomerDecisionReport';

// Guard: the fallback must be the sanitized Correctness V2 customer renderer,
// NOT the legacy analysis report — and it must never fetch or start a job.

// If any network client were reachable from the fallback, these would throw.
jest.mock('../../lib/api/perizia', () => ({
  __esModule: true,
  getCorrectnessV2CustomerView: jest.fn(() => { throw new Error('no network from fallback'); }),
  generateCorrectnessV2Lot: jest.fn(() => { throw new Error('no job from fallback'); }),
}));

const perizia = require('../../lib/api/perizia');

function mount(ui) {
  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);
  act(() => { root.render(ui); });
  return { container, unmount: () => act(() => root.unmount()) };
}

const sanitizedV2Report = {
  report_status: 'REPORT_READY',
  title: 'Report cliente',
  case_identity: { tribunale: 'Tribunale X', address: 'Via 1' },
  lot_structure: { selected_lot: '1' },
  money_sections: { valuation_chain: [{ label: 'Valore', amount_display: '€ 100,00', kind: 'value' }] },
  occupancy_section: { status_label: 'Occupato' },
  compliance_section: [], formalities_section: [], buyer_checklist: [],
  customer_evidence_index: [], risk_sections: [],
  // NOTE: no decision_model on purpose (old safe artifact)
};

// D-proof: fallback renders sanitized V2 content, no network, no job
test('fallback renders sanitized V2 content without network or job calls', () => {
  const { container, unmount } = mount(<V2CustomerReportFallback report={sanitizedV2Report} />);
  const text = container.textContent;
  // sanitized Correctness V2 customer content is present (identity + money chain)
  expect(text).toContain('Cosa stai comprando');
  expect(text).toContain('Numeri principali');
  expect(text).toContain('€ 100,00');
  // no legacy analysis-report headings
  for (const legacy of ['Panoramica', 'Red Flag', 'Dettagli tecnici', 'Vista admin', 'Analisi legacy']) {
    expect(text).not.toContain(legacy);
  }
  // no network / job side effects were triggered by rendering
  expect(perizia.getCorrectnessV2CustomerView).not.toHaveBeenCalled();
  expect(perizia.generateCorrectnessV2Lot).not.toHaveBeenCalled();
  unmount();
});

// the decision report path is a DIFFERENT component (fallback never overrides it)
test('fallback and decision report are distinct components', () => {
  expect(V2CustomerReportFallback).not.toBe(CustomerDecisionReport);
});
