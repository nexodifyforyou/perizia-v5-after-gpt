import fs from 'fs';
import { buildPeriziaPrintReportModel, normalizeAnalysisResponse } from './periziaPrintModel';

const payload = normalizeAnalysisResponse(
  JSON.parse(fs.readFileSync('/tmp/periziascan_live_payloads/analysis_a7f41b222261.json', 'utf8'))
);

const flattenStrings = (node) => {
  if (typeof node === 'string') return [node];
  if (Array.isArray(node)) return node.flatMap(flattenStrings);
  if (node && typeof node === 'object') return Object.values(node).flatMap(flattenStrings);
  return [];
};

const BANNED = [
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

describe('periziaPrintModel live acceptance', () => {
  test('real multilot payload stays customer-safe in print model', () => {
    const model = buildPeriziaPrintReportModel(payload);
    const text = flattenStrings(model).join('\n').toLowerCase();

    expect(model.cover.summaryIt).toContain('Agibilità assente / non rilasciata.');
    expect(model.overview.decisionIt).toContain('Agibilità assente / non rilasciata.');
    expect(text).toContain('verifica titoli edilizi, agibilità/abitabilità e costi necessari per la regolarizzazione.');

    for (const phrase of BANNED) {
      expect(text).not.toContain(phrase.toLowerCase());
    }
  });
});
