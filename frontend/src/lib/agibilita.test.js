import { resolveAgibilitaDetail } from './agibilita';

describe('resolveAgibilitaDetail', () => {
  test('positive-only evidence resolves to PRESENTE', () => {
    const detail = resolveAgibilitaDetail({
      candidateValues: ['PRESENTE'],
      evidenceSources: [
        [{ page: 9, quote: "L'immobile risulta agibile.", search_hint: 'immobile risulta agibile' }],
      ],
    });

    expect(detail.value).toBe('PRESENTE');
    expect(detail.note).toBe('');
  });

  test('absent-only evidence resolves to Assente', () => {
    const detail = resolveAgibilitaDetail({
      candidateValues: ['ASSENTE'],
      evidenceSources: [
        [{ page: 4, quote: "Non risulta agibile.", search_hint: 'non risulta agibile' }],
      ],
    });

    expect(detail.value).toBe('Assente');
    expect(detail.note).toBe('');
  });

  test('true positive/negative conflict resolves to Da verificare', () => {
    const detail = resolveAgibilitaDetail({
      candidateValues: ['PRESENTE'],
      evidenceSources: [
        [
          { page: 9, quote: "L'immobile risulta agibile.", search_hint: 'immobile risulta agibile' },
          { page: 10, quote: "Non risulta agibile.", search_hint: 'non risulta agibile' },
        ],
      ],
    });

    expect(detail.value).toBe('Da verificare');
    expect(detail.note).toContain('Indicazioni contrastanti');
  });
});
