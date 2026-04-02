import {
  buildCanonicalLegalPriorityMeta,
  getCanonicalTopAttentionItems,
  isPositiveOrNeutralLegalTruth,
  pickCanonicalTopAttentionItem,
} from './legalPriority';

describe('legalPriority', () => {
  test('material blocker outranks background-note servitu', () => {
    const items = [
      { title: 'Servitù rilevata', kind: 'background_note', decisionScore: 70 },
      { title: 'Formalità da cancellare', kind: 'material_blocker', decisionScore: 72 },
    ];

    expect(pickCanonicalTopAttentionItem(items)?.title).toBe('Formalità da cancellare');
    expect(getCanonicalTopAttentionItems(items).map((item) => item.title)).toEqual(['Formalità da cancellare']);
  });

  test('positive truths stay neutral and do not become top attention', () => {
    expect(isPositiveOrNeutralLegalTruth('regolarita_urbanistica', 'NON EMERGONO ABUSI')).toBe(true);
    expect(
      buildCanonicalLegalPriorityMeta({
        key: 'regolarita_urbanistica',
        title: 'Urbanistica',
        detail: 'NON EMERGONO ABUSI',
      }).kind
    ).toBe('neutral_truth');
  });

  test('canonical top attention selection is stable among top-rank items', () => {
    const items = [
      { title: 'Difformità urbanistico-catastali', kind: 'material_blocker', decisionScore: 55 },
      { title: 'Formalità da cancellare', kind: 'material_blocker', decisionScore: 72 },
      { title: 'Occupazione', kind: 'material_blocker', decisionScore: 61 },
    ];

    expect(pickCanonicalTopAttentionItem(items)?.title).toBe('Formalità da cancellare');
  });
});
