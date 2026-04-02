import { buildCustomerCostPolicy } from './costPolicy';

describe('buildCustomerCostPolicy', () => {
  test('keeps deprezzamenti separate and only accepts explicit buyer costs with amount + evidence + burden wording', () => {
    const policy = buildCustomerCostPolicy({
      panoramica_contract: {
        valuation_waterfall: {
          deprezzamenti_eur: 12000,
          evidence: { deprezzamenti_eur: [{ page: 3, quote: 'Deprezzamento pari a euro 12.000', search_hint: 'deprezzamento' }] },
        },
      },
      money_box: {
        items: [
          {
            label_it: 'Sanatoria urbanistica',
            stima_euro: '€ 5.000',
            stima_nota: 'Spesa per sanatoria urbanistica € 5.000',
            evidence: [{ page: 5, quote: 'Spesa per sanatoria urbanistica € 5.000', search_hint: 'sanatoria urbanistica' }],
          },
          {
            label_it: 'Valore finale di stima',
            stima_euro: '€ 90.000',
            stima_nota: 'Valore finale di stima € 90.000',
            evidence: [{ page: 8, quote: 'Valore finale di stima € 90.000', search_hint: 'valore finale di stima' }],
          },
        ],
      },
    });

    expect(policy.valuationAdjustments.amount).toBe('€ 12.000');
    expect(policy.explicitBuyerCosts).toHaveLength(1);
    expect(policy.explicitBuyerCosts[0].__policy_label).toBe('Sanatoria urbanistica');
    expect(policy.totalSummary.kind).toBe('explicit_total');
    expect(policy.totalSummary.text).toBe('€ 5.000');
  });

  test('requires evidence plus buyer-burden wording for grounded non-quantified burdens', () => {
    const policy = buildCustomerCostPolicy({
      money_box: {
        qualitative_burdens: [
          {
            label_it: 'Allineamento catastale da verificare',
            stima_nota: 'Regolarizzazione catastale da verificare',
            evidence: [{ page: 6, quote: 'Necessario allineamento catastale', search_hint: 'allineamento catastale' }],
          },
          {
            label_it: 'Generico richiamo non supportato',
            stima_nota: 'Da verificare',
            evidence: [],
          },
        ],
      },
    });

    expect(policy.explicitBuyerCosts).toHaveLength(0);
    expect(policy.groundedUnquantifiedBurdens).toHaveLength(1);
    expect(policy.groundedUnquantifiedBurdens[0].label).toBe('Allineamento catastale da verificare');
    expect(policy.totalSummary.kind).toBe('non_quantified');
  });

  test('positive truths and legal-only context do not become costs and return conservative summary', () => {
    const policy = buildCustomerCostPolicy({
      money_box: {
        items: [
          {
            label_it: 'Urbanistica conforme',
            stima_euro: '€ 2.000',
            stima_nota: 'Immobile conforme',
            evidence: [{ page: 2, quote: 'Urbanistica conforme', search_hint: 'conforme' }],
          },
          {
            label_it: 'Formalità da cancellare',
            stima_euro: '€ 3.000',
            stima_nota: 'Formalità da cancellare con decreto di trasferimento',
            evidence: [{ page: 14, quote: 'Formalità da cancellare con decreto di trasferimento', search_hint: 'formalità da cancellare' }],
          },
        ],
        qualitative_burdens: [
          {
            label_it: 'Libero',
            stima_nota: 'Immobile libero',
            evidence: [{ page: 7, quote: 'Immobile libero', search_hint: 'libero' }],
          },
        ],
      },
    });

    expect(policy.explicitBuyerCosts).toHaveLength(0);
    expect(policy.groundedUnquantifiedBurdens).toHaveLength(0);
    expect(policy.totalSummary.kind).toBe('none');
    expect(policy.totalSummary.text).toBe('Nessun costo extra lato acquirente difendibile rilevato nella perizia.');
  });
});
