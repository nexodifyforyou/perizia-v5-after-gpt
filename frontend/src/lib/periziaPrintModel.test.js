import { buildPeriziaPrintReportModel, splitQuotaFromDiritto } from './periziaPrintModel';

describe('periziaPrintModel', () => {
  test('splitQuotaFromDiritto separates compact diritto/quota strings', () => {
    expect(splitQuotaFromDiritto('Proprietà1/1', '1/1')).toBe('Proprietà');
  });

  test('print flags do not reintroduce background-note servitu when a stronger blocker exists', () => {
    const model = buildPeriziaPrintReportModel({
      result: {
        section_9_legal_killers: {
          top_items: [
            {
              killer: 'Formalità da cancellare',
              status: 'GIALLO',
              status_it: 'ATTENZIONE',
              reason_it: 'Formalità da cancellare',
              evidence: [{ page: 14, quote: 'Formalità da cancellare con il decreto di trasferimento', search_hint: 'formalità da cancellare' }],
              decision_score: 72,
            },
            {
              killer: 'Servitù rilevata',
              status: 'GIALLO',
              status_it: 'ATTENZIONE',
              reason_it: 'Servitù rilevata',
              evidence: [{ page: 10, quote: 'eventuali vincoli e servitù passive o attive', search_hint: 'servitù passive o attive' }],
              decision_score: 70,
            },
          ],
        },
        panoramica_contract: {},
      },
    });

    expect(model.flags.some((item) => item.title === 'Formalità da cancellare')).toBe(true);
    expect(model.flags.some((item) => item.title === 'Servitù rilevata')).toBe(false);
  });

  test('print flags prefer cleaned customer contract fields over generic legacy text', () => {
    const model = buildPeriziaPrintReportModel({
      result: {
        section_11_red_flags: [
          {
            headline_it: 'Occupazione da verificare',
            flag_it: 'Titolo occupazione',
            action_it: 'Verificare titolo, data, registrazione e opponibilità.',
            explanation_it: 'Testo pulito da non usare come azione se action_it esiste.',
            verify_next_it: 'Fallback successivo.',
            explanation: 'Internal explanation should not win.',
            detail: 'Legacy detail should not win.',
            reason_it: 'Legacy reason should not win.',
            severity: 'AMBER',
          },
        ],
        panoramica_contract: {},
      },
    });

    expect(model.flags).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          title: 'Occupazione da verificare',
          detail: 'Verificare titolo, data, registrazione e opponibilità.',
        }),
      ])
    );
  });

  test('print legal items prefer explanation_it over legacy reason/action fields', () => {
    const model = buildPeriziaPrintReportModel({
      result: {
        section_9_legal_killers: {
          top_items: [
            {
              headline_it: 'Agibilità assente',
              killer: 'Titolo legacy',
              status: 'GIALLO',
              status_it: 'ATTENZIONE',
              explanation_it: 'La perizia non mostra un certificato di agibilità conclusivo.',
              verify_next_it: 'Verificare certificato e allegati edilizi richiamati.',
              reason_it: 'Legacy internal reason',
              action: 'Legacy action',
              evidence: [{ page: 9, quote: 'non risulta prodotto certificato di agibilità', search_hint: 'agibilità' }],
            },
          ],
        },
        panoramica_contract: {},
      },
    });

    expect(model.legal).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          title: 'Agibilità assente',
          detail: 'La perizia non mostra un certificato di agibilità conclusivo.',
        }),
      ])
    );
  });

  test('print cover summary prefers summary_for_client_bundle over dirty legacy summaries', () => {
    const model = buildPeriziaPrintReportModel({
      result: {
        summary_for_client_bundle: {
          decision_summary_it: 'Clean bundle summary',
        },
        summary_for_client: {
          summary_it: 'INTERNAL DIRTY explanation',
        },
        decision_rapida_client: {
          summary_it: 'INTERNAL DIRTY action',
        },
        semaforo_generale: {
          status: 'AMBER',
          status_it: 'ATTENZIONE',
          reason_it: 'INTERNAL DIRTY summary',
        },
        panoramica_contract: {},
      },
    });

    expect(model.cover.summaryIt).toBe('Clean bundle summary');
    expect(model.overview.decisionIt).toBe('Clean bundle summary');
  });
});
