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
});
