import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import CustomerDecisionReport from './CustomerDecisionReport';

function mount(ui) {
  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);
  act(() => { root.render(ui); });
  return {
    container,
    unmount: () => act(() => { root.unmount(); }),
  };
}

const q = (c, sel) => c.querySelector(sel);
const qa = (c, sel) => Array.from(c.querySelectorAll(sel));

function model(overrides = {}) {
  return {
    schema_version: 'cv2.customer_decision.v1',
    esito: {
      level: 'ambra',
      headline: 'Verifiche necessarie prima di procedere',
      sentence: 'Alcuni aspetti da controllare.',
      drivers: [
        { finding_id: 'occ-1', title: 'Stato di occupazione', section: 'occupazione' },
        { finding_id: 'cmp-1', title: 'Edilizia', section: 'conformita' },
      ],
    },
    readiness: { state: 'CONFIRMATIONS_REQUIRED', label: 'Conferme necessarie' },
    sections: {
      acquisto: {
        identity: { tribunale: 'Tribunale X', indirizzo: 'Via 1', tipologia: 'appartamento', pagine: [1] },
        beni: [{ titolo: 'Bene principale', pertinenze: [{ label: 'soffitta' }], pagine: [1] }],
        occupazione_sintesi: 'Occupato',
      },
      numeri: {
        catena: [
          { label: 'Valore di mercato', amount: 43654.2, amount_display: '€ 43.654,20', kind: 'value' },
          { label: 'Costi di regolarizzazione', amount: 5250, amount_display: '€ 5.250,00', kind: 'deduction' },
          { label: 'Valore di vendita', amount: 38110.2, amount_display: '€ 38.110,20', kind: 'value', terminal: true },
        ],
        costi_potenziali: [
          { label: 'Costo cancellazione', amount_display: '€ 294,00', included_in_valuation: true, nota: 'Già considerato nel valore finale: non sommare nuovamente.' },
        ],
        comparatives_summary: { count: 3, pages: [5, 6] },
      },
      occupazione: {
        stato: 'Occupato', dettaglio: 'occupato con contratto', perche_conta: 'Incide sui tempi.',
        cosa_verificare: ['Opponibilità del titolo da verificare'], pagina: 3, pagine: [3],
      },
      verifiche: {
        items: [{ title: 'Verificare opponibilità', why: 'x', status: 'da_verificare', page: 3, link: 'occupazione' }],
        total: 1,
      },
      conformita: {
        groups: [{ group: 'Edilizia', items: ['cmp-1'] }, { group: 'Urbanistica', items: ['cmp-2'] }],
      },
      formalita: {
        cancellate: [{ type_label: 'Ipoteca', statement: 'Formalità indicata come cancellata a cura della procedura.', note: 'nota', amount_display: '€ 150.000,00', details: ['a', 'b'], pages: [3] }],
      },
      altri: { items: [{ title: 'Soffitta', summary: 'non ispezionata', pages: [9] }] },
      fonti: {
        primary: [
          { source_id: 'src-1', page: 18, title: 'Catena di valutazione', excerpt: 'valore di vendita 38110' },
          { source_id: 'src-2', page: 8, title: 'Conformità', excerpt: null },
        ],
        all_count: 20,
      },
      stato_verifiche: { label: 'Conferme necessarie', confirmations_total: 2, confirmations_done: 1, professional_checks_open: 1 },
    },
    findings: [
      { finding_id: 'cmp-1', section: 'conformita', title: 'Edilizia', status: 'regolarizzabile',
        status_label: 'Regolarizzabile secondo la perizia', customer_summary: 'difformità',
        amount_display: '€ 2.500,00', timing: '6 mesi', page: 7, pages: [7],
        evidence: { page: 7, excerpt: 'difformità edilizia' } },
      { finding_id: 'cmp-2', section: 'conformita', title: 'Urbanistica', status: 'conforme',
        status_label: 'Conforme secondo la perizia', customer_summary: 'Nessuna difformità.', page: 8, pages: [8] },
      { finding_id: 'occ-1', section: 'occupazione', title: 'Stato di occupazione', status: 'da_verificare',
        page: 3, evidence: { page: 3, excerpt: 'occupato' },
        confirm_class: 'occupancy',
        confirmation: { eligible: true, question: 'Secondo la pagina 3, l\'immobile risulta:',
          options: [{ option_id: 'occupato_opponibile', label: 'Occupato con contratto opponibile' },
                    { option_id: 'libero', label: 'L\'immobile è libero' }],
          unsure_option: { option_id: 'non_sicuro', label: 'Non sono sicuro' } } },
    ],
    confirmations: [],
    ...overrides,
  };
}

const report = (m) => ({ decision_model: m });

// 1. section order matches §Part 3
test('renders sections in the canonical order', () => {
  const { container, unmount } = mount(<CustomerDecisionReport report={report(model())} />);
  const order = ['cv2-esito', 'cv2-acquisto', 'cv2-numeri', 'cv2-occupazione', 'cv2-verifiche',
    'cv2-conformita', 'cv2-formalita', 'cv2-altri', 'cv2-fonti', 'cv2-stato-verifiche'];
  const seen = qa(container, '[data-testid]')
    .map((el) => el.getAttribute('data-testid'))
    .filter((id) => order.includes(id));
  const dedup = order.filter((id) => seen.includes(id));
  // relative order preserved
  const positions = dedup.map((id) => seen.indexOf(id));
  expect(positions).toEqual([...positions].sort((a, b) => a - b));
  unmount();
});

// 2. empty sections omitted (no "0 beni" cards)
test('omits sections absent from the payload', () => {
  const m = model();
  delete m.sections.formalita;
  delete m.sections.altri;
  const { container, unmount } = mount(<CustomerDecisionReport report={report(m)} />);
  expect(q(container, '[data-testid="cv2-formalita"]')).toBeNull();
  expect(q(container, '[data-testid="cv2-altri"]')).toBeNull();
  unmount();
});

// 3. esito drivers <=5 and no generic counts anywhere
test('esito shows headline + drivers, no numeric counts', () => {
  const { container, unmount } = mount(<CustomerDecisionReport report={report(model())} />);
  const esito = q(container, '[data-testid="cv2-esito"]');
  expect(esito.textContent).toContain('Verifiche necessarie prima di procedere');
  const drivers = qa(container, '[data-testid="cv2-esito-drivers"] li');
  expect(drivers.length).toBeLessThanOrEqual(5);
  expect(container.textContent).not.toMatch(/punti di attenzione|evidenze|aspetti non verificati/);
  unmount();
});

// 4. identity rendered once
test('identity rendered once', () => {
  const { container, unmount } = mount(<CustomerDecisionReport report={report(model())} />);
  const matches = (container.textContent.match(/Via 1/g) || []).length;
  expect(matches).toBe(1);
  unmount();
});

// 5. money chain rendered once with a single gold terminal
test('money chain rendered once with terminal', () => {
  const { container, unmount } = mount(<CustomerDecisionReport report={report(model())} />);
  const chains = qa(container, '[data-testid="cv2-catena"]');
  expect(chains.length).toBe(1);
  expect(chains[0].textContent).toContain('€ 38.110,20');
  unmount();
});

// 6. already-included buyer cost note
test('included buyer cost shows non-sommare note', () => {
  const { container, unmount } = mount(<CustomerDecisionReport report={report(model())} />);
  expect(q(container, '[data-testid="cv2-included-note"]').textContent).toContain('non sommare nuovamente');
  unmount();
});

// 7. occupancy practical: STATO/PERCHÉ/COSA/PAGINE
test('occupancy card is practical and readable', () => {
  const { container, unmount } = mount(<CustomerDecisionReport report={report(model())} />);
  const occ = q(container, '[data-testid="cv2-occupazione"]');
  expect(occ.textContent).toContain('Occupato');
  expect(occ.textContent).toContain('Perché conta');
  expect(occ.textContent).toContain('Opponibilità del titolo da verificare');
  expect(occ.textContent).toContain('p. 3');
  unmount();
});

// 8. conformity semantic colors: conforme green, regolarizzabile amber
test('conformity status chips use semantic tones', () => {
  const { container, unmount } = mount(<CustomerDecisionReport report={report(model())} />);
  const conf = q(container, '[data-testid="cv2-conformita"]');
  expect(conf.textContent).toContain('Conforme secondo la perizia');
  expect(conf.textContent).toContain('Regolarizzabile secondo la perizia');
  const greens = qa(conf, '.text-emerald-200');
  const ambers = qa(conf, '.text-amber-200');
  expect(greens.length).toBeGreaterThan(0);
  expect(ambers.length).toBeGreaterThan(0);
  unmount();
});

// 9. formality cancelled treatment clear + amount collapsed
test('formality shows cancelled treatment and collapses the amount', () => {
  const { container, unmount } = mount(<CustomerDecisionReport report={report(model())} />);
  const frm = q(container, '[data-testid="cv2-formalita"]');
  expect(frm.textContent).toContain('cancellata a cura della procedura');
  // registered amount lives inside a collapsed <details>
  const details = q(frm, 'details');
  expect(details).not.toBeNull();
  expect(details.textContent).toContain('€ 150.000,00');
  expect(frm.querySelector('.text-red-200')).toBeNull(); // never red
  unmount();
});

// 10. decisive evidence limited + "Mostra tutte le fonti"
test('fonti caps primary list and offers show-all', () => {
  const { container, unmount } = mount(<CustomerDecisionReport report={report(model())} />);
  const items = qa(container, '[data-testid="cv2-fonti"] li');
  expect(items.length).toBeLessThanOrEqual(8);
  const more = q(container, '[data-testid="cv2-fonti-more"]');
  expect(more.textContent).toContain('Mostra tutte le fonti (20)');
  unmount();
});

// 11. comparatives single collapsed line
test('comparatives shown as a single line, not risk cards', () => {
  const { container, unmount } = mount(<CustomerDecisionReport report={report(model())} />);
  const comp = q(container, '[data-testid="cv2-comparatives"]');
  expect(comp.textContent).toContain('Metodo di stima basato su comparativi');
  unmount();
});

// 12. eligible finding shows a Conferma necessaria entry point
test('eligible finding exposes a confirmation entry point', () => {
  const { container, unmount } = mount(<CustomerDecisionReport report={report(model())} />);
  expect(q(container, '[data-testid="cv2-confirm-open-occ-1"]')).not.toBeNull();
  unmount();
});

// 13. confirmed finding renders "Confermato dall'utente"
test('confirmed finding shows user-confirmed wording', () => {
  const m = model();
  m.sections.conferme = { items: [{ finding_id: 'occ-1', title: 'Stato di occupazione',
    selected_label: 'Occupato con contratto opponibile', page: 3, status: 'confermato_utente',
    stale: false, wording: "Confermato dall'utente sulla base della pagina 3." }] };
  const { container, unmount } = mount(<CustomerDecisionReport report={report(m)} />);
  const sec = q(container, '[data-testid="cv2-conferme"]');
  expect(sec.textContent).toContain("Confermato dall'utente");
  unmount();
});

// B1: chain-excluded ambiguous amounts are rendered (never silently dropped)
test('renders numeri.da_chiarire chain-excluded amounts', () => {
  const m = model();
  m.sections.numeri.da_chiarire = [
    { label: 'Deprezzamento 20%', amount_display: '€ 56.068,00', motivo: 'non determinato' },
  ];
  const { container, unmount } = mount(<CustomerDecisionReport report={report(m)} />);
  const dc = q(container, '[data-testid="cv2-da-chiarire"]');
  expect(dc).not.toBeNull();
  expect(dc.textContent).toContain('€ 56.068,00');
  expect(dc.textContent).toContain('Deprezzamento 20%');
  unmount();
});

// B1b: non-stale "Non sono sicuro" confirmation shows an amber chip, not green
test('conferme non_sicuro shows amber Non sono sicuro chip', () => {
  const m = model();
  m.sections.conferme = { items: [{ finding_id: 'occ-1', title: 'Stato di occupazione',
    selected_label: 'Non sono sicuro', page: 3, status: 'non_sicuro', stale: false,
    wording: 'Hai indicato «Non sono sicuro» in base alla pagina 3: la verifica resta aperta.' }] };
  const { container, unmount } = mount(<CustomerDecisionReport report={report(m)} />);
  const sec = q(container, '[data-testid="cv2-conferme"]');
  expect(sec.textContent).toContain('Non sono sicuro');
  expect(sec.textContent).not.toContain("Confermato dall'utente");
  expect(q(sec, '.text-emerald-200')).toBeNull(); // no green chip
  unmount();
});

// CK-fe1: the checklist reflects the reconciled status — a confirmed item shows
//         "Confermato dall'utente" (green), never amber "Da verificare".
test('checklist item never contradicts a confirmed finding', () => {
  const m = model();
  // reconciled backend output: the occupancy finding is confirmed everywhere
  m.findings = m.findings.map((f) =>
    f.finding_id === 'occ-1'
      ? { ...f, status: 'confermato_utente', status_label: "Confermato dall'utente", user_confirmed: true, confirmation: undefined }
      : f);
  m.sections.verifiche = {
    items: [{ title: 'Verificare opponibilità', why: 'x', status: 'confermato_utente',
      status_label: "Confermato dall'utente", page: 3, link: 'occupazione', finding_id: 'occ-1' }],
    total: 1, open_count: 0, completed_count: 1,
  };
  m.sections.conferme = { items: [{ finding_id: 'occ-1', title: 'Stato di occupazione',
    selected_label: 'Opponibile', page: 3, status: 'confermato_utente', stale: false,
    wording: "Confermato dall'utente sulla base della pagina 3." }] };
  const { container, unmount } = mount(<CustomerDecisionReport report={report(m)} />);
  const checklistItem = q(container, '[data-testid="cv2-verifiche"] [data-finding="occ-1"]');
  expect(checklistItem).not.toBeNull();
  expect(checklistItem.textContent).toContain("Confermato dall'utente");
  expect(checklistItem.textContent).not.toContain('Da verificare');
  // green (completed) chip, not amber
  expect(q(checklistItem, '.text-emerald-200')).not.toBeNull();
  expect(q(checklistItem, '.text-amber-200')).toBeNull();
  unmount();
});

// CK-fe2: open vs completed counts come from the reconciled checklist
test('checklist renders reconciled open/completed items distinctly', () => {
  const m = model();
  m.sections.verifiche = {
    items: [
      { title: 'A', status: 'confermato_utente', status_label: "Confermato dall'utente", finding_id: 'x1' },
      { title: 'B', status: 'da_verificare', status_label: 'Da verificare', finding_id: 'x2' },
    ],
    total: 2, open_count: 1, completed_count: 1,
  };
  const { container, unmount } = mount(<CustomerDecisionReport report={report(m)} />);
  const sec = q(container, '[data-testid="cv2-verifiche"]');
  expect(qa(sec, '.text-emerald-200').length).toBe(1); // one completed
  expect(qa(sec, '.text-amber-200').length).toBe(1);   // one open
  unmount();
});

// 14. no raw enums / internal codes / English risk headings in the DOM
test('no raw codes or English risk headings leak', () => {
  const { container, unmount } = mount(<CustomerDecisionReport report={report(model())} />);
  const text = container.textContent;
  for (const bad of ['CONFIRMATIONS_REQUIRED', 'regularizable', 'LOW_CONFIDENCE', 'Risk sections', 'manual_review']) {
    expect(text).not.toContain(bad);
  }
  unmount();
});
