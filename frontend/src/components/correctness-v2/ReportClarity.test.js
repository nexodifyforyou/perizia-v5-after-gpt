import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import CustomerDecisionReport, {
  SIX_STATE, isCriticalCheck,
} from './CustomerDecisionReport';

function mount(ui) {
  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);
  act(() => { root.render(ui); });
  return { container, unmount: () => act(() => { root.unmount(); }) };
}

const q = (c, sel) => c.querySelector(sel);
const qa = (c, sel) => Array.from(c.querySelectorAll(sel));

function clarityModel(overrides = {}) {
  return {
    schema_version: 'cv2.customer_decision.v1',
    clarity_enabled: true,
    esito: { level: 'rosso', headline: 'Verifica tecnica richiesta', sentence: 'Serve una verifica.', drivers: [] },
    readiness: { state: 'TECHNICAL_REVIEW_REQUIRED', label: 'Verifica tecnica richiesta' },
    sections: {
      acquisto: { identity: { indirizzo: 'Via 1', tipologia: 'appartamento', pagine: [1] } },
      numeri: { catena: [{ label: 'Valore di vendita', amount_display: '€ 38.110,20', terminal: true }] },
      verifiche: {
        items: [
          { finding_id: 'v-crit', title: 'Verifica bloccante', why: 'perché', status: 'da_verificare', page: 3, severity: 1 },
          { finding_id: 'v-sec', title: 'Verifica minore', why: 'perché2', status: 'da_verificare', page: 4, severity: 6 },
        ],
        total: 2,
      },
      conflitti: { items: ['cf-1', 'cf-2'] },
      conformita: { groups: [{ group: 'Edilizia', items: ['cmp-1'] }] },
      stato_verifiche: { label: 'Verifica tecnica richiesta', confirmations_total: 0, confirmations_done: 0, professional_checks_open: 1 },
    },
    findings: [
      { finding_id: 'cmp-1', section: 'conformita', title: 'Edilizia', status: 'non_dichiarato', status_label: 'Non dichiarato', customer_summary: 'non risulta dichiarato', page: 7 },
      { finding_id: 'cf-1', section: 'conflitti', title: 'Stato di occupazione', status: 'in_conflitto', status_label: 'In conflitto tra le fonti', customer_summary: 'Le fonti riportano valori discordanti.', page: null },
      { finding_id: 'cf-2', section: 'conflitti', title: 'Valore finale della perizia', status: 'non_determinabile', status_label: 'Non determinabile dalla sola perizia', customer_summary: 'Non determinabile.', page: null },
    ],
    confirmations: [],
    ...overrides,
  };
}

const report = (m) => ({ decision_model: m, job_id: 'j1', report_status: m.report_status || 'PARTIAL_REPORT_AVAILABLE' });

describe('report-clarity six-state table', () => {
  const SIX = ['dichiarato_perizia', 'confermato_utente', 'da_verificare', 'non_dichiarato', 'in_conflitto', 'non_determinabile'];

  test('each of the six states has a distinct label', () => {
    const labels = SIX.map((s) => SIX_STATE[s].label);
    expect(new Set(labels).size).toBe(SIX.length);
  });

  test('no two distinct states share BOTH the same label and the same icon', () => {
    for (let i = 0; i < SIX.length; i += 1) {
      for (let j = i + 1; j < SIX.length; j += 1) {
        const a = SIX_STATE[SIX[i]]; const b = SIX_STATE[SIX[j]];
        expect(a.label === b.label && a.icon === b.icon).toBe(false);
      }
    }
  });

  test('omission states are never rendered with a green (verde) tone', () => {
    for (const s of ['non_dichiarato', 'non_determinabile', 'in_conflitto']) {
      expect(SIX_STATE[s].tone).not.toBe('verde');
    }
  });
});

describe('isCriticalCheck partition', () => {
  test('blocking or severity<3 is critical; otherwise secondary; nothing dropped', () => {
    const items = [
      { severity: 0 }, { severity: 2 }, { severity: 3 }, { blocking: true, severity: 9 }, { severity: 6 },
    ];
    const crit = items.filter(isCriticalCheck);
    const sec = items.filter((it) => !isCriticalCheck(it));
    expect(crit.length + sec.length).toBe(items.length);
    expect(crit.length).toBe(3); // sev0, sev2, blocking
  });
});

describe('CustomerDecisionReport clarity rendering', () => {
  test('renders hero, conflitti section and critical/secondary split when clarity_enabled', () => {
    const { container, unmount } = mount(<CustomerDecisionReport report={report(clarityModel())} />);
    expect(q(container, '[data-testid="cv2-hero"]')).toBeTruthy();
    const conflitti = q(container, '[data-testid="cv2-conflitti"]');
    expect(conflitti).toBeTruthy();
    expect(conflitti.textContent).toContain('In conflitto tra le fonti');
    expect(conflitti.textContent).toContain('Non determinabile dalla sola perizia');
    // critical + secondary partition present and disjoint, summing to total
    const crit = qa(container, '[data-testid="cv2-verifiche-critical"] [data-finding]');
    const sec = qa(container, '[data-testid="cv2-verifiche-secondary"] [data-finding]');
    expect(crit.length + sec.length).toBe(2);
    expect(crit.length).toBe(1); // severity 1 → critical
    unmount();
  });

  test('conflitti finding count matches conflitti.items (P3 / invariant 6)', () => {
    const { container, unmount } = mount(<CustomerDecisionReport report={report(clarityModel())} />);
    const rows = qa(container, '[data-testid="cv2-conflitti"] [data-finding]');
    expect(rows.length).toBe(2);
    unmount();
  });

  test('no raw enum / internal token reaches rendered text', () => {
    const { container, unmount } = mount(<CustomerDecisionReport report={report(clarityModel())} />);
    const body = container.textContent || '';
    for (const raw of ['in_conflitto', 'non_determinabile', 'CONFLICT_REQUIRES_REVIEW', 'POSSIBLE_CROSS_LOT_LEAKAGE', 'TECHNICAL_REVIEW_REQUIRED']) {
      expect(body).not.toContain(raw);
    }
    unmount();
  });
});

describe('bilingual layer', () => {
  const translations = [
    { source: 'Verifica bloccante', en: 'Blocking check' },
    { source: 'Le fonti riportano valori discordanti.', en: 'The sources report conflicting values.' },
  ];

  test('English appears under Italian when clarity on + translations provided', () => {
    const { container, unmount } = mount(
      <CustomerDecisionReport report={report(clarityModel())} translations={translations} />,
    );
    const en = qa(container, '[data-testid="cv2-en"]').map((n) => n.textContent);
    // dynamic translation
    expect(en).toContain('Blocking check');
    // static-glossary translation of a section header (no dynamic entry needed)
    expect(en).toContain('What to verify before proceeding');
    unmount();
  });

  test('no English rendered when clarity is off (flag-off byte-for-byte)', () => {
    const m = clarityModel();
    delete m.clarity_enabled;
    const { container, unmount } = mount(
      <CustomerDecisionReport report={report(m)} translations={translations} />,
    );
    expect(qa(container, '[data-testid="cv2-en"]').length).toBe(0);
    expect(q(container, '[data-testid="cv2-hero"]')).toBeFalsy();
    expect(q(container, '[data-testid="cv2-conflitti"]')).toBeFalsy();
    unmount();
  });
});

describe('presentation-only conflict amber floor (Fable #3)', () => {
  const FLOOR_IT = 'Nessun blocco automatico rilevato, ma sono presenti informazioni in conflitto da verificare.';
  const FLOOR_EN = 'No automatic blocking issue was detected, but conflicting information requires verification.';
  const floorModel = () => clarityModel({
    report_status: 'REPORT_READY',
    esito: { level: 'verde', headline: 'Nessun elemento bloccante', sentence: 'Nulla di bloccante.',
             clarity_conflict_floor: true, clarity_summary_it: FLOOR_IT, drivers: [] },
  });

  test('green summary is replaced by the qualified amber message; esito.level stays verde in data', () => {
    const m = floorModel();
    const { container, unmount } = mount(<CustomerDecisionReport report={report(m)} />);
    const text = container.textContent;
    expect(text).toContain(FLOOR_IT);                 // qualified message shown
    expect(text).not.toContain('Nessun elemento bloccante'); // no unconditional green message
    expect(text).toContain('Verifiche necessarie');   // displayed tone clamped to amber
    expect(m.esito.level).toBe('verde');              // data never mutated (no re-derivation)
    unmount();
  });

  test('English renders directly under Italian for the floor message', () => {
    const { container, unmount } = mount(
      <CustomerDecisionReport report={report(floorModel())} translations={{ [FLOOR_IT]: FLOOR_EN }} />,
    );
    expect(container.textContent).toContain(FLOOR_IT);  // Italian first/primary
    expect(container.textContent).toContain(FLOOR_EN);  // English underneath
    unmount();
  });
});
