import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import LotWorkspace, { buildLotSummaryLine, creditPreviewText } from './LotWorkspace';
import {
  generateCorrectnessV2Lot,
  getCorrectnessV2LotCreditPreview,
} from '../../lib/api/perizia';

jest.mock('../../lib/api/perizia', () => ({
  generateCorrectnessV2Lot: jest.fn(),
  getCorrectnessV2LotCreditPreview: jest.fn(),
}));

// Authoritative backend credit preview: rendered verbatim, never computed in JS.
const creditPreview = {
  can_start: true,
  will_consume_credit: false,
  credits_required: 0,
  available_credits: 12,
  already_paid_at_upload: true,
  exempt: false,
  reason: null,
};

const workspaceFixture = {
  analysis_id: 'analysis_ws',
  multi_lot: true,
  lot_count: 6,
  analysis_state: 'LOT_OVERVIEW',
  summary: {
    lot_count: 6,
    ready: 1,
    preparing: 1,
    confirmation_required: 1,
    verification_required: 1,
    failed: 1,
    not_analyzed: 1,
  },
  lots: [
    {
      lot_id: '1', label: 'Lotto 1', address: 'Via Uno 1', property_type: 'Appartamento',
      occupancy_summary: 'Libero', final_value: '€ 38.110,20', state: 'REPORT_READY',
      has_safe_report: true, job_running: false, last_attempt_failed: false,
      latest_report_at: '2026-07-10T10:00:00Z', report_version: 2,
      actions: ['open_report', 'rerun'],
    },
    {
      lot_id: '2', label: 'Lotto 2', address: 'Via Due 2', property_type: 'Magazzino',
      state: 'RUNNING', has_safe_report: false, job_running: true,
      last_attempt_failed: false, latest_report_at: null, report_version: null, actions: [],
    },
    {
      lot_id: '3', label: 'Lotto 3', state: 'MONEY_CONFIRMATION_REQUIRED',
      has_safe_report: true, job_running: false, last_attempt_failed: false,
      actions: ['open_report'],
    },
    {
      lot_id: '4', label: 'Lotto 4', state: 'VERIFICATION_REQUIRED',
      has_safe_report: false, job_running: false, last_attempt_failed: false,
      actions: ['open_report', 'rerun'],
    },
    {
      lot_id: '5', label: 'Lotto 5', state: 'FAILED',
      has_safe_report: false, job_running: false, last_attempt_failed: true,
      actions: ['rerun'],
    },
    {
      lot_id: '6', label: 'Lotto 6', state: 'NOT_ANALYZED',
      has_safe_report: false, job_running: false, last_attempt_failed: false,
      actions: ['generate'],
    },
  ],
  credit_preview: creditPreview,
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

const makeState = (workspace = workspaceFixture) => ({
  loading: false,
  resolved: true,
  available: true,
  workspace,
  preparing: false,
  reload: jest.fn(),
  refresh: jest.fn(() => Promise.resolve()),
});

const render = async (props = {}) => {
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
  await act(async () => {
    root.render(
      <LotWorkspace
        analysisId="analysis_ws"
        state={makeState()}
        onOpenLot={jest.fn()}
        {...props}
      />
    );
  });
  await flush();
};

const byTestId = (testId) => container.querySelector(`[data-testid="${testId}"]`);
const text = () => container.textContent || '';

const click = async (testId) => {
  const node = byTestId(testId);
  if (!node) throw new Error(`Missing ${testId}`);
  await act(async () => {
    node.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    await Promise.resolve();
  });
  await flush();
};

describe('LotWorkspace helpers', () => {
  test('buildLotSummaryLine omits zero categories', () => {
    expect(buildLotSummaryLine({
      lot_count: 6, ready: 4, preparing: 0, confirmation_required: 0,
      verification_required: 1, failed: 0, not_analyzed: 1,
    })).toBe('6 lotti · 4 pronti · 1 da verificare · 1 non analizzato');
    expect(buildLotSummaryLine({ lot_count: 1, ready: 1 })).toBe('1 lotto · 1 pronto');
    expect(buildLotSummaryLine(null)).toBe('');
    expect(buildLotSummaryLine({ lot_count: 0 })).toBe('');
  });

  test('creditPreviewText renders backend values verbatim (no client-side math)', () => {
    expect(creditPreviewText(creditPreview)).toBe("0 crediti · già incluso nell'analisi");
    expect(creditPreviewText({
      credits_required: 1, will_consume_credit: true,
      already_paid_at_upload: false, exempt: false,
    })).toBe('1 credito · verrà detratto dai crediti disponibili');
    expect(creditPreviewText(null)).toBe('');
  });
});

describe('LotWorkspace', () => {
  beforeEach(() => {
    globalThis.IS_REACT_ACT_ENVIRONMENT = true;
    document.body.innerHTML = '';
    jest.clearAllMocks();
    getCorrectnessV2LotCreditPreview.mockResolvedValue({ data: { ...creditPreview, lot_state: 'REPORT_READY', can_start: true } });
    generateCorrectnessV2Lot.mockResolvedValue({ data: { spawned: true, state: 'RUNNING', preparing: true, job_id: 'cv2_new' } });
  });

  afterEach(() => {
    if (root) act(() => root.unmount());
    if (container?.parentNode) container.parentNode.removeChild(container);
    container = null;
    root = null;
  });

  test('renders the summary line and one dominant action per lot state', async () => {
    await render();

    expect(byTestId('cv2-lot-workspace')).not.toBeNull();
    expect(byTestId('cv2-lot-summary').textContent).toContain(
      '6 lotti · 1 pronto · 1 in preparazione · 1 conferma richiesta · 1 da verificare · 1 non completato · 1 non analizzato'
    );

    // REPORT_READY -> primary "Apri report" (+ separate explicit rerun).
    expect(byTestId('cv2-lot-open-1').textContent).toContain('Apri report');
    expect(byTestId('cv2-lot-rerun-1').textContent).toContain('Rianalizza lotto');
    // RUNNING -> preparing indicator, no start button.
    expect(byTestId('cv2-lot-running-2').textContent).toContain('Report in preparazione');
    expect(byTestId('cv2-lot-generate-2')).toBeNull();
    // MONEY_CONFIRMATION_REQUIRED / VERIFICATION_REQUIRED -> open the verification.
    expect(byTestId('cv2-lot-open-3').textContent).toContain('Vedi verifica richiesta');
    expect(byTestId('cv2-lot-open-4').textContent).toContain('Vedi verifica richiesta');
    expect(byTestId('cv2-lot-rerun-4').textContent).toContain('Riprova analisi');
    // FAILED -> failure copy + explicit retry only.
    expect(byTestId('cv2-lot-card-5').textContent).toContain('Analisi non completata');
    expect(byTestId('cv2-lot-rerun-5').textContent).toContain('Riprova analisi');
    // NOT_ANALYZED -> explicit generate.
    expect(byTestId('cv2-lot-generate-6').textContent).toContain('Genera report lotto');

    // Rendering the overview NEVER starts anything.
    expect(generateCorrectnessV2Lot).not.toHaveBeenCalled();
    expect(getCorrectnessV2LotCreditPreview).not.toHaveBeenCalled();
  });

  test('"Apri report" opens the stored report: zero generate/preview calls', async () => {
    const onOpenLot = jest.fn();
    await render({ onOpenLot });

    await click('cv2-lot-open-1');

    expect(onOpenLot).toHaveBeenCalledWith('1');
    expect(generateCorrectnessV2Lot).not.toHaveBeenCalled();
    expect(getCorrectnessV2LotCreditPreview).not.toHaveBeenCalled();
  });

  test('rerun opens the confirmation modal with the backend credit preview verbatim', async () => {
    await render();

    await click('cv2-lot-rerun-1');

    expect(byTestId('cv2-lot-generate-modal')).not.toBeNull();
    expect(text()).toContain('Rianalizzare il lotto?');
    expect(text()).toContain('Lotto 1');
    // Current report date + version.
    expect(text()).toContain('versione 2');
    // Backend preview rendered verbatim.
    expect(getCorrectnessV2LotCreditPreview).toHaveBeenCalledWith('analysis_ws', '1');
    expect(byTestId('cv2-lot-credit-preview').textContent).toContain("0 crediti · già incluso nell'analisi");
    expect(byTestId('cv2-lot-credit-preview').textContent).toContain('Crediti disponibili: 12');
    // Nothing started yet.
    expect(generateCorrectnessV2Lot).not.toHaveBeenCalled();

    // Annulla closes without any job.
    await click('cv2-lot-generate-cancel');
    expect(byTestId('cv2-lot-generate-modal')).toBeNull();
    expect(generateCorrectnessV2Lot).not.toHaveBeenCalled();
  });

  test('confirming a rerun calls generate with force=true; double-click cannot double-submit', async () => {
    let resolveGenerate;
    generateCorrectnessV2Lot.mockImplementation(() => new Promise((resolve) => {
      resolveGenerate = resolve;
    }));
    await render();

    await click('cv2-lot-rerun-1');
    await click('cv2-lot-generate-confirm');
    // Second click while the first request is in flight: must be a no-op.
    await click('cv2-lot-generate-confirm');

    expect(generateCorrectnessV2Lot).toHaveBeenCalledTimes(1);
    expect(generateCorrectnessV2Lot).toHaveBeenCalledWith('analysis_ws', '1', true);
    expect(byTestId('cv2-lot-generate-confirm').disabled).toBe(true);

    await act(async () => {
      resolveGenerate({ data: { spawned: true, state: 'RUNNING', preparing: true } });
      await Promise.resolve();
    });
    await flush();
    // Modal closed, lot flips to the running indicator while the workspace refreshes.
    expect(byTestId('cv2-lot-generate-modal')).toBeNull();
    expect(generateCorrectnessV2Lot).toHaveBeenCalledTimes(1);
  });

  test('FAILED lot: no silent rerun — retry requires the explicit confirmation', async () => {
    await render();

    // Nothing fired on render or on merely looking at the failed card.
    expect(generateCorrectnessV2Lot).not.toHaveBeenCalled();

    await click('cv2-lot-rerun-5');
    expect(byTestId('cv2-lot-generate-modal')).not.toBeNull();
    expect(generateCorrectnessV2Lot).not.toHaveBeenCalled();

    await click('cv2-lot-generate-confirm');
    expect(generateCorrectnessV2Lot).toHaveBeenCalledTimes(1);
    expect(generateCorrectnessV2Lot).toHaveBeenCalledWith('analysis_ws', '5', true);
  });

  test('NOT_ANALYZED: explicit "Genera report lotto" confirms then calls generate with force=false', async () => {
    await render();

    await click('cv2-lot-generate-6');
    expect(text()).toContain('Generare il report del lotto?');
    expect(byTestId('cv2-lot-credit-preview').textContent).toContain("0 crediti · già incluso nell'analisi");
    expect(generateCorrectnessV2Lot).not.toHaveBeenCalled();

    await click('cv2-lot-generate-confirm');
    expect(generateCorrectnessV2Lot).toHaveBeenCalledWith('analysis_ws', '6', false);
  });

  test('prior safe report survives a failed rerun: notice + open-safe-report action', async () => {
    const onOpenLot = jest.fn();
    const workspace = {
      ...workspaceFixture,
      lots: [{
        lot_id: '1', label: 'Lotto 1', state: 'FAILED',
        has_safe_report: true, job_running: false, last_attempt_failed: true,
        latest_report_at: '2026-07-10T10:00:00Z', report_version: 2,
        actions: ['open_report', 'rerun'],
      }],
    };
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
    await act(async () => {
      root.render(
        <LotWorkspace analysisId="analysis_ws" state={makeState(workspace)} onOpenLot={onOpenLot} />
      );
    });
    await flush();

    expect(text()).toContain("L'ultimo tentativo non è stato completato.");
    expect(byTestId('cv2-lot-open-safe-1').textContent).toContain('Ultimo report verificato disponibile');

    await click('cv2-lot-open-safe-1');
    expect(onOpenLot).toHaveBeenCalledWith('1');
    expect(generateCorrectnessV2Lot).not.toHaveBeenCalled();
  });

  test('a 409 LOT_FAILED_RERUN_REQUIRED shows the explicit-rerun message, no silent retry', async () => {
    generateCorrectnessV2Lot.mockRejectedValue({
      response: { status: 409, data: { detail: { reason_code: 'LOT_FAILED_RERUN_REQUIRED' } } },
    });
    await render();

    await click('cv2-lot-generate-6');
    await click('cv2-lot-generate-confirm');

    expect(generateCorrectnessV2Lot).toHaveBeenCalledTimes(1);
    expect(byTestId('cv2-lot-generate-modal')).not.toBeNull();
    expect(text()).toContain('rianalisi esplicita');
  });
});
