import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import { useLotWorkspace } from './useLotWorkspace';
import { getCorrectnessV2Workspace } from '../../lib/api/perizia';

jest.mock('../../lib/api/perizia', () => ({
  getCorrectnessV2Workspace: jest.fn(),
}));

const runningWorkspace = {
  analysis_id: 'analysis_ws',
  analysis_state: 'LOT_OVERVIEW',
  summary: { lot_count: 2, ready: 1, preparing: 1 },
  lots: [
    { lot_id: '1', state: 'REPORT_READY', job_running: false },
    { lot_id: '2', state: 'RUNNING', job_running: true },
  ],
};

const readyWorkspace = {
  analysis_id: 'analysis_ws',
  analysis_state: 'LOT_OVERVIEW',
  summary: { lot_count: 2, ready: 2, preparing: 0 },
  lots: [
    { lot_id: '1', state: 'REPORT_READY', job_running: false },
    { lot_id: '2', state: 'REPORT_READY', job_running: false },
  ],
};

const Harness = () => {
  const state = useLotWorkspace('analysis_ws');
  return (
    <div data-testid="ws-state">
      {state.resolved ? 'resolved' : 'pending'}
      :{state.available ? 'available' : 'unavailable'}
      :{state.preparing ? 'preparing' : 'idle'}
    </div>
  );
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

const render = async () => {
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
  await act(async () => {
    root.render(<Harness />);
  });
  await flush();
};

const stateText = () => container.querySelector('[data-testid="ws-state"]').textContent;

describe('useLotWorkspace', () => {
  beforeEach(() => {
    globalThis.IS_REACT_ACT_ENVIRONMENT = true;
    document.body.innerHTML = '';
    jest.clearAllMocks();
    jest.useFakeTimers();
  });

  afterEach(() => {
    if (root) act(() => root.unmount());
    if (container?.parentNode) container.parentNode.removeChild(container);
    container = null;
    root = null;
    jest.useRealTimers();
  });

  test('a RUNNING lot triggers silent read-only polling; polling stops when done', async () => {
    getCorrectnessV2Workspace
      .mockResolvedValueOnce({ data: runningWorkspace })
      .mockResolvedValueOnce({ data: readyWorkspace });

    await render();
    expect(getCorrectnessV2Workspace).toHaveBeenCalledTimes(1);
    expect(stateText()).toBe('resolved:available:preparing');

    // The 8s poll re-reads the workspace (pure GET, no job creation possible).
    await act(async () => {
      jest.advanceTimersByTime(8000);
      await Promise.resolve();
    });
    await flush();
    expect(getCorrectnessV2Workspace).toHaveBeenCalledTimes(2);
    expect(stateText()).toBe('resolved:available:idle');

    // Once every lot is terminal the polling stops entirely.
    await act(async () => {
      jest.advanceTimersByTime(30000);
      await Promise.resolve();
    });
    await flush();
    expect(getCorrectnessV2Workspace).toHaveBeenCalledTimes(2);
  });

  test('no polling when no lot is running', async () => {
    getCorrectnessV2Workspace.mockResolvedValue({ data: readyWorkspace });

    await render();
    expect(getCorrectnessV2Workspace).toHaveBeenCalledTimes(1);

    await act(async () => {
      jest.advanceTimersByTime(60000);
      await Promise.resolve();
    });
    await flush();
    expect(getCorrectnessV2Workspace).toHaveBeenCalledTimes(1);
  });

  test('404 rollout: resolved + unavailable so the page can fall back safely', async () => {
    getCorrectnessV2Workspace.mockRejectedValue({ response: { status: 404 } });

    await render();
    expect(stateText()).toBe('resolved:unavailable:idle');
  });
});
