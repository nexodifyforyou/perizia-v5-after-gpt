import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import axios from 'axios';
import TestersTab from './TestersTab';

jest.mock('axios');
jest.mock('sonner', () => ({ toast: { error: jest.fn(), success: jest.fn(), info: jest.fn() } }));

const tester = (over = {}) => ({
  membership_id: 'betam_1', normalized_email: 't@example.test', display_name: 'Beta',
  status: 'ACTIVE', account_linked: true, added_at: '2026-07-01T00:00:00Z',
  activated_at: '2026-07-01T00:00:00Z', revoked_at: null, analyses_total: 3, feedback_total: 1,
  ...over,
});

const renderTab = async (rows) => {
  axios.get.mockResolvedValue({ data: { items: rows, total: rows.length, page: 1, page_size: 25 } });
  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);
  await act(async () => { root.render(<TestersTab active={true} />); });
  await act(async () => { await Promise.resolve(); });
  return { container, root, cleanup: async () => { await act(async () => root.unmount()); } };
};

const fireInput = (el, value) => {
  const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
  setter.call(el, value);
  el.dispatchEvent(new Event('input', { bubbles: true }));
};
const click = (el) => el.dispatchEvent(new MouseEvent('click', { bubbles: true }));

describe('TestersTab', () => {
  test('renders status badges and account-linked labels', async () => {
    const { container, cleanup } = await renderTab([
      tester({ status: 'ACTIVE' }),
      tester({ membership_id: 'betam_2', status: 'PENDING', account_linked: false, normalized_email: 'p@example.test' }),
      tester({ membership_id: 'betam_3', status: 'REVOKED', normalized_email: 'r@example.test' }),
    ]);
    expect(container.textContent).toContain('Attivo');
    expect(container.textContent).toContain('In attesa');
    expect(container.textContent).toContain('Revocato');
    expect(container.textContent).toContain('Registrazione in attesa');
    expect(container.textContent).toContain('Account registrato');
    // No raw Mongo _id leaks.
    expect(container.textContent).not.toContain('_id');
    await cleanup();
  });

  test('add-tester form validates email client-side before POST', async () => {
    const { container, cleanup } = await renderTab([]);
    const emailInput = container.querySelector('[data-testid="beta-add-email"]');
    fireInput(emailInput, 'not-an-email');
    await act(async () => { click(container.querySelector('[data-testid="beta-add-submit"]')); });
    expect(container.querySelector('[data-testid="beta-add-email-error"]')).not.toBeNull();
    expect(axios.post).not.toHaveBeenCalled();
    await cleanup();
  });

  test('revoke shows a confirmation dialog explaining what is preserved', async () => {
    const { container, cleanup } = await renderTab([tester({ status: 'ACTIVE' })]);
    click(container.querySelector('[data-testid="beta-revoke-btn"]'));
    await act(async () => { await Promise.resolve(); });
    const dialog = container.querySelector('[data-testid="beta-revoke-dialog"]');
    expect(dialog).not.toBeNull();
    expect(dialog.textContent).toContain("L'account resta attivo");
    expect(dialog.textContent).toContain('report storici restano');
    expect(dialog.textContent).toContain('feedback resta conservato');
    expect(dialog.textContent).toContain('crediti già acquistati restano preservati');
    await cleanup();
  });

  test('revoked tester exposes a Riattiva action', async () => {
    const { container, cleanup } = await renderTab([tester({ status: 'REVOKED' })]);
    expect(container.querySelector('[data-testid="beta-reactivate-btn"]')).not.toBeNull();
    expect(container.querySelector('[data-testid="beta-revoke-btn"]')).toBeNull();
    await cleanup();
  });
});
