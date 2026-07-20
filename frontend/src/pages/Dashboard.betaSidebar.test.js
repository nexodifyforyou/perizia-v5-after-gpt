import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import { Sidebar } from './Dashboard';

jest.mock('react-router-dom', () => ({
  Link: ({ children, to, ...props }) => <a href={to} {...props}>{children}</a>,
  useNavigate: () => jest.fn(),
  useLocation: () => ({ pathname: '/dashboard' }),
}), { virtual: true });

jest.mock('sonner', () => ({ toast: { error: jest.fn(), success: jest.fn(), info: jest.fn() } }));

const renderSidebar = (user) => {
  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);
  act(() => { root.render(<Sidebar user={user} logout={jest.fn()} />); });
  return { container, cleanup: () => act(() => root.unmount()) };
};

// The admin nav ENTRY is a link to /admin/beta-program (distinct from the plan
// label text "Programma Beta" a beta tester legitimately sees).
const betaProgramNavLinks = (c) => c.querySelectorAll('a[href="/admin/beta-program"]');
const legacyNavLinks = (c) => c.querySelectorAll('a[href="/admin/beta-feedback"]');

describe('Sidebar — Programma Beta nav entry', () => {
  test('exact owner sees a single Programma Beta nav entry and no legacy entry', () => {
    const { container, cleanup } = renderSidebar(
      { name: 'Owner', email: 'o@x.it', is_master_admin: true, correctness_v2_admin_view: true });
    expect(betaProgramNavLinks(container)).toHaveLength(1);
    expect(legacyNavLinks(container)).toHaveLength(0);
    expect(container.textContent).not.toContain('Beta Feedback');
    cleanup();
  });

  test('master admin who is NOT exact owner has no Programma Beta nav entry', () => {
    const { container, cleanup } = renderSidebar(
      { name: 'Admin', email: 'a@x.it', is_master_admin: true, correctness_v2_admin_view: false });
    expect(betaProgramNavLinks(container)).toHaveLength(0);
    expect(legacyNavLinks(container)).toHaveLength(0);
    cleanup();
  });

  test('normal customer has no admin section and no Programma Beta nav entry', () => {
    const { container, cleanup } = renderSidebar({ name: 'Mario', email: 'm@x.it', is_master_admin: false });
    expect(betaProgramNavLinks(container)).toHaveLength(0);
    expect(container.textContent).not.toContain('ADMIN');
    cleanup();
  });

  test('active beta tester (non-admin) has no Programma Beta nav entry', () => {
    const { container, cleanup } = renderSidebar(
      { name: 'Beta', email: 'b@x.it', is_master_admin: false, is_beta_partner: true,
        beta_program: { active: true } });
    expect(betaProgramNavLinks(container)).toHaveLength(0);
    cleanup();
  });

  test('active beta tester sees "Illimitate" and never a fake 9999 number', () => {
    const { container, cleanup } = renderSidebar(
      { name: 'Beta', email: 'b@x.it', is_master_admin: false, is_beta_partner: true,
        beta_program: { active: true } });
    expect(container.querySelector('[data-testid="sidebar-beta-unlimited"]')).not.toBeNull();
    expect(container.textContent).toContain('Illimitate');
    expect(container.textContent).not.toContain('9999');
    cleanup();
  });
});
