import { getAccountState } from './featureAccess';

describe('featureAccess — beta program', () => {
  test('active beta maps betaProgram and overrides plan label to "Programma Beta"', () => {
    const state = getAccountState({
      plan: 'free',
      beta_program: { active: true, display_name: 'Geom. Beta', member_since: '2026-07-01T00:00:00Z' },
    });
    expect(state.betaProgram.active).toBe(true);
    expect(state.betaProgram.displayName).toBe('Geom. Beta');
    expect(state.betaProgram.memberSince).toBe('2026-07-01T00:00:00Z');
    expect(state.planLabel).toBe('Programma Beta');
  });

  test('inactive beta keeps the real plan label', () => {
    const state = getAccountState({ plan: 'pro', beta_program: { active: false } });
    expect(state.betaProgram.active).toBe(false);
    expect(state.planLabel).toBe('Pro');
  });

  test('no beta_program field defaults to inactive and normal label', () => {
    const state = getAccountState({ plan: 'solo' });
    expect(state.betaProgram.active).toBe(false);
    expect(state.planLabel).toBe('Solo');
  });

  test('isExactOwner reflects the exact-owner flag only', () => {
    expect(getAccountState({ correctness_v2_admin_view: true }).isExactOwner).toBe(true);
    expect(getAccountState({ is_master_admin: true }).isExactOwner).toBe(false);
    expect(getAccountState({}).isExactOwner).toBe(false);
  });

  test('real purchased balance is preserved in state even when beta is active', () => {
    const state = getAccountState({
      plan: 'free',
      beta_program: { active: true },
      perizia_credits: { total_available: 8, monthly_remaining: 0 },
    });
    // The real number stays available (Billing shows it as "Crediti preservati");
    // it is never replaced by a fake 9999/unlimited number.
    expect(state.periziaCredits.totalAvailable).toBe(8);
    expect(state.periziaCredits.totalAvailable).not.toBe(9999);
  });

  test('falls back to is_beta_partner when beta_program is absent', () => {
    const state = getAccountState({ plan: 'free', is_beta_partner: true, beta_partner_name: 'X' });
    expect(state.betaProgram.active).toBe(true);
    expect(state.planLabel).toBe('Programma Beta');
  });
});
