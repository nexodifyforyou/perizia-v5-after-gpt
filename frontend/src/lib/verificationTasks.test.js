import {
  buildVerificationTask,
  buildVerificationTasks,
  isSufficientVerificationTask,
  buildCostVerificationSeed,
  buildFormalitiesVerificationSeed,
} from './verificationTasks';

describe('verificationTasks', () => {
  it('builds an actionable task from an urbanistica row with what/why/pages/who', () => {
    const task = buildVerificationTask({
      topic: 'Regolarità urbanistica',
      text: 'Difformità urbanistica non sanata. Controllare p. 7, 8.',
      pages: [7, 8, 11],
    });
    expect(task.what_to_verify_it).toMatch(/sanabile|commerciabilità/i);
    expect(task.why_it_matters_it.length).toBeGreaterThan(12);
    expect(task.pages).toEqual([7, 8, 11]);
    expect(task.who_should_verify_it).toMatch(/tecnico|geometra|comunale/i);
    expect(task.urgency).toBeTruthy();
  });

  it('routes occupation topics to custode/legale', () => {
    const task = buildVerificationTask({ topic: 'Stato occupativo / opponibilità', pages: [14] });
    expect(task.title_it).toMatch(/occupativ|occupazione/i);
    expect(task.what_to_verify_it).toMatch(/occupa|liberazione/i);
    expect(task.who_should_verify_it).toMatch(/custode|delegato|legale/i);
    expect(task.pages).toEqual([14]);
  });

  it('derives pages from evidence when pages are not provided', () => {
    const task = buildVerificationTask({
      title: 'Formalità / ipoteche',
      detail: 'Ipoteca da cancellare',
      evidence: [{ page: 26 }, { page: 26 }, { page: 3 }],
    });
    expect(task.pages).toEqual([3, 26]);
    expect(task.who_should_verify_it).toMatch(/delegato|legale/i);
  });

  it('never accepts a bare "Controllare p.X" as a sufficient task', () => {
    const bare = {
      title_it: 'X',
      what_to_verify_it: 'Controllare p.52',
      why_it_matters_it: 'Controllare p.52',
    };
    expect(isSufficientVerificationTask(bare)).toBe(false);
  });

  it('always produces a what + why (no bare page-only task text)', () => {
    const task = buildVerificationTask({ topic: 'Voce critica', text: 'Controllare p.52.', pages: [52] });
    expect(isSufficientVerificationTask(task)).toBe(true);
    expect(task.what_to_verify_it).not.toMatch(/^controllare\s+p/i);
  });

  it('dedupes tasks by title and merges pages', () => {
    const tasks = buildVerificationTasks([
      { topic: 'Regolarità urbanistica', pages: [7] },
      { topic: 'Regolarità urbanistica', pages: [8, 11] },
    ]);
    expect(tasks).toHaveLength(1);
    expect(tasks[0].pages).toEqual([7, 8, 11]);
  });

  it('supports synthetic cost and formalità seeds', () => {
    const tasks = buildVerificationTasks([], {
      extras: [buildCostVerificationSeed([59, 60]), buildFormalitiesVerificationSeed([26])],
    });
    const titles = tasks.map((t) => t.title_it.toLowerCase());
    expect(titles.some((t) => t.includes('importi'))).toBe(true);
    expect(titles.some((t) => t.includes('formalità') || t.includes('formalita'))).toBe(true);
    const cost = tasks.find((t) => t.title_it.toLowerCase().includes('importi'));
    expect(cost.pages).toEqual([59, 60]);
  });
});
