import { computeCorrectnessV2Visibility } from './visibility';

describe('computeCorrectnessV2Visibility', () => {
  test('normal customer, probe resolved: V2 surface only, no admin tab', () => {
    const v = computeCorrectnessV2Visibility({
      isExactAdmin: false,
      v2Resolved: true,
    });
    expect(v.showV2Surface).toBe(true);
    expect(v.showAdminTab).toBe(false);
    expect(v.showLoadingPlaceholder).toBe(false);
  });

  test('normal customer, probe still resolving: placeholder, surface still mounted', () => {
    const v = computeCorrectnessV2Visibility({
      isExactAdmin: false,
      v2Resolved: false,
    });
    expect(v.showV2Surface).toBe(true);
    expect(v.showLoadingPlaceholder).toBe(true);
  });

  test('exact admin, probe resolved: surface + admin tab', () => {
    const v = computeCorrectnessV2Visibility({
      isExactAdmin: true,
      v2Resolved: true,
    });
    expect(v.showV2Surface).toBe(true);
    expect(v.showAdminTab).toBe(true);
    expect(v.showLoadingPlaceholder).toBe(false);
  });

  test('no arguments: safe defaults, surface always on', () => {
    const v = computeCorrectnessV2Visibility();
    expect(v.showV2Surface).toBe(true);
    expect(v.showAdminTab).toBe(false);
    expect(v.showLoadingPlaceholder).toBe(true);
  });

  test('the V2 surface mounts in EVERY state (blank-page regression)', () => {
    [
      {},
      { isExactAdmin: false, v2Resolved: false },
      { isExactAdmin: false, v2Resolved: true },
      { isExactAdmin: true, v2Resolved: false },
      { isExactAdmin: true, v2Resolved: true },
    ].forEach((input) => {
      expect(computeCorrectnessV2Visibility(input).showV2Surface).toBe(true);
    });
  });

  test('legacy visibility keys no longer exist', () => {
    const v = computeCorrectnessV2Visibility({
      isExactAdmin: true,
      v2Resolved: true,
      // Former inputs must be inert if ever passed.
      hasSafeV2: false,
      v2Preparing: true,
      legacyReveal: true,
    });
    expect(v).not.toHaveProperty('showLegacyBody');
    expect(v).not.toHaveProperty('canRevealLegacy');
    expect(v).not.toHaveProperty('legacyFallback');
    expect(v).not.toHaveProperty('showPreparingBanner');
    expect(Object.keys(v).sort()).toEqual([
      'showAdminTab',
      'showLoadingPlaceholder',
      'showV2Surface',
    ]);
  });
});
