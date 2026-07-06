import { computeCorrectnessV2Visibility } from './visibility';

describe('computeCorrectnessV2Visibility', () => {
  test('normal customer with a safe V2 report: V2 surface, no legacy', () => {
    const v = computeCorrectnessV2Visibility({
      isExactAdmin: false,
      hasSafeV2: true,
      v2Resolved: true,
    });
    expect(v.showV2Surface).toBe(true);
    expect(v.showLegacyBody).toBe(false);
    expect(v.canRevealLegacy).toBe(false);
    expect(v.showLoadingPlaceholder).toBe(false);
  });

  test('normal customer without a safe V2 report: legacy fallback only', () => {
    const v = computeCorrectnessV2Visibility({
      isExactAdmin: false,
      hasSafeV2: false,
      v2Resolved: true,
    });
    expect(v.showV2Surface).toBe(false);
    expect(v.showLegacyBody).toBe(true);
    expect(v.canRevealLegacy).toBe(false);
  });

  test('probe still resolving for a customer: no legacy flash, placeholder shown', () => {
    const v = computeCorrectnessV2Visibility({
      isExactAdmin: false,
      hasSafeV2: false,
      v2Resolved: false,
    });
    expect(v.showV2Surface).toBe(false);
    expect(v.showLegacyBody).toBe(false);
    expect(v.showLoadingPlaceholder).toBe(true);
  });

  test('exact admin with a safe V2 report: surface shown, legacy hidden but revealable', () => {
    const v = computeCorrectnessV2Visibility({
      isExactAdmin: true,
      hasSafeV2: true,
      v2Resolved: true,
      legacyReveal: false,
    });
    expect(v.showV2Surface).toBe(true);
    expect(v.showLegacyBody).toBe(false);
    expect(v.canRevealLegacy).toBe(true);
    expect(v.showLoadingPlaceholder).toBe(false);
  });

  test('exact admin opting in reveals the legacy report', () => {
    const v = computeCorrectnessV2Visibility({
      isExactAdmin: true,
      hasSafeV2: true,
      v2Resolved: true,
      legacyReveal: true,
    });
    expect(v.showLegacyBody).toBe(true);
    expect(v.canRevealLegacy).toBe(true);
  });

  test('exact admin without a safe V2 report: surface (for run controls) + legacy fallback', () => {
    const v = computeCorrectnessV2Visibility({
      isExactAdmin: true,
      hasSafeV2: false,
      v2Resolved: true,
    });
    expect(v.showV2Surface).toBe(true);
    expect(v.showLegacyBody).toBe(true);
    expect(v.canRevealLegacy).toBe(false);
    expect(v.showLoadingPlaceholder).toBe(false);
  });

  test('exact admin while probe resolving: surface reachable, no placeholder', () => {
    const v = computeCorrectnessV2Visibility({
      isExactAdmin: true,
      hasSafeV2: false,
      v2Resolved: false,
    });
    expect(v.showV2Surface).toBe(true);
    expect(v.showLoadingPlaceholder).toBe(false);
    expect(v.showLegacyBody).toBe(false);
  });

  test('V2 job preparing, no safe report yet: banner + legacy fallback stays readable', () => {
    const v = computeCorrectnessV2Visibility({
      isExactAdmin: false,
      hasSafeV2: false,
      v2Resolved: true,
      v2Preparing: true,
    });
    expect(v.showPreparingBanner).toBe(true);
    expect(v.showLegacyBody).toBe(true);
    expect(v.showV2Surface).toBe(false);
  });

  test('safe V2 available: no preparing banner even if a rerun is in flight', () => {
    const v = computeCorrectnessV2Visibility({
      isExactAdmin: false,
      hasSafeV2: true,
      v2Resolved: true,
      v2Preparing: true,
    });
    expect(v.showPreparingBanner).toBe(false);
    expect(v.showLegacyBody).toBe(false);
  });

  test('probe unresolved: no preparing banner (placeholder covers it)', () => {
    const v = computeCorrectnessV2Visibility({
      isExactAdmin: false,
      hasSafeV2: false,
      v2Resolved: false,
      v2Preparing: true,
    });
    expect(v.showPreparingBanner).toBe(false);
  });

  test('lot selection (LOT_SELECTION_REQUIRED is safe): customer surface, no legacy underneath', () => {
    // The hook reports available=true for LOT_SELECTION_REQUIRED, so hasSafeV2
    // is true and the legacy report stays hidden while the lot selector shows.
    const v = computeCorrectnessV2Visibility({
      isExactAdmin: false,
      hasSafeV2: true,
      v2Resolved: true,
    });
    expect(v.showV2Surface).toBe(true);
    expect(v.showLegacyBody).toBe(false);
  });
});
