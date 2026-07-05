// Pure visibility rules for the Correctness V2 vs legacy report surface.
//
// PRODUCT RULE: the customer sees exactly ONE report.
//   * When a safe sanitized V2 customer report exists, the V2 surface is shown
//     and the legacy report is hidden from the main page for everyone.
//   * When no safe V2 report exists, the legacy report is the fallback and is
//     shown unchanged.
//   * The exact-email admin always has the V2 surface (so the Vista admin tab
//     with run/quality/debug is reachable) and can OPT IN to reveal the legacy
//     report for inspection; it is never auto-stacked under the V2 tabs.
//
// Inputs:
//   isExactAdmin  exact-email operator (user.correctness_v2_admin_view)
//   hasSafeV2     a safe sanitized V2 customer report is available
//   v2Resolved    the availability probe has finished (avoids legacy flash)
//   legacyReveal  exact admin toggled the opt-in legacy inspection panel
export const computeCorrectnessV2Visibility = ({
  isExactAdmin = false,
  hasSafeV2 = false,
  v2Resolved = false,
  legacyReveal = false,
} = {}) => {
  // The V2 surface (tabs) renders for the exact admin always, and for anyone
  // once a safe customer report exists. Other admins/customers without a safe
  // V2 report never see the surface (they get the legacy fallback instead).
  const showV2Surface = Boolean(isExactAdmin || hasSafeV2);

  // Only the exact admin may reveal the legacy report while a safe V2 exists.
  const canRevealLegacy = Boolean(isExactAdmin && hasSafeV2);

  // Legacy is the single-report fallback ONLY when the probe resolved and no
  // safe V2 report exists. While the probe is unresolved we hide legacy to
  // avoid flashing a second report that then disappears.
  const legacyFallback = Boolean(v2Resolved && !hasSafeV2);

  const showLegacyBody = Boolean(legacyFallback || (canRevealLegacy && legacyReveal));

  // Neutral placeholder for non-admins while the probe is still resolving, so
  // the page is never blank and never briefly shows the wrong report.
  const showLoadingPlaceholder = Boolean(!v2Resolved && !isExactAdmin);

  return {
    showV2Surface,
    showLegacyBody,
    canRevealLegacy,
    legacyFallback,
    showLoadingPlaceholder,
  };
};

export default computeCorrectnessV2Visibility;
