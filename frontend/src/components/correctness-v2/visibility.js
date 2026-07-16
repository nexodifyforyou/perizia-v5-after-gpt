// Pure visibility rules for the Correctness V2 report surface.
//
// PRODUCT RULE: the V2 surface is the ONLY report surface. It always mounts
// once the availability probe resolves, for every role. CustomerReportView
// owns every sub-state internally (ready / lot selection / preparing / busy /
// verification required / unavailable / no report), so no state can ever
// produce a blank page.
//
// Inputs:
//   isExactAdmin  exact-email operator (user.correctness_v2_admin_view)
//   v2Resolved    the availability probe has finished
export const computeCorrectnessV2Visibility = ({
  isExactAdmin = false,
  v2Resolved = false,
} = {}) => ({
  // The V2 surface is the only report surface: always mount it once resolved.
  showV2Surface: true,
  showAdminTab: Boolean(isExactAdmin),
  // Neutral placeholder while the probe is still resolving, so the page is
  // never blank and never flashes an intermediate state.
  showLoadingPlaceholder: Boolean(!v2Resolved),
});

export default computeCorrectnessV2Visibility;
