// V2 customer-surface snapshot capture (Playwright).
//
// REPLACES the old legacy-report parity capture. The legacy report body was
// removed permanently (branch: feature-customer-access-and-legacy-removal), and
// with it the `window.__UI_SNAPSHOT__` global that the previous version of this
// script waited on. That capture asserted legacy DOM fields and can never pass
// again, so this script now captures the CORRECTNESS V2 surface instead and
// records the evidence needed by the V2 safety gate:
//
//   * the V2 customer surface actually mounts (never a blank page);
//   * which customer state rendered;
//   * that NO legacy DOM exists (body / reveal toggle / print / download);
//   * the full network log, so the gate can prove the page fetches the
//     metadata-only endpoint and NEVER the legacy report payload or /pdf.
//
// Env: NEW_AID, RUN_DIR, FRONTEND_URL, SESSION_TOKEN (token is never printed).
import { chromium } from "playwright";
import fs from "fs";

const NEW_AID = process.env.NEW_AID;
const RUN_DIR = process.env.RUN_DIR;
const FRONTEND_URL = process.env.FRONTEND_URL;
const API_DOMAIN = process.env.API_COOKIE_DOMAIN || "api-periziascan.nexodify.com";
const SESSION_TOKEN = process.env.SESSION_TOKEN;

if (!NEW_AID || !RUN_DIR || !SESSION_TOKEN || !FRONTEND_URL) {
  console.error("Missing env NEW_AID/RUN_DIR/SESSION_TOKEN/FRONTEND_URL");
  process.exit(2);
}

// Legacy surface markers that must NOT exist anywhere in the DOM.
const LEGACY_TESTIDS = [
  "legacy-report-body",
  "legacy-report-reveal",
  "print-view-btn",
  "download-pdf-btn",
];

// Customer states the V2 surface may render. Exactly one should be present.
const V2_STATE_TESTIDS = [
  "cv2-customer-report",
  "cv2-customer-lot-selector",
  "cv2-customer-lot-pending",
  "cv2-customer-preparing",
  "cv2-customer-verification-required",
  "cv2-customer-service-busy",
  "cv2-customer-service-unavailable",
  "cv2-customer-not-readable",
  "cv2-customer-unavailable",
];

const browser = await chromium.launch();
const context = await browser.newContext();

// Cookie attributes must match the target scheme: a Secure/SameSite=None cookie
// is rejected over plain http (local gate runs), which would silently produce an
// unauthenticated capture and a misleading failure.
const isHttps = FRONTEND_URL.startsWith("https://");
await context.addCookies([
  {
    name: "session_token",
    value: SESSION_TOKEN,
    domain: API_DOMAIN,
    path: "/",
    httpOnly: true,
    secure: isHttps,
    sameSite: isHttps ? "None" : "Lax",
  },
]);

const page = await context.newPage();

// Record every request the page issues so the gate can prove no legacy fetch.
const requests = [];
page.on("request", (req) => requests.push({ method: req.method(), url: req.url() }));

await page.goto(FRONTEND_URL, { waitUntil: "networkidle", timeout: 120000 });

// Wait for the V2 surface to resolve (it always mounts once the probe settles).
try {
  await page.waitForSelector('[data-testid="cv2-customer-tab-panel"]', { timeout: 45000 });
} catch (err) {
  // Continue and dump state anyway: a missing surface is itself a gate failure.
}

const dom = await page.evaluate(
  ({ legacyIds, stateIds }) => {
    const has = (id) => Boolean(document.querySelector(`[data-testid="${id}"]`));
    const legacyPresent = legacyIds.filter(has);
    const statesPresent = stateIds.filter(has);
    const bodyText = document.body ? document.body.innerText : "";
    return {
      v2_surface_mounted: has("cv2-customer-tab-panel") || has("cv2-admin-tab-panel"),
      v2_customer_panel: has("cv2-customer-tab-panel"),
      admin_tab_present: has("cv2-tab-admin"),
      admin_panel_present: has("cv2-admin-tab-panel"),
      legacy_testids_present: legacyPresent,
      v2_states_present: statesPresent,
      page_is_blank: bodyText.trim().length === 0,
      body_text_len: bodyText.length,
    };
  },
  { legacyIds: LEGACY_TESTIDS, stateIds: V2_STATE_TESTIDS }
);

const analysisId = NEW_AID;
const pathOf = (u) => {
  try {
    return new URL(u).pathname.replace(/\/$/, "");
  } catch (e) {
    return u;
  }
};
const network = {
  all: requests.map((r) => r.url),
  // metadata-only endpoint that feeds the page shell
  meta_fetches: requests
    .filter((r) => pathOf(r.url) === `/api/analysis/perizia/${analysisId}/meta`)
    .map((r) => r.url),
  // the sanitized V2 customer view
  customer_view_fetches: requests
    .filter((r) => r.url.includes("/correctness-v2/customer-view/latest"))
    .map((r) => r.url),
  // ANY of these is a legacy exposure regression
  legacy_payload_fetches: requests
    .filter((r) => {
      const p = pathOf(r.url);
      return (
        (p === `/api/analysis/perizia/${analysisId}` && r.method === "GET") ||
        p === `/api/history/perizia/${analysisId}`
      );
    })
    .map((r) => `${r.method} ${r.url}`),
  legacy_render_fetches: requests
    .filter((r) => /\/(pdf|pdf-html|html)$/.test(pathOf(r.url)))
    .map((r) => `${r.method} ${r.url}`),
};

const snap = { analysis_id: analysisId, captured_at: new Date().toISOString(), dom, network };
fs.writeFileSync(`${RUN_DIR}/v2_surface_snapshot.json`, JSON.stringify(snap, null, 2), "utf-8");

const pageState = await page.evaluate(() => ({
  url: window.location.href,
  title: document.title,
  text: document.body ? document.body.innerText.slice(0, 1200) : "",
}));
fs.writeFileSync(`${RUN_DIR}/frontend_page_state.json`, JSON.stringify(pageState, null, 2), "utf-8");
await page.screenshot({ path: `${RUN_DIR}/frontend_render.png`, fullPage: true });

console.log("V2_SURFACE_MOUNTED", dom.v2_surface_mounted);
console.log("V2_STATES_PRESENT", JSON.stringify(dom.v2_states_present));
console.log("LEGACY_DOM_PRESENT", JSON.stringify(dom.legacy_testids_present));
console.log("LEGACY_PAYLOAD_FETCHES", network.legacy_payload_fetches.length);
console.log("Saved v2_surface_snapshot.json and frontend_render.png");
await browser.close();
