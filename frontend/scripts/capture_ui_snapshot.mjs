import { chromium } from "playwright";
import fs from "fs";

const NEW_AID = process.env.NEW_AID;
const RUN_DIR = process.env.RUN_DIR;
const FRONTEND_URL = process.env.FRONTEND_URL;
const API_DOMAIN = "api-periziascan.nexodify.com";
const SESSION_TOKEN = process.env.SESSION_TOKEN;

if (!NEW_AID || !RUN_DIR || !SESSION_TOKEN || !FRONTEND_URL) {
  console.error("Missing env NEW_AID/RUN_DIR/SESSION_TOKEN/FRONTEND_URL");
  process.exit(2);
}
const debugPayloadPath = `${RUN_DIR}/system.json`;
const debugPayload = JSON.parse(fs.readFileSync(debugPayloadPath, "utf-8"));

const browser = await chromium.launch();
const context = await browser.newContext();
await context.addInitScript((payload) => {
  window.__DEBUG_ANALYSIS_PAYLOAD__ = payload;
}, debugPayload);

await context.addCookies([
  {
    name: "session_token",
    value: SESSION_TOKEN,
    domain: API_DOMAIN,
    path: "/",
    httpOnly: true,
    secure: true,
    sameSite: "None"
  }
]);

const page = await context.newPage();
await page.goto(FRONTEND_URL, { waitUntil: "networkidle", timeout: 120000 });
try {
  await page.waitForFunction(() => window.__UI_SNAPSHOT__ !== undefined, { timeout: 45000 });
} catch (err) {
  // Continue and dump page state even when snapshot is missing.
}

const snap = await page.evaluate(() => window.__UI_SNAPSHOT__ || null);
fs.writeFileSync(`${RUN_DIR}/frontend_snapshot.json`, JSON.stringify(snap, null, 2), "utf-8");
const pageState = await page.evaluate(() => ({
  url: window.location.href,
  title: document.title,
  text: document.body ? document.body.innerText.slice(0, 1200) : ""
}));
fs.writeFileSync(`${RUN_DIR}/frontend_page_state.json`, JSON.stringify(pageState, null, 2), "utf-8");
await page.screenshot({ path: `${RUN_DIR}/frontend_render.png`, fullPage: true });

console.log("SNAPSHOT_NULL", snap === null);
console.log("Saved frontend_snapshot.json and frontend_render.png");
await browser.close();
