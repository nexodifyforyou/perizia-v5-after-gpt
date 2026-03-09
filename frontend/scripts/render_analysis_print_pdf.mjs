import fs from "fs";
import { chromium } from "playwright";

const ANALYSIS_ID = process.env.ANALYSIS_ID;
const FRONTEND_URL = process.env.FRONTEND_URL;
const API_BASE_URL = process.env.API_BASE_URL;
const SESSION_TOKEN = process.env.SESSION_TOKEN;
const OUT_PATH = process.env.OUT_PATH;
const DEBUG_PAYLOAD_PATH = process.env.DEBUG_PAYLOAD_PATH || "";

if (!ANALYSIS_ID || !FRONTEND_URL || !API_BASE_URL || !OUT_PATH) {
  console.error("Missing env ANALYSIS_ID/FRONTEND_URL/API_BASE_URL/OUT_PATH");
  process.exit(2);
}
if (!SESSION_TOKEN && !DEBUG_PAYLOAD_PATH) {
  console.error("SESSION_TOKEN is required unless DEBUG_PAYLOAD_PATH is provided");
  process.exit(2);
}

const frontend = new URL(FRONTEND_URL);
const backend = new URL(API_BASE_URL);
const pageUrl = new URL(`/analysis/${ANALYSIS_ID}/print`, frontend);
pageUrl.searchParams.set("pdf", "1");

let debugPayload = null;
if (DEBUG_PAYLOAD_PATH) {
  debugPayload = JSON.parse(fs.readFileSync(DEBUG_PAYLOAD_PATH, "utf-8"));
  pageUrl.searchParams.set("debug", "1");
}

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({
  viewport: { width: 1440, height: 2200 },
  colorScheme: "light",
  deviceScaleFactor: 1,
});

if (SESSION_TOKEN) {
  await context.addCookies([
    {
      name: "session_token",
      value: SESSION_TOKEN,
      domain: backend.hostname,
      path: "/",
      httpOnly: true,
      secure: backend.protocol === "https:",
      sameSite: backend.protocol === "https:" ? "None" : "Lax",
    },
  ]);
}

if (debugPayload) {
  await context.addInitScript((payload) => {
    window.__DEBUG_ANALYSIS_PAYLOAD__ = payload;
  }, debugPayload);
}

const page = await context.newPage();
const consoleMessages = [];
page.on("console", (message) => {
  consoleMessages.push(`[${message.type()}] ${message.text()}`);
});
page.on("pageerror", (error) => {
  consoleMessages.push(`[pageerror] ${error.message}`);
});
await page.goto(pageUrl.toString(), { waitUntil: "domcontentloaded", timeout: 120000 });
try {
  await page.waitForFunction(() => window.__PERIZIA_PRINT_READY__ === true, { timeout: 60000 });
} catch (error) {
  const failureState = await page.evaluate(() => ({
    href: window.location.href,
    title: document.title,
    ready: window.__PERIZIA_PRINT_READY__,
    bodyReady: document.body?.getAttribute("data-print-report-ready"),
    text: document.body?.innerText?.slice(0, 2000) || "",
  }));
  await page.screenshot({ path: `${OUT_PATH}.failure.png`, fullPage: true });
  fs.writeFileSync(
    `${OUT_PATH}.failure.json`,
    JSON.stringify({ failureState, consoleMessages }, null, 2),
    "utf-8",
  );
  throw error;
}
await page.emulateMedia({ media: "print" });
await page.pdf({
  path: OUT_PATH,
  format: "A4",
  printBackground: true,
  preferCSSPageSize: true,
  displayHeaderFooter: false,
});

const state = await page.evaluate(() => ({
  href: window.location.href,
  title: document.title,
  ready: window.__PERIZIA_PRINT_READY__ === true,
  bodyReady: document.body?.getAttribute("data-print-report-ready"),
}));
fs.writeFileSync(`${OUT_PATH}.meta.json`, JSON.stringify(state, null, 2), "utf-8");

await browser.close();
console.log(JSON.stringify({ ok: true, out_path: OUT_PATH, page: state }, null, 2));
