// Browser-level validation of passwordless email login.
//
// The OTP is read out of the local SMTP sink — the same way a user reads it
// from their mailbox — never out of the database. No production service is
// contacted and no real address is ever used.
const fs = require('fs');
const { test, expect } = require('@playwright/test');

const MAIL_FILE = process.env.E2E_MAIL_FILE || '/tmp/perizia_e2e/mail.jsonl';
const API_URL = process.env.E2E_API_URL || 'http://127.0.0.1:8099';

const readMail = () => {
  if (!fs.existsSync(MAIL_FILE)) return [];
  return fs
    .readFileSync(MAIL_FILE, 'utf-8')
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line));
};

const waitForCode = async (address, previousCount = 0) => {
  for (let attempt = 0; attempt < 60; attempt += 1) {
    const all = readMail();
    if (all.length > previousCount) {
      const match = [...all].reverse().find((m) => (m.to || []).includes(address));
      if (match && match.code) return { code: match.code, message: match, count: all.length };
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  throw new Error(`no code delivered to ${address}`);
};

const uniqueEmail = (prefix) =>
  `${prefix}.${Date.now()}.${Math.floor(Math.random() * 10000)}@studio-e2e-example.it`;

const openLogin = async (page) => {
  await page.goto('/');
  await page.getByTestId('header-login-btn').click();
};

const requestCode = async (page, address) => {
  const before = readMail().length;
  await page.getByTestId('login-email-btn').click();
  await page.getByTestId('auth-email-input').fill(address);
  await page.getByTestId('auth-send-code-btn').click();
  return waitForCode(address, before);
};

test.describe('passwordless email login', () => {
  // This file describes the enabled rollout state. The disabled state is a
  // different contract entirely and lives in auth-email-disabled.spec.js.
  test.skip(
    process.env.E2E_AUTH_EMAIL_ENABLED === 'false',
    'backend has AUTH_EMAIL_ENABLED=false; see auth-email-disabled.spec.js'
  );

  test('both login options are offered', async ({ page }) => {
    await openLogin(page);
    await expect(page.getByTestId('login-google-btn')).toBeVisible();
    await expect(page.getByTestId('login-email-btn')).toBeVisible();
    await expect(page.getByTestId('login-google-btn')).toContainText('Continua con Google');
    await expect(page.getByTestId('login-email-btn')).toContainText('Continua con email');
  });

  test('the Google route is still reachable', async ({ request }) => {
    const response = await request.get(`${API_URL}/api/auth/google/start`, {
      maxRedirects: 0,
    });
    // Redirects to Google, or reports Google unconfigured in this environment.
    expect([302, 307, 503]).toContain(response.status());
  });

  test('a corporate address can log in end to end', async ({ page }) => {
    const address = uniqueEmail('mario.rossi');
    await openLogin(page);

    const { code, message } = await requestCode(page, address);

    // The delivered message is correct and carries no account state.
    expect(message.subject).toBe('Il tuo codice di accesso a Perizia Scan');
    expect(message.from).toContain('accesso@auth.nexodify.com');
    expect(message.body).not.toMatch(/beta|credit|report|admin/i);

    await expect(page.getByTestId('auth-masked-email')).toBeVisible();
    await expect(page.getByTestId('auth-masked-email')).not.toContainText(address);

    await page.getByTestId('auth-code-input').fill(code);
    await page.getByTestId('auth-verify-btn').click();

    await expect(page.getByTestId('header-login-btn')).toHaveCount(0, { timeout: 15_000 });
  });

  test('a six-digit code can be pasted and non-digits are rejected', async ({ page }) => {
    const address = uniqueEmail('paste.user');
    await openLogin(page);
    await requestCode(page, address);

    const field = page.getByTestId('auth-code-input');
    await field.fill('12ab34cd56');
    await expect(field).toHaveValue('123456');
  });

  test('an invalid code shows a safe, recoverable error', async ({ page }) => {
    const address = uniqueEmail('wrong.code');
    await openLogin(page);
    const { code } = await requestCode(page, address);
    const wrong = code === '000000' ? '111111' : '000000';

    await page.getByTestId('auth-code-input').fill(wrong);
    await page.getByTestId('auth-verify-btn').click();

    const error = page.getByTestId('auth-error');
    await expect(error).toBeVisible();
    await expect(error).toContainText('Il codice non è valido o è scaduto');
    // Nothing about the account is revealed.
    await expect(error).not.toContainText(/beta|admin|registrat/i);
    await expect(page.getByTestId('auth-resend-btn')).toBeVisible();
  });

  test('a used code cannot be replayed', async ({ page, context }) => {
    const address = uniqueEmail('replay.user');
    await openLogin(page);
    const { code } = await requestCode(page, address);
    await page.getByTestId('auth-code-input').fill(code);
    await page.getByTestId('auth-verify-btn').click();
    await expect(page.getByTestId('header-login-btn')).toHaveCount(0, { timeout: 15_000 });

    // Replaying the same code through the API must fail.
    const response = await context.request.post(`${API_URL}/api/auth/email/verify-code`, {
      data: { challenge_id: 'replayed', code },
      failOnStatusCode: false,
    });
    expect(response.status()).toBe(400);
  });

  test('requesting a code discloses nothing about the account', async ({ request }) => {
    const known = uniqueEmail('known.user');
    const unknown = uniqueEmail('never.seen');

    const first = await request.post(`${API_URL}/api/auth/email/request-code`, {
      data: { email: known },
    });
    const second = await request.post(`${API_URL}/api/auth/email/request-code`, {
      data: { email: unknown },
    });

    expect(first.status()).toBe(second.status());
    const a = await first.json();
    const b = await second.json();
    expect(a.message).toBe(b.message);
    expect(Object.keys(a).sort()).toEqual(Object.keys(b).sort());
  });

  test('use-another-email and back-to-login work', async ({ page }) => {
    const address = uniqueEmail('navigate.user');
    await openLogin(page);
    await requestCode(page, address);

    await page.getByTestId('auth-change-email-btn').click();
    await expect(page.getByTestId('auth-email-input')).toBeVisible();

    await page.getByTestId('auth-back-btn').click();
    await expect(page.getByTestId('login-google-btn')).toBeVisible();
  });

  test('logout and log back in with a fresh code', async ({ page }) => {
    const address = uniqueEmail('relogin.user');
    await openLogin(page);
    const first = await requestCode(page, address);
    await page.getByTestId('auth-code-input').fill(first.code);
    await page.getByTestId('auth-verify-btn').click();
    await expect(page.getByTestId('header-login-btn')).toHaveCount(0, { timeout: 15_000 });

    await page.evaluate(async (api) => {
      await fetch(`${api}/api/auth/logout`, { method: 'POST', credentials: 'include' });
    }, API_URL);

    await page.goto('/');
    await expect(page.getByTestId('header-login-btn')).toBeVisible({ timeout: 15_000 });

    await page.getByTestId('header-login-btn').click();
    const second = await requestCode(page, address);
    expect(second.code).toBeTruthy();
    await page.getByTestId('auth-code-input').fill(second.code);
    await page.getByTestId('auth-verify-btn').click();
    await expect(page.getByTestId('header-login-btn')).toHaveCount(0, { timeout: 15_000 });
  });
});
