// Browser-level validation of the DISABLED rollout state.
//
// This is the state production is deployed in first: a push to main auto-builds
// the frontend on Vercel while the backend still runs AUTH_EMAIL_ENABLED=false.
// The build under test here is byte-identical to the one auth-email.spec.js
// exercises with the feature on — only the backend flag differs — which is what
// proves the frontend takes its instruction from the backend and not from a
// build-time variable.
const { test, expect } = require('@playwright/test');

const API_URL = process.env.E2E_API_URL || 'http://127.0.0.1:8099';

const openLogin = async (page) => {
  await page.goto('/');
  await page.getByTestId('header-login-btn').click();
};

test.describe('email login disabled on the backend', () => {
  test.skip(
    process.env.E2E_AUTH_EMAIL_ENABLED !== 'false',
    'only meaningful when the backend runs with AUTH_EMAIL_ENABLED=false'
  );

  test('the capability endpoint reports the feature off', async ({ request }) => {
    const response = await request.get(`${API_URL}/api/auth/capabilities`);
    expect(response.status()).toBe(200);
    const body = await response.json();
    expect(body.email_otp_enabled).toBe(false);
    expect(body.google_enabled).toBe(true);
  });

  test('the capability payload carries no secret or internal configuration', async ({
    request,
  }) => {
    const response = await request.get(`${API_URL}/api/auth/capabilities`);
    const body = await response.json();
    expect(Object.keys(body).sort()).toEqual(['email_otp_enabled', 'google_enabled']);

    const raw = (await response.text()).toLowerCase();
    for (const forbidden of [
      'pepper',
      'resend',
      'api_key',
      'sink',
      'mongo',
      'feature_disabled',
      'reason',
      'nexodify.com',
    ]) {
      expect(raw).not.toContain(forbidden);
    }
  });

  test('Google login remains visible and the email option is absent', async ({ page }) => {
    await openLogin(page);
    await expect(page.getByTestId('login-google-btn')).toBeVisible();
    await expect(page.getByTestId('login-google-btn')).toContainText('Continua con Google');
    await expect(page.getByTestId('login-email-btn')).toHaveCount(0);
    await expect(page.getByText('Continua con email')).toHaveCount(0);
  });

  test('no broken or misleading screen is shown', async ({ page }) => {
    await openLogin(page);
    // The dialog renders its normal choice step: title, Google, cancel.
    await expect(page.getByText('Accesso sicuro')).toBeVisible();
    await expect(page.getByText('Annulla')).toBeVisible();
    // No error banner, and no copy promising an email option that is not there.
    await expect(page.getByTestId('auth-error')).toHaveCount(0);
    await expect(page.getByText(/indirizzo email aziendale/i)).toHaveCount(0);
  });

  test('no email-auth request is triggered by opening the login dialog', async ({ page }) => {
    const otpCalls = [];
    page.on('request', (req) => {
      if (req.url().includes('/api/auth/email/')) otpCalls.push(req.url());
    });

    await openLogin(page);
    await page.waitForTimeout(1000);

    expect(otpCalls).toEqual([]);
  });

  test('the Google route is still reachable', async ({ request }) => {
    const response = await request.get(`${API_URL}/api/auth/google/start`, {
      maxRedirects: 0,
    });
    expect([302, 307, 503]).toContain(response.status());
  });

  test('a direct request-code call fails closed', async ({ request }) => {
    const response = await request.post(`${API_URL}/api/auth/email/request-code`, {
      data: { email: 'someone.e2e@studio-e2e-example.it' },
    });
    expect(response.status()).toBe(503);
    const body = await response.json();
    expect(body.detail).toBe(
      'Al momento non è possibile inviare il codice. Riprova più tardi.'
    );
  });

  test('a direct verify-code call fails closed', async ({ request }) => {
    const response = await request.post(`${API_URL}/api/auth/email/verify-code`, {
      data: { challenge_id: 'aec_nonexistent', code: '123456' },
    });
    expect(response.status()).toBe(503);
  });

  test('the disabled response reveals nothing about account existence', async ({
    request,
  }) => {
    // A known-shaped address and a certainly-unknown one must be identical in
    // status and body, so the disabled state is not an enumeration oracle.
    const known = await request.post(`${API_URL}/api/auth/email/request-code`, {
      data: { email: 'nexodifyforyou@gmail.com' },
    });
    const unknown = await request.post(`${API_URL}/api/auth/email/request-code`, {
      data: { email: 'definitely.not.a.user.e2e@studio-e2e-example.it' },
    });

    expect(known.status()).toBe(unknown.status());
    expect(await known.text()).toBe(await unknown.text());
  });
});
