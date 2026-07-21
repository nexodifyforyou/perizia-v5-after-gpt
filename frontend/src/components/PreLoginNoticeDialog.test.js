import React from 'react';
import { createRoot } from 'react-dom/client';
import { act } from 'react';
import axios from 'axios';
import PreLoginNoticeDialog, { maskEmail } from './PreLoginNoticeDialog';
import { AuthProvider } from '../context/AuthContext';

// Hand-rolled rendering with createRoot + act, matching the convention used by
// the existing suites in this repo (no @testing-library is installed).
jest.mock('axios');

// The Radix dialog primitives render through a portal and pull in focus-trap
// behaviour that jsdom handles poorly; the flow under test is the multi-step
// form, not the dialog shell, so the primitives are reduced to plain elements.
jest.mock('./ui/dialog', () => ({
  Dialog: ({ open, children }) => (open ? <div>{children}</div> : null),
  DialogContent: ({ children }) => <div>{children}</div>,
  DialogHeader: ({ children }) => <div>{children}</div>,
  DialogTitle: ({ children }) => <h2>{children}</h2>,
  DialogDescription: ({ children }) => <p>{children}</p>,
  DialogFooter: ({ children }) => <div>{children}</div>,
}));

jest.mock('./ui/button', () => ({
  Button: ({ children, ...props }) => <button {...props}>{children}</button>,
}));

let container;
let root;

const setup = async (props = {}) => {
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
  await act(async () => {
    root.render(
      <AuthProvider>
        <PreLoginNoticeDialog
          open
          onOpenChange={props.onOpenChange || (() => {})}
          onConfirm={props.onConfirm || (() => {})}
        />
      </AuthProvider>
    );
  });
};

const teardown = async () => {
  if (root) {
    await act(async () => root.unmount());
  }
  if (container) container.remove();
  container = null;
  root = null;
};

const byTestId = (id) => container.querySelector(`[data-testid="${id}"]`);
const click = async (element) => {
  await act(async () => {
    element.dispatchEvent(new MouseEvent('click', { bubbles: true }));
  });
};
const type = async (input, value) => {
  const setter = Object.getOwnPropertyDescriptor(
    window.HTMLInputElement.prototype,
    'value'
  ).set;
  await act(async () => {
    setter.call(input, value);
    input.dispatchEvent(new Event('input', { bubbles: true }));
  });
};
const submit = async (form) => {
  await act(async () => {
    form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
  });
};

// AuthProvider issues two GETs on mount: /auth/me and the public capability
// probe. Routing by URL lets a test choose the backend's answer for the email
// option without disturbing the session check.
const mockCapabilities = (emailOtpEnabled) => {
  axios.get.mockImplementation((url) => {
    if (String(url).includes('/api/auth/capabilities')) {
      return Promise.resolve({
        data: { email_otp_enabled: emailOtpEnabled, google_enabled: true },
      });
    }
    return Promise.resolve({ data: null });
  });
};

const mockCapabilitiesUnreachable = () => {
  axios.get.mockImplementation((url) => {
    if (String(url).includes('/api/auth/capabilities')) {
      return Promise.reject(new Error('network down'));
    }
    return Promise.resolve({ data: null });
  });
};

beforeEach(() => {
  jest.clearAllMocks();
  jest.useRealTimers();
  // Default to the enabled backend: these suites exercise the OTP flow itself.
  mockCapabilities(true);
  axios.post.mockReset();
});

afterEach(teardown);

const goToEmailStep = async () => {
  await click(byTestId('login-email-btn'));
};

const goToCodeStep = async (email = 'mario.rossi@studio-example.it', resendIn = 60) => {
  await goToEmailStep();
  await type(byTestId('auth-email-input'), email);
  axios.post.mockResolvedValueOnce({
    data: { challenge_id: 'aec_1', expires_in: 600, resend_available_in: resendIn },
  });
  await submit(byTestId('auth-send-code-btn').closest('form'));
};

// ---------------------------------------------------------------------------
// Choice step
// ---------------------------------------------------------------------------
describe('login options', () => {
  it('keeps the Google button visible', async () => {
    await setup();
    const google = byTestId('login-google-btn');
    expect(google).toBeTruthy();
    expect(google.textContent).toContain('Accedi con Google');
  });

  it('offers continue-with-email equally prominently', async () => {
    await setup();
    const email = byTestId('login-email-btn');
    expect(email).toBeTruthy();
    expect(email.textContent).toContain('Accedi con email');
  });

  it('leaves the existing Google flow unchanged', async () => {
    const onConfirm = jest.fn();
    await setup({ onConfirm });
    await click(byTestId('login-google-btn'));
    expect(onConfirm).toHaveBeenCalledTimes(1);
    // The Google path must not touch the OTP endpoints.
    expect(axios.post).not.toHaveBeenCalled();
  });

  it('loads no beta or admin information before authentication', async () => {
    await setup();
    const calls = axios.get.mock.calls.map(([url]) => url);
    expect(calls.some((url) => /beta|admin/i.test(url))).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Backend capability gating
//
// The frontend auto-deploys to Vercel on push to main while the backend rolls
// out separately with AUTH_EMAIL_ENABLED=false. During that window the new
// build must not offer an email button that can only fail.
// ---------------------------------------------------------------------------
describe('capability gating', () => {
  it('hides the email option when the backend reports it disabled', async () => {
    mockCapabilities(false);
    await setup();
    expect(byTestId('login-email-btn')).toBeNull();
    expect(container.textContent).not.toContain('Accedi con email');
  });

  it('shows the email option when the backend reports it enabled', async () => {
    mockCapabilities(true);
    await setup();
    expect(byTestId('login-email-btn')).toBeTruthy();
  });

  it('keeps Google visible and working while email is disabled', async () => {
    mockCapabilities(false);
    const onConfirm = jest.fn();
    await setup({ onConfirm });
    const google = byTestId('login-google-btn');
    expect(google).toBeTruthy();
    expect(google.textContent).toContain('Accedi con Google');
    await click(google);
    expect(onConfirm).toHaveBeenCalledTimes(1);
  });

  it('triggers no email-auth request at all while disabled', async () => {
    mockCapabilities(false);
    await setup();
    const posted = axios.post.mock.calls.map(([url]) => String(url));
    expect(posted.some((url) => url.includes('/api/auth/email/'))).toBe(false);
    expect(axios.post).not.toHaveBeenCalled();
  });

  it('shows no error or broken screen while disabled', async () => {
    mockCapabilities(false);
    await setup();
    // The choice step renders normally: a title, Google, and cancel.
    expect(byTestId('auth-error')).toBeNull();
    expect(container.textContent).toContain('Accedi a PeriziaScan');
    expect(container.textContent).toContain('Annulla');
  });

  it('adapts the description so it never promises email login', async () => {
    mockCapabilities(false);
    await setup();
    expect(container.textContent).not.toMatch(/vuoi accedere/i);
    expect(container.textContent).toContain('Google');
  });

  it('hides email but keeps Google when the capability probe fails', async () => {
    mockCapabilitiesUnreachable();
    await setup();
    expect(byTestId('login-email-btn')).toBeNull();
    expect(byTestId('login-google-btn')).toBeTruthy();
  });

  it('treats a missing capability field as disabled', async () => {
    axios.get.mockResolvedValue({ data: {} });
    await setup();
    expect(byTestId('login-email-btn')).toBeNull();
    expect(byTestId('login-google-btn')).toBeTruthy();
  });

  it('does not accept a non-boolean truthy capability value', async () => {
    axios.get.mockImplementation((url) =>
      String(url).includes('/api/auth/capabilities')
        ? Promise.resolve({ data: { email_otp_enabled: 'false' } })
        : Promise.resolve({ data: null })
    );
    await setup();
    expect(byTestId('login-email-btn')).toBeNull();
  });

  it('reflects a backend change on the next mount, with nothing cached', async () => {
    mockCapabilities(false);
    await setup();
    expect(byTestId('login-email-btn')).toBeNull();
    await teardown();

    // Operator enables the feature; the user simply refreshes the page.
    mockCapabilities(true);
    await setup();
    expect(byTestId('login-email-btn')).toBeTruthy();

    // Nothing about the capability was persisted across the reload.
    expect(window.localStorage.getItem('email_otp_enabled')).toBeNull();
    expect(window.sessionStorage.getItem('email_otp_enabled')).toBeNull();
  });

  it('probes the capability without credentials or query parameters', async () => {
    await setup();
    const call = axios.get.mock.calls.find(([url]) =>
      String(url).includes('/api/auth/capabilities')
    );
    expect(call).toBeTruthy();
    // No email or identifier is sent, so the probe cannot leak account state.
    expect(String(call[0])).toMatch(/\/api\/auth\/capabilities$/);
    expect(call[1]).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Email step
// ---------------------------------------------------------------------------
describe('email step', () => {
  it('renders the specified copy', async () => {
    await setup();
    await goToEmailStep();
    expect(container.textContent).toContain('Accedi con la tua email');
    expect(container.textContent).toContain(
      'Ti invieremo un codice monouso al tuo indirizzo email.'
    );
    expect(byTestId('auth-email-input')).toBeTruthy();
    expect(byTestId('auth-send-code-btn').textContent).toContain('Invia codice');
  });

  it('labels the email field for assistive technology', async () => {
    await setup();
    await goToEmailStep();
    const input = byTestId('auth-email-input');
    const label = container.querySelector('label[for="auth-email-input"]');
    expect(label).toBeTruthy();
    expect(label.textContent).toContain('Email');
    expect(input.getAttribute('autocomplete')).toBe('email');
  });

  it('disables submit until the address looks valid', async () => {
    await setup();
    await goToEmailStep();
    expect(byTestId('auth-send-code-btn').disabled).toBe(true);
    await type(byTestId('auth-email-input'), 'not-an-email');
    expect(byTestId('auth-send-code-btn').disabled).toBe(true);
    await type(byTestId('auth-email-input'), 'utente@example-ms365.onmicrosoft.com');
    expect(byTestId('auth-send-code-btn').disabled).toBe(false);
  });

  it('shows a safe validation message for an invalid address', async () => {
    await setup();
    await goToEmailStep();
    await type(byTestId('auth-email-input'), 'broken@');
    await submit(byTestId('auth-send-code-btn').closest('form'));
    expect(byTestId('auth-error').textContent).toContain(
      'Inserisci un indirizzo email valido.'
    );
    expect(axios.post).not.toHaveBeenCalled();
  });

  it('accepts a non-Google corporate address', async () => {
    await setup();
    await goToCodeStep('mario.rossi@studio-example.it');
    expect(axios.post).toHaveBeenCalledWith(
      expect.stringContaining('/api/auth/email/request-code'),
      { email: 'mario.rossi@studio-example.it' },
      expect.objectContaining({ withCredentials: true })
    );
  });

  it('does not disclose whether the account already exists', async () => {
    await setup();
    await goToCodeStep();
    // The success path reveals nothing beyond the masked address the user typed.
    expect(container.textContent).not.toMatch(/registrat|esiste|nuovo account|beta/i);
  });

  it('prevents a duplicate send while a request is in flight', async () => {
    await setup();
    await goToEmailStep();
    await type(byTestId('auth-email-input'), 'mario.rossi@studio-example.it');

    let resolve;
    axios.post.mockReturnValueOnce(new Promise((r) => { resolve = r; }));

    const form = byTestId('auth-send-code-btn').closest('form');
    await submit(form);
    expect(byTestId('auth-send-code-btn').disabled).toBe(true);
    await submit(form);
    expect(axios.post).toHaveBeenCalledTimes(1);

    await act(async () => {
      resolve({ data: { challenge_id: 'aec_1', resend_available_in: 60 } });
    });
  });

  it('surfaces a generic message when delivery is unavailable', async () => {
    await setup();
    await goToEmailStep();
    await type(byTestId('auth-email-input'), 'mario.rossi@studio-example.it');
    axios.post.mockRejectedValueOnce({
      response: { data: { detail: 'Al momento non è possibile inviare il codice. Riprova più tardi.' } },
    });
    await submit(byTestId('auth-send-code-btn').closest('form'));

    expect(byTestId('auth-error').textContent).toContain(
      'Al momento non è possibile inviare il codice.'
    );
    // Still on the email step; no challenge was established.
    expect(byTestId('auth-email-input')).toBeTruthy();
  });
});

// ---------------------------------------------------------------------------
// Code step
// ---------------------------------------------------------------------------
describe('code step', () => {
  it('renders the specified copy and masks the address', async () => {
    await setup();
    await goToCodeStep('mario.rossi@studio-example.it');
    expect(container.textContent).toContain('Inserisci il codice');
    expect(container.textContent).toContain('Abbiamo inviato un codice a');
    expect(byTestId('auth-masked-email').textContent).toBe('ma•••••••••@studio-example.it');
    expect(byTestId('auth-verify-btn').textContent).toContain('Verifica e accedi');
  });

  it('labels the code field and enables one-time-code autofill', async () => {
    await setup();
    await goToCodeStep();
    const input = byTestId('auth-code-input');
    const label = container.querySelector('label[for="auth-code-input"]');
    expect(label.textContent).toContain('Codice a 6 cifre');
    expect(input.getAttribute('autocomplete')).toBe('one-time-code');
    expect(input.getAttribute('inputmode')).toBe('numeric');
  });

  it('accepts a pasted six-digit code', async () => {
    await setup();
    await goToCodeStep();
    await type(byTestId('auth-code-input'), '123456');
    expect(byTestId('auth-code-input').value).toBe('123456');
    expect(byTestId('auth-verify-btn').disabled).toBe(false);
  });

  it('rejects non-digit characters and caps the length', async () => {
    await setup();
    await goToCodeStep();
    await type(byTestId('auth-code-input'), '12a3-45b6789');
    expect(byTestId('auth-code-input').value).toBe('123456');
  });

  it('keeps verify disabled below six digits', async () => {
    await setup();
    await goToCodeStep();
    await type(byTestId('auth-code-input'), '1234');
    expect(byTestId('auth-verify-btn').disabled).toBe(true);
  });

  it('shows the resend countdown and unlocks it at zero', async () => {
    jest.useFakeTimers();
    await setup();
    await goToEmailStep();
    await type(byTestId('auth-email-input'), 'mario.rossi@studio-example.it');
    axios.post.mockResolvedValueOnce({
      data: { challenge_id: 'aec_1', resend_available_in: 3 },
    });
    await submit(byTestId('auth-send-code-btn').closest('form'));

    expect(byTestId('auth-resend-btn').textContent).toContain('tra 3s');
    expect(byTestId('auth-resend-btn').disabled).toBe(true);

    for (let i = 0; i < 3; i += 1) {
      await act(async () => {
        jest.advanceTimersByTime(1000);
      });
    }

    expect(byTestId('auth-resend-btn').disabled).toBe(false);
    expect(byTestId('auth-resend-btn').textContent).toContain('Invia un nuovo codice');
    jest.useRealTimers();
  });

  it('establishes the session on a correct code', async () => {
    const onOpenChange = jest.fn();
    await setup({ onOpenChange });
    await goToCodeStep();
    await type(byTestId('auth-code-input'), '123456');

    axios.post.mockResolvedValueOnce({
      data: { user: { user_id: 'u1', email: 'mario.rossi@studio-example.it' }, session_token: 'sess_x' },
    });
    await submit(byTestId('auth-verify-btn').closest('form'));

    expect(axios.post).toHaveBeenLastCalledWith(
      expect.stringContaining('/api/auth/email/verify-code'),
      { challenge_id: 'aec_1', code: '123456' },
      expect.objectContaining({ withCredentials: true })
    );
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('keeps an invalid-code error usable and clears the field', async () => {
    await setup();
    await goToCodeStep();
    await type(byTestId('auth-code-input'), '000000');
    axios.post.mockRejectedValueOnce({
      response: { data: { detail: 'Il codice non è valido o è scaduto. Richiedine uno nuovo.' } },
    });
    await submit(byTestId('auth-verify-btn').closest('form'));

    expect(byTestId('auth-error').textContent).toContain('Il codice non è valido o è scaduto.');
    expect(byTestId('auth-code-input').value).toBe('');
    expect(byTestId('auth-resend-btn')).toBeTruthy();
  });

  it('keeps an expired-code state recoverable', async () => {
    await setup();
    // No cooldown, so the resend action is immediately available.
    await goToCodeStep('mario.rossi@studio-example.it', 0);
    await type(byTestId('auth-code-input'), '123456');
    axios.post.mockRejectedValueOnce({
      response: { data: { detail: 'Il codice non è valido o è scaduto. Richiedine uno nuovo.' } },
    });
    await submit(byTestId('auth-verify-btn').closest('form'));

    // A new code can be requested straight away.
    axios.post.mockResolvedValueOnce({
      data: { challenge_id: 'aec_2', resend_available_in: 60 },
    });
    await click(byTestId('auth-resend-btn'));
    expect(byTestId('auth-error')).toBeNull();
  });

  it('announces errors to screen readers', async () => {
    await setup();
    await goToCodeStep();
    await type(byTestId('auth-code-input'), '000000');
    axios.post.mockRejectedValueOnce({ response: { data: { detail: 'Errore' } } });
    await submit(byTestId('auth-verify-btn').closest('form'));

    const error = byTestId('auth-error');
    expect(error.getAttribute('role')).toBe('alert');
    expect(error.getAttribute('aria-live')).toBe('assertive');
  });

  it('offers use-another-email and back-to-login', async () => {
    await setup();
    await goToCodeStep();
    expect(byTestId('auth-change-email-btn').textContent).toContain('Usa un’altra email');

    await click(byTestId('auth-back-to-login-btn'));
    expect(byTestId('login-google-btn')).toBeTruthy();
    expect(byTestId('login-email-btn')).toBeTruthy();
  });

  it('returns to the email step without losing the flow', async () => {
    await setup();
    await goToCodeStep();
    await click(byTestId('auth-change-email-btn'));
    expect(byTestId('auth-email-input')).toBeTruthy();
  });
});

// ---------------------------------------------------------------------------
// Masking helper
// ---------------------------------------------------------------------------
describe('maskEmail', () => {
  it('keeps the domain and hides the local part', () => {
    expect(maskEmail('mario.rossi@studio-example.it')).toBe('ma•••••••••@studio-example.it');
  });

  it('handles very short local parts', () => {
    expect(maskEmail('a@example.com')).toBe('a•@example.com');
  });

  it('leaves a malformed value alone', () => {
    expect(maskEmail('not-an-email')).toBe('not-an-email');
  });
});
