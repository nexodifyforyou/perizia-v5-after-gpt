import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Loader2, Mail, Shield } from 'lucide-react';
import { Button } from './ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle
} from './ui/dialog';
import { useAuth } from '../context/AuthContext';

export const STEP_CHOICE = 'choice';
export const STEP_EMAIL = 'email';
export const STEP_CODE = 'code';

// Same generic copy the backend returns, so a network-level failure and a
// refusal look identical to the user and neither reveals account state.
const GENERIC_SEND_ERROR = 'Al momento non è possibile inviare il codice. Riprova più tardi.';
const GENERIC_CODE_ERROR = 'Il codice non è valido o è scaduto. Richiedine uno nuovo.';
const INVALID_EMAIL_ERROR = 'Inserisci un indirizzo email valido.';

// Deliberately permissive: the backend is the authority on validity. This only
// catches obvious typos before spending a request.
const EMAIL_PATTERN = /^[^@\s]+@[^@\s.]+\.[^@\s]+$/;

export const maskEmail = (raw) => {
  const value = String(raw || '').trim();
  const at = value.lastIndexOf('@');
  if (at < 1) return value;
  const local = value.slice(0, at);
  const domain = value.slice(at + 1);
  const head = local.length <= 2 ? local.slice(0, 1) : local.slice(0, 2);
  return `${head}${'•'.repeat(Math.max(1, local.length - head.length))}@${domain}`;
};

const errorMessage = (error, fallback) => {
  const detail = error?.response?.data?.detail;
  return typeof detail === 'string' && detail ? detail : fallback;
};

const PreLoginNoticeDialog = ({ open, onOpenChange, onConfirm }) => {
  const { requestEmailCode, verifyEmailCode, emailOtpEnabled } = useAuth();
  // Treat anything but an explicit true as "off": an older provider that does
  // not supply the flag must not resurrect the button.
  const emailLoginAvailable = emailOtpEnabled === true;

  const [step, setStep] = useState(STEP_CHOICE);
  const [email, setEmail] = useState('');
  const [code, setCode] = useState('');
  const [challengeId, setChallengeId] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [cooldown, setCooldown] = useState(0);

  const emailInputRef = useRef(null);
  const codeInputRef = useRef(null);

  const reset = useCallback(() => {
    setStep(STEP_CHOICE);
    setEmail('');
    setCode('');
    setChallengeId(null);
    setBusy(false);
    setError('');
    setCooldown(0);
  }, []);

  useEffect(() => {
    if (!open) reset();
  }, [open, reset]);

  // If the capability resolves to false while a stale email step is showing
  // (slow probe, or the operator disabled it mid-session), fall back to the
  // choice step rather than leaving a form that can only 503.
  useEffect(() => {
    if (!emailLoginAvailable && step !== STEP_CHOICE) reset();
  }, [emailLoginAvailable, step, reset]);

  // Resend countdown. Purely informational — the backend enforces the real
  // limit, so a user who edits this cannot bypass anything.
  useEffect(() => {
    if (cooldown <= 0) return undefined;
    const timer = setTimeout(() => setCooldown((value) => Math.max(0, value - 1)), 1000);
    return () => clearTimeout(timer);
  }, [cooldown]);

  useEffect(() => {
    if (step === STEP_EMAIL) emailInputRef.current?.focus();
    if (step === STEP_CODE) codeInputRef.current?.focus();
  }, [step]);

  const emailValid = useMemo(() => EMAIL_PATTERN.test(email.trim()), [email]);
  const codeValid = code.length === 6;

  const sendCode = useCallback(
    async (targetEmail) => {
      if (busy) return; // guards against a double submit
      setBusy(true);
      setError('');
      try {
        const data = await requestEmailCode(targetEmail);
        setChallengeId(data?.challenge_id || null);
        // A configured cooldown of 0 is legitimate, so `|| 60` would be wrong.
        const nextCooldown = Number(data?.resend_available_in);
        setCooldown(Number.isFinite(nextCooldown) && nextCooldown >= 0 ? nextCooldown : 60);
        setCode('');
        setStep(STEP_CODE);
      } catch (err) {
        setError(errorMessage(err, GENERIC_SEND_ERROR));
      } finally {
        setBusy(false);
      }
    },
    [busy, requestEmailCode]
  );

  const handleEmailSubmit = async (event) => {
    event.preventDefault();
    const trimmed = email.trim();
    if (!EMAIL_PATTERN.test(trimmed)) {
      setError(INVALID_EMAIL_ERROR);
      return;
    }
    await sendCode(trimmed);
  };

  const handleCodeSubmit = async (event) => {
    event.preventDefault();
    if (busy || !codeValid || !challengeId) return;
    setBusy(true);
    setError('');
    try {
      await verifyEmailCode(challengeId, code);
      onOpenChange(false);
    } catch (err) {
      setError(errorMessage(err, GENERIC_CODE_ERROR));
      setCode('');
      codeInputRef.current?.focus();
    } finally {
      setBusy(false);
    }
  };

  // Accept only digits, and cap at six, so paste and autofill of a full code
  // work while stray characters are simply dropped.
  const handleCodeChange = (event) => {
    setCode(String(event.target.value || '').replace(/\D/g, '').slice(0, 6));
  };

  const errorBlock = error ? (
    <p
      role="alert"
      aria-live="assertive"
      data-testid="auth-error"
      className="mt-3 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-200"
    >
      {error}
    </p>
  ) : null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-md border border-gold/20 bg-zinc-950 p-0 text-zinc-100 shadow-[0_24px_80px_rgba(0,0,0,0.55)]">
        <div className="rounded-lg border border-white/5 bg-gradient-to-b from-zinc-900 via-zinc-950 to-black p-6">
          {step === STEP_CHOICE && (
            <>
              <DialogHeader className="space-y-3 text-left">
                <div className="flex h-11 w-11 items-center justify-center rounded-xl border border-gold/20 bg-gold/10 text-gold">
                  <Shield className="h-5 w-5" />
                </div>
                <DialogTitle className="font-serif text-2xl text-zinc-50">
                  Accedi a PeriziaScan
                </DialogTitle>
                <DialogDescription className="text-sm leading-6 text-zinc-300">
                  {emailLoginAvailable
                    ? 'Scegli come vuoi accedere al tuo account.'
                    : 'Accedi con il tuo account Google.'}
                </DialogDescription>
              </DialogHeader>

              <div className="mt-6 flex flex-col gap-3">
                <Button
                  type="button"
                  data-testid="login-google-btn"
                  onClick={onConfirm}
                  className="w-full bg-gold text-zinc-950 hover:bg-gold-dim font-semibold gold-glow"
                >
                  Accedi con Google
                </Button>
                {emailLoginAvailable && (
                  <Button
                    type="button"
                    variant="outline"
                    data-testid="login-email-btn"
                    onClick={() => {
                      setError('');
                      setStep(STEP_EMAIL);
                    }}
                    className="w-full border-zinc-700 bg-transparent text-zinc-100 hover:bg-zinc-900"
                  >
                    <Mail className="mr-2 h-4 w-4" aria-hidden="true" />
                    Accedi con email
                  </Button>
                )}
              </div>

              <DialogFooter className="mt-6 sm:justify-end">
                <Button
                  type="button"
                  variant="ghost"
                  onClick={() => onOpenChange(false)}
                  className="text-zinc-400 hover:bg-zinc-900 hover:text-zinc-100"
                >
                  Annulla
                </Button>
              </DialogFooter>
            </>
          )}

          {step === STEP_EMAIL && (
            <form onSubmit={handleEmailSubmit} noValidate>
              <DialogHeader className="space-y-3 text-left">
                <DialogTitle className="font-serif text-2xl text-zinc-50">
                  Accedi con la tua email
                </DialogTitle>
                <DialogDescription className="text-sm leading-6 text-zinc-300">
                  Ti invieremo un codice monouso al tuo indirizzo email.
                </DialogDescription>
              </DialogHeader>

              <div className="mt-5">
                <label htmlFor="auth-email-input" className="block text-sm text-zinc-300">
                  Email
                </label>
                <input
                  id="auth-email-input"
                  ref={emailInputRef}
                  data-testid="auth-email-input"
                  type="email"
                  name="email"
                  autoComplete="email"
                  inputMode="email"
                  value={email}
                  onChange={(event) => {
                    setEmail(event.target.value);
                    if (error) setError('');
                  }}
                  aria-invalid={Boolean(error)}
                  aria-describedby={error ? 'auth-error-text' : undefined}
                  className="mt-2 w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-zinc-100 outline-none focus:border-gold"
                />
              </div>

              <div id="auth-error-text">{errorBlock}</div>

              <DialogFooter className="mt-6 flex-col-reverse gap-3 sm:flex-row sm:justify-end sm:space-x-0">
                <Button
                  type="button"
                  variant="outline"
                  data-testid="auth-back-btn"
                  onClick={() => {
                    setError('');
                    setStep(STEP_CHOICE);
                  }}
                  className="border-zinc-700 bg-transparent text-zinc-300 hover:bg-zinc-900"
                >
                  Torna all’accesso
                </Button>
                <Button
                  type="submit"
                  data-testid="auth-send-code-btn"
                  disabled={busy || !emailValid}
                  className="bg-gold text-zinc-950 hover:bg-gold-dim font-semibold gold-glow"
                >
                  {busy && <Loader2 className="mr-2 h-4 w-4 animate-spin" aria-hidden="true" />}
                  {busy ? 'Invio in corso…' : 'Invia codice'}
                </Button>
              </DialogFooter>
            </form>
          )}

          {step === STEP_CODE && (
            <form onSubmit={handleCodeSubmit} noValidate>
              <DialogHeader className="space-y-3 text-left">
                <DialogTitle className="font-serif text-2xl text-zinc-50">
                  Inserisci il codice
                </DialogTitle>
                <DialogDescription className="text-sm leading-6 text-zinc-300">
                  Abbiamo inviato un codice a{' '}
                  <span data-testid="auth-masked-email" className="text-zinc-100">
                    {maskEmail(email)}
                  </span>
                  .
                </DialogDescription>
              </DialogHeader>

              <div className="mt-5">
                <label htmlFor="auth-code-input" className="block text-sm text-zinc-300">
                  Codice a 6 cifre
                </label>
                <input
                  id="auth-code-input"
                  ref={codeInputRef}
                  data-testid="auth-code-input"
                  type="text"
                  name="one-time-code"
                  // Lets iOS/Android offer the code straight from the SMS/email.
                  autoComplete="one-time-code"
                  inputMode="numeric"
                  pattern="[0-9]*"
                  value={code}
                  onChange={handleCodeChange}
                  aria-invalid={Boolean(error)}
                  className="mt-2 w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-center text-2xl tracking-[0.5em] text-zinc-100 outline-none focus:border-gold"
                />
              </div>

              {errorBlock}

              <div className="mt-4 flex flex-col gap-2 text-sm">
                <button
                  type="button"
                  data-testid="auth-resend-btn"
                  disabled={busy || cooldown > 0}
                  onClick={() => sendCode(email.trim())}
                  className="text-left text-gold disabled:text-zinc-500"
                >
                  {cooldown > 0
                    ? `Invia un nuovo codice tra ${cooldown}s`
                    : 'Invia un nuovo codice'}
                </button>
                <button
                  type="button"
                  data-testid="auth-change-email-btn"
                  onClick={() => {
                    setError('');
                    setCode('');
                    setChallengeId(null);
                    setStep(STEP_EMAIL);
                  }}
                  className="text-left text-zinc-400 hover:text-zinc-200"
                >
                  Usa un’altra email
                </button>
                <button
                  type="button"
                  data-testid="auth-back-to-login-btn"
                  onClick={reset}
                  className="text-left text-zinc-400 hover:text-zinc-200"
                >
                  Torna all’accesso
                </button>
              </div>

              <DialogFooter className="mt-6 sm:justify-end">
                <Button
                  type="submit"
                  data-testid="auth-verify-btn"
                  disabled={busy || !codeValid}
                  className="bg-gold text-zinc-950 hover:bg-gold-dim font-semibold gold-glow"
                >
                  {busy && <Loader2 className="mr-2 h-4 w-4 animate-spin" aria-hidden="true" />}
                  {busy ? 'Verifica in corso…' : 'Verifica e accedi'}
                </Button>
              </DialogFooter>
            </form>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
};

export default PreLoginNoticeDialog;
