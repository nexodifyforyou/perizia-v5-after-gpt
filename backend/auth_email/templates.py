"""The single OTP email body.

Deliberately content-free with respect to the account: no beta status, no
credits, no report data, no indication that the address is even registered. The
message is identical for an owner, a beta tester, an existing customer and a
stranger, which is what keeps the flow non-enumerating end to end.
"""

from __future__ import annotations

SUBJECT = "Il tuo codice di accesso a Perizia Scan"


def _minutes(ttl_seconds: int) -> int:
    return max(1, int(round(ttl_seconds / 60)))


def render_text(code: str, ttl_seconds: int) -> str:
    minutes = _minutes(ttl_seconds)
    return (
        "Usa questo codice per accedere a Perizia Scan:\n"
        f"\n{code}\n\n"
        f"Il codice scade tra {minutes} minuti e può essere utilizzato una sola volta.\n\n"
        "Se non hai richiesto questo accesso, puoi ignorare questa email.\n\n"
        "Questo messaggio è inviato da un indirizzo che non accetta risposte."
    )


def render_html(code: str, ttl_seconds: int) -> str:
    minutes = _minutes(ttl_seconds)
    return (
        '<div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;'
        'font-size:16px;line-height:1.5;color:#111827;">'
        "<p>Usa questo codice per accedere a Perizia Scan:</p>"
        '<p style="font-size:32px;font-weight:700;letter-spacing:6px;margin:24px 0;">'
        f"{code}</p>"
        f"<p>Il codice scade tra {minutes} minuti e pu&ograve; essere utilizzato una sola volta.</p>"
        "<p>Se non hai richiesto questo accesso, puoi ignorare questa email.</p>"
        '<p style="color:#6b7280;font-size:13px;">Questo messaggio &egrave; inviato da un '
        "indirizzo che non accetta risposte.</p>"
        "</div>"
    )
