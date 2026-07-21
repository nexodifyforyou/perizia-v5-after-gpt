"""Minimal SMTP capture server for end-to-end authentication tests.

Accepts messages, writes each one to a JSON-lines file, and delivers nothing.
This is what lets a browser test read the OTP the way a real user would —
out of a mailbox — instead of reaching into the database.

Deliberately test-only: it lives outside ``backend/`` and is never importable by
the application. Production uses the Resend adapter; the ``sink`` provider is
selected only by the e2e harness.

    python e2e/smtp_sink.py --port 1025 --out /tmp/mail.jsonl
"""

from __future__ import annotations

import argparse
import asyncio
import email
import json
import re
from pathlib import Path

CODE_RE = re.compile(r"\b(\d{6})\b")


class SMTPSession:
    def __init__(self, out_path: Path):
        self.out_path = out_path

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        async def send(line: str) -> None:
            writer.write(f"{line}\r\n".encode())
            await writer.drain()

        await send("220 localhost SMTP sink")
        recipients: list[str] = []

        try:
            while True:
                raw = await reader.readline()
                if not raw:
                    break
                command = raw.decode("utf-8", "replace").strip()
                upper = command.upper()

                if upper.startswith(("HELO", "EHLO")):
                    await send("250 localhost")
                elif upper.startswith("MAIL FROM"):
                    await send("250 OK")
                elif upper.startswith("RCPT TO"):
                    match = re.search(r"<([^>]*)>", command)
                    if match:
                        recipients.append(match.group(1))
                    await send("250 OK")
                elif upper.startswith("DATA"):
                    await send("354 End data with <CR><LF>.<CR><LF>")
                    lines: list[str] = []
                    while True:
                        chunk = await reader.readline()
                        if not chunk:
                            break
                        text = chunk.decode("utf-8", "replace")
                        if text.strip() == ".":
                            break
                        lines.append(text.rstrip("\r\n"))
                    self._record("\n".join(lines), recipients)
                    await send("250 OK")
                elif upper.startswith("QUIT"):
                    await send("221 Bye")
                    break
                elif upper.startswith("RSET"):
                    recipients = []
                    await send("250 OK")
                else:
                    await send("250 OK")
        except Exception:
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass

    def _record(self, raw_message: str, recipients: list[str]) -> None:
        message = email.message_from_string(raw_message)

        body_parts: list[str] = []
        if message.is_multipart():
            for part in message.walk():
                if part.get_content_type() == "text/plain":
                    body_parts.append(part.get_payload(decode=True).decode("utf-8", "replace"))
        else:
            payload = message.get_payload(decode=True)
            if payload:
                body_parts.append(payload.decode("utf-8", "replace"))
        body = "\n".join(body_parts)

        found = CODE_RE.search(body)
        record = {
            "to": recipients,
            "from": message.get("From"),
            "subject": message.get("Subject"),
            "body": body,
            "code": found.group(1) if found else None,
        }
        with self.out_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


async def main_async(port: int, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("", encoding="utf-8")
    session = SMTPSession(out)
    server = await asyncio.start_server(session.handle, "127.0.0.1", port)
    print(f"smtp sink listening on 127.0.0.1:{port} -> {out}", flush=True)
    async with server:
        await server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=1025)
    parser.add_argument("--out", type=Path, default=Path("/tmp/perizia_mail_sink.jsonl"))
    args = parser.parse_args()
    try:
        asyncio.run(main_async(args.port, args.out))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
