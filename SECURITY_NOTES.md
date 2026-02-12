# Security Notes

- `ALLOW_OFFLINE_QA` must remain unset or `0` in production.
- `OFFLINE_QA_TOKEN` must not be configured in production.
- `OFFLINE_QA` fixture mode is intended only for localhost regression with explicit token-gated headers.
- Defense in depth: block inbound `X-OFFLINE-QA` and `X-OFFLINE-QA-TOKEN` headers at the nginx edge for public traffic.
