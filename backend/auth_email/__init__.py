"""Provider-independent passwordless email authentication (six-digit OTP).

Identity is the verified normalized email, never the provider. This package adds
a second authentication method alongside the existing Google OAuth flow; both
converge on ``server._create_local_login`` so session, cookie, owner
authorization and beta linking behave identically regardless of how the user
authenticated.

Modules:
- ``config``     : environment reads + fail-closed preflight
- ``sender``     : provider-neutral ``EmailSender`` + Resend/fake/sink adapters
- ``templates``  : the single OTP message body (no account state, ever)
- ``challenges`` : challenge lifecycle, hashing, atomic single-use consumption
- ``ratelimit``  : Mongo-backed atomic counters (no shared cache exists)
- ``api``        : the two public endpoints
"""
