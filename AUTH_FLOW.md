# Auth Flow — PeriziaScan

Date: 2026-02-12

**Actual Auth Mechanism**
- Frontend uses Emergent Auth (hosted at `https://auth.emergentagent.com`) and expects a `session_id` in the callback URL (hash/query/path). `AuthCallback` extracts `session_id` and exchanges it with backend (`/api/auth/session`) (`frontend/src/pages/AuthCallback.js:18`-`frontend/src/pages/AuthCallback.js:72`).
- Backend `/api/auth/session` calls Emergent Auth session-data endpoint (`https://demobackend.emergentagent.com/auth/v1/env/oauth/session-data`) with `X-Session-ID` and then creates a local session (`session_token`) stored in Mongo (`db.user_sessions`) (`backend/server.py:313`-`backend/server.py:393`).
- Session is stored in a cookie named `session_token` (`HttpOnly`, `Secure`, `SameSite=None`, 7 days) and is also returned in the JSON response (`backend/server.py:402`-`backend/server.py:416`).
- Backend accepts either cookie `session_token` or `Authorization: Bearer <session_token>` to authenticate (`backend/server.py:175`-`backend/server.py:181`).

**What the Frontend Sends**
- Login redirect: browser navigates to `https://auth.emergentagent.com/?redirect=<origin>/dashboard` (`frontend/src/context/AuthContext.js:29`-`frontend/src/context/AuthContext.js:33`).
- Session exchange: POST `/api/auth/session` with JSON `{ "session_id": "..." }` and `withCredentials: true` (`frontend/src/context/AuthContext.js:35`-`frontend/src/context/AuthContext.js:41`).

**Backend Endpoints + Session Storage**
- `POST /api/auth/session`: Exchanges Emergent `session_id` for local `session_token`, creates `User` + `UserSession`, sets `session_token` cookie (`backend/server.py:313`-`backend/server.py:416`).
- `GET /api/auth/me`: Returns current user based on cookie or bearer token (`backend/server.py:418`-`backend/server.py:427`).
- `POST /api/auth/logout`: Deletes session from `db.user_sessions` and clears cookie (`backend/server.py:429`-`backend/server.py:437`).

**Non-Browser Auth for Local Testing (127.0.0.1:8081)**
The repo includes a DB-seeded session flow (no browser needed) via `auth_testing.md` (`auth_testing.md:3`-`auth_testing.md:47`). Use it with the repo’s Mongo settings (`backend/.env`):

1) Create a local user + session token (MongoDB):
```bash
MONGO_URL=$(grep '^MONGO_URL=' /srv/perizia/app/backend/.env | cut -d '=' -f2)
DB_NAME=$(grep '^DB_NAME=' /srv/perizia/app/backend/.env | cut -d '=' -f2)

SESSION_TOKEN=$(mongosh "$MONGO_URL/$DB_NAME" --quiet --eval '
  var userId = "test-user-" + Date.now();
  var sessionToken = "test_session_" + Date.now();
  db.users.insertOne({
    user_id: userId,
    email: "test.user." + Date.now() + "@example.com",
    name: "Test User",
    picture: "https://via.placeholder.com/150",
    plan: "pro",
    is_master_admin: false,
    quota: { perizia_scans_remaining: 50, image_scans_remaining: 100, assistant_messages_remaining: 9999 },
    created_at: new Date()
  });
  db.user_sessions.insertOne({
    user_id: userId,
    session_token: sessionToken,
    expires_at: new Date(Date.now() + 7*24*60*60*1000),
    created_at: new Date()
  });
  print(sessionToken);
')

echo "SESSION_TOKEN=$SESSION_TOKEN"
```

2) Authenticate locally and save token for reuse (no browser):
```bash
# Using Bearer token (supported by backend auth helper)
API_URL=http://127.0.0.1:8081
curl -sS "$API_URL/api/auth/me" -H "Authorization: Bearer $SESSION_TOKEN" | jq .
```

3) Call `POST /api/analysis/perizia` with the test PDF:
```bash
API_URL=http://127.0.0.1:8081
curl -sS -X POST "$API_URL/api/analysis/perizia" \
  -H "Authorization: Bearer $SESSION_TOKEN" \
  -F "file=@/srv/perizia/app/perizia_test.pdf" | jq . > /tmp/perizia_analysis_response.json
```

4) Fetch stored analysis JSON and download report HTML:
```bash
ANALYSIS_ID=$(jq -r '.analysis_id' /tmp/perizia_analysis_response.json)

curl -sS "$API_URL/api/history/perizia/$ANALYSIS_ID" \
  -H "Authorization: Bearer $SESSION_TOKEN" | jq . > /tmp/perizia_analysis_record.json

curl -sS "$API_URL/api/analysis/perizia/$ANALYSIS_ID/pdf" \
  -H "Authorization: Bearer $SESSION_TOKEN" \
  -o /tmp/nexodify_report_${ANALYSIS_ID}.html
```

Notes:
- If you have a real Emergent Auth `session_id`, you can call `/api/auth/session` to get a cookie. Without that, the bearer-token path above is the only non-browser flow supported by this code.
