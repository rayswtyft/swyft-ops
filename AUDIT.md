# Swyft-Ops Audit — 2026-07-03

Scope: `server.js` (3953 lines, Express monolith), `public/index.html` (6318-line SPA),
`public/sw.js`, `database.js`, `migrate-db.js`, `sync-inventory.js`. Postgres-backed
(`memoryDb` in-memory mirror). Business runs US-Eastern; server assumed UTC (Render default).

Method: four parallel read-only passes (backend security, money/QuickBooks, date-tz/data-integrity/recent-commits, frontend), then hand-verification of every CRITICAL/HIGH finding against the actual source and schema. Every finding below was confirmed by reading real code — file:line cited.

## VERDICT: NOT CLEAN

Two CRITICAL and five HIGH findings are confirmed. The dominant issue is that **the entire API is unauthenticated** — anyone who can reach the port can push invoices into the live QuickBooks company, overwrite the Google Drive OAuth token, dump employee GPS/PII, and clock any employee in/out. Independently, a **timezone bug** mis-dates evening work and a **silently-swallowed SQL error** loses service end-times on every finish-day. Do not treat this codebase as safe to expose without addressing C-001 first.

---

## Findings (most severe first)

### [C-001] CRITICAL — No authentication or authorization on any endpoint
`server.js:151-168` (middleware chain is only `express.json` + static + `morgan` + a `writeLimiter`). No session, JWT, API key, cookie, or role check exists anywhere; there is no admin/user/van role concept in this codebase at all. Every one of the 85 routes is fully public. Highest-impact open routes:
- `POST /admin/drive-token` (`server.js:2966`) — any anonymous caller overwrites the Google Drive OAuth refresh/access token (persisted + `process.env`), hijacking or breaking all photo storage.
- `POST /qb/invoice`, `POST /qb/send-day`, `GET /qb/connect` (`server.js:3606, 3635, 3472`) — anyone can push invoices into the real QuickBooks company or mass-fire daily invoicing.
- `POST /admin/reload-db`, `GET/POST /admin/backfill-*` (`server.js:2985, 3857, 3881, 3927`) — open; the address backfill also burns Google Maps quota on demand.
- `GET /reports/weekly-payroll`, `/reports/weekly-activity`, `/time-clock`, `/contractors` (`server.js:2062, 2171, 3761, 2274`) — dump employee names, hours, precise clock-in/out **GPS coordinates**, and contractor emails/phones/addresses to any anonymous GET.

Scenario: `curl -X POST host/qb/send-day` → real invoices generated in QuickBooks by an unauthenticated attacker. This is the NOT-CLEAN driver.
Fix: add an auth gate (session/token middleware) mounted before the route handlers; at minimum protect `/admin/*`, `/qb/*`, `/reports/*`, `/time-clock`, and all mutating verbs.

### [C-002] CRITICAL — `todayString()` returns UTC date → wrong calendar day every evening ET
`server.js:321-322`: `return new Date().toISOString().slice(0, 10);`. On a UTC server this rolls to the next day at 00:00 UTC = **8:00pm EDT / 7:00pm EST**. From ~8pm ET to midnight ET it returns *tomorrow's* date. It is the fallback date across the app:
- Clock-in date bucket (`server.js:3811`): an evening clock-in with no `dailySetup.date` files the timecard under tomorrow → payroll grouped on the wrong day.
- Job creation (`server.js:2375`, `/jobs/quick-dump` `2425`): an evening-created job lands on tomorrow's board.
- Finish-day / daily-invoice date (`server.js:3272`, and `dailySetup.date || todayString()` fallbacks at 1871, 1911, 1919, 1966, 1996, 2013, 2348, 2355, 3244, 3596, 3637).

Mitigation present: most paths prefer `db.dailySetup.date` (correct when Daily Setup was run). The bug bites whenever that is unset/stale. Fix: derive the calendar day in America/New_York (e.g. `Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York'})`) in `todayString()`.

### [C-003] HIGH — finish-day persists to non-existent columns/tables → service end-times & clock-outs silently lost
`server.js:3296-3300`: `UPDATE jobs SET services=$1, finished_at=$2 WHERE id=$3`. The `jobs` table (schema `migrate-db.js:22-37`) has **no `services` column** — services live in `job_services`. The UPDATE throws `column "services" does not exist` and is swallowed by the surrounding try/catch. Same defect at `server.js:3328`: `UPDATE time_entries SET clock_out=$1 ... WHERE ... date=$2` — no `time_entries` table and no `date` column exist (real table is `time_clock_entries`, column `entry_date`, per `migrate-db.js:118-127`).
Effect: when finish-day force-closes running services (`s.endTime = now`, `3288`) and auto-clocks-out employees, **none of it persists to Postgres** — it lives only in `memoryDb`. On any Postgres reload the job is archived (that bulk UPDATE at `3319` uses the real `archived_at` column and succeeds) but the service `endTime`s and clock-outs are gone, corrupting the hours/payroll/invoice math that depends on `endTime - startTime`.
Fix: write endTimes via the `job_services` upsert path; clock out via `time_clock_entries`/`entry_date`. Also: never leave a silent `catch` on a write whose failure loses data.

### [C-004] HIGH — Stored XSS: server-rendered invoice + frontend innerHTML with no escaping
No HTML-escaping helper exists anywhere in the frontend, and free-text fields (`serviceAddress`, `notes`, `contractorName`, `employee.name`, photo `caption`/`tag`) are user-controlled at creation and rendered raw.
- Server: `GET /invoice/:id` (`server.js:3351-3455`) interpolates `serviceAddress`, contractor company/contact/email/phone, and photo tag/caption directly into HTML (e.g. `3410-3420, 3445`).
- Frontend `innerHTML` sinks (all raw): `renderStandbyList` (`index.html:3442-3457` — also breaks out of a single-quoted `onclick='scheduleStandbyJob('${j.id}')'`), `renderJobList` (`3648-3652`), `renderSelectedJob` (`3761-3826`, incl. `job.notes` at `3788`), `loadEstimates` (`2655-2657`), `loadHistory` (`2697`), employee lists (`5840-5846`, `value="${e.name}"` attribute break-out).

Scenario: create a job with `serviceAddress = "<img src=x onerror=fetch('/admin/drive-token',{method:'POST',...})>"`; it executes in staff's authenticated context (which, per C-001, can do anything) whenever they open the board or invoice. Fix: add an `escapeHtml()` helper, wrap every interpolated server string, and escape ids used inside inline `on*='...'` handlers.

### [C-005] HIGH — QuickBooks DocNumber generation produces duplicate / wrong invoice numbers
`server.js:1656-1674`. Reads QB `SELECT DocNumber FROM Invoice ORDER BY DocNumber DESC MAXRESULTS 1` then posts `max(lastNum+1, 2000)`. Three defects:
1. **Race / read-after-write:** no lock or reservation. `/qb/invoice` and the `/qb/send-day` batch loop can overlap, and even the sequential loop re-queries QB immediately after each POST — QB's query index is eventually consistent, so invoice N may not be indexed yet and N gets regenerated. This is the exact failure the commit meant to fix, moved from the local counter onto QB's index.
2. **Lexical string sort** (`server.js:1662`): QB sorts `DocNumber` as text. At the 4→5 digit boundary `"9999" > "10000"` lexically, so the query returns `9999` forever → generation permanently stuck/duplicating at 10000. Any legacy invoice with a higher leading digit (e.g. `"9500"`) also wins the sort and skews the max.
3. **Fallback collides** (`server.js:1670-1673`): on query error, `docNumber = 2000 + sentJobs.length` — `sentJobs.length` has no relation to real QB DocNumbers (deleted/externally-invoiced jobs desync it), so it can emit a number that already exists.

Fix: fetch enough rows and sort numerically client-side; serialize invoice creation (mutex/DB sequence); on QB query failure, fail loudly rather than guess a low number.

### [C-006] HIGH — Double-submit races on money/job actions (duplicate jobs, double QB invoice send)
No primary action handler disables its button or guards re-entry while the request is in flight (the inventory save at `index.html:2447` *does* — proof the pattern was known but not applied): `createJob` (`3500-3555`), `createEstimate` (`3559-3605`), `confirmFinishDay` (`5347-5370`), `confirmQuickBooksSend` (`5460-5492` — **double invoice send to QuickBooks**), `clockInEmployee` (`5735-5757` — guard is only a stale client check), `convertEstimate` (`3609-3625`), `jobAction` finish (`4469-4484`). On mobile (the UI's target) a double-tap fires the POST twice. Compounded by [C-007]: `createJob`/`createEstimate` never check `res.ok` (`3532-3555`, `3569-3603`) — on a 4xx/5xx they still clear the form, close the modal, and alert success, so the user is told it worked and loses their input. Fix: disable-in-flight + check `res.ok`/`data.error` before success UI.

### [C-007] HIGH — Multer upload has no size or type limits
`server.js:180` (`multer({ storage })`, used at `2858`): no `limits`, no `fileFilter`. The 256kb `express.json` cap does not apply to multipart. Two impacts: (1) unbounded file size → disk-exhaustion DoS; (2) the saved filename keeps the attacker's extension (`server.js:175`) and is served from `/uploads` static (`server.js:154`), so uploading `x.html` yields a same-origin `/uploads/<ts>.html` — stored content executing on the app origin. Fix: set `limits: { fileSize }`, a `fileFilter` allowlisting image/video MIME types, and force a safe extension.

---

### [C-008] MEDIUM — contractorName fallback is dead code; null `contractorId` job cannot be invoiced
`server.js:1570-1576`. The fallback added by commit 56b0220 never runs: (a) jobs have no `contractorName` field (schema `migrate-db.js:22`; grep finds no assignment) so `if (!contractor && job.contractorName)` is always false; (b) it also compares `c.name`, but contractors only have `companyName`/`contactName` (`c.name` is always undefined). A job with null `contractorId` therefore throws at `server.js:1576` and cannot be invoiced. If repaired to match on `companyName`, note the `find` returns the *first* match, so two contractors sharing a name would bill the wrong QB customer (amounts unaffected — rates are flat by category).

### [C-009] MEDIUM — Standby / reschedule never assigns `sortOrder` → day-board ordering collides at 0
Standby jobs are created with `sortOrder: 0` (`server.js:2390`). Scheduling them (`POST /jobs/:id/reschedule`, `server.js:2630-2659`) sets `serviceDate` but never recomputes `sortOrder` (unlike `PUT /jobs` which calls `nextSortOrderForDate` at `2473`). All jobs scheduled from standby onto a day tie at `sortOrder=0`; the tie-break `Number(a.id)-Number(b.id)` is a no-op because ids are UUIDs (`Number(uuid)=NaN`), so their order is nondeterministic until a manual reorder. Same root cause makes a date→date reschedule collide with a job already on the target day. Fix: call `nextSortOrderForDate` in the reschedule path.

### [C-010] MEDIUM — Drive-hosted videos now render as images (regression from commit 0d33934)
`public/index.html:4064, 4095`: `isVideo = /\.(mp4|mov|avi|webm|hevc)/i.test(p.url)`. Drive uploads store `photo.url` as a `webViewLink` like `https://drive.google.com/file/d/FILE_ID/view` — **no file extension** (`server.js:2915-2917`), so the regex never matches and real Drive videos are rendered via `<img>`. The commit fixed photos-shown-as-video but inverted it for videos. Fix: detect by stored MIME/type, not URL extension.

### [C-011] MEDIUM — Concurrent writes lose in-memory updates (no locking/transactions)
The write pattern is `readDb()` deep-clone → mutate → per-entity SQL → `memoryDb = db` whole-object reassign (e.g. `server.js:2412, 2447, 2938, 3817`). Two requests interleaving across their awaits each start from a stale clone; the last `memoryDb = db` wins, dropping the other's change from the API/socket view until a Postgres reload heals it. Multi-statement writes (`syncCrewSizeFromClockIns`, `server.js:3784-3794`) are not wrapped in a transaction. Fix: serialize db mutations or apply writes to the live `memoryDb` rather than a pre-read clone.

### [C-012] MEDIUM — `/admin/backfill-addresses` calls undefined `loadDb()` → always 500s
`server.js:3946`: `await loadDb();` — the function is `loadDbFromPostgres` (`server.js:507`; the sibling `/admin/backfill-photo-ids` uses it correctly at `3919`). Geocode UPDATEs commit inside the loop, then this ReferenceError throws (caught at `3948`) → HTTP 500 and `memoryDb` never reloads, so results don't show until a manual reload. Same class as commit 851fa0a, missed here.

### [C-013] MEDIUM — Secrets exposed to unauthenticated clients
`GET /maps-config` (`server.js:1731-1733`) returns `GOOGLE_MAPS_API_KEY` to anyone (billable if not referrer/IP-restricted). Hardcoded Drive folder id (`server.js:32`) is exposed via `/admin/drive-status` (`2987`) and folders are shared public (`driveSetPublic`, role anyone, `92-98`), so job photos sit at guessable public Drive URLs. QuickBooks tokens are *not* leaked (only `realmId`) — that part is OK.

### [C-014] LOW-MED — QB concrete line `Qty × UnitPrice ≠ Amount`
`server.js:1622-1624`: `UnitPrice = (amt / max(linearFeet,1)).toFixed(2)`, `Qty = linearFeet`, but `Amount = amt` is sent explicitly and QB treats it as authoritative. Example: `amt=100, linearFeet=3` → UnitPrice `33.33 × 3 = 99.99 ≠ 100`. Invoice total stays correct; only the displayed unit price reconciles wrong. Cosmetic.

### [C-015] LOW — Assorted
Evening-ET "tomorrow" reschedule default is +2 days via `toISOString()` (`index.html:5147-5149`, manual pick unaffected); weekly-payroll "current week" uses server-UTC date (`server.js:2087, 2188`, reporting-only); `res.status(500).json({error: err.message})` and `/qb/invoice` returning `e.response?.data` leak internal/upstream error text; async photo handlers (`server.js:2858, 2943`) lack an outer try/catch and there is no global Express error middleware.

---

## Verified clean (silence is provable)

| Area | Result |
|---|---|
| SQL injection (Postgres layer) | **Clean** — every `query()` uses parameterized `$1/$2`; the only string-interpolated SQL (`insertServiceRow`/`upsertJob`, `server.js:660,721,731`) interpolates hardcoded table/column constants, never user input. |
| Core per-service money math | **Clean** — every total rounds to cents: `materialsBreakdown` (`967`), `materialsTotal` (`989`), `calcBaseServiceTotal` (`1010-1024`), `calcServiceTotal` (`1027`), `hydrateJob.totalCost` (`1104`). No un-rounded float reaches a stored/sent amount. |
| `$45` cleaning-supplies gating (commit e933978) | **Verified good** — `cleaningSuppliesAutoCharge` (`server.js:935-939`) exact-matches trimmed, lowercased subtype via `===` against exactly "Deep clean" / "Post construction cleanup". No substring/case bug. |
| Frontend money | **Clean** — `fmtMoney` only formats backend-provided totals; no authoritative money is computed client-side. |
| Service worker | **Clean** — `public/sw.js` is a no-op (empty `fetch` handler, caches nothing); no stale-data risk. |
| `move-to-standby`, `/jobs/reorder`, `addDays`, optional-field access | **Clean** — reorder maps index→sortOrder consistently and scoped to sent ids; `addDays` is UTC-stable; `hydrateJob`/`normalizeDbShape` default arrays so common optional paths don't throw. |

## Recommended fix order
1. **C-001** auth gate (blocks everything).
2. **C-002** timezone in `todayString()`; **C-003** finish-day writes to real tables/columns.
3. **C-004** escapeHtml; **C-005** serialize + numeric-sort DocNumber; **C-006/C-007** disable-in-flight + res.ok checks + multer limits.
4. Medium/low as scheduled.
