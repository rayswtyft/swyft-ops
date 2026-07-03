# Swyft-Ops Audit — Backend + Van App — 2026-07-03

Scope: full read-only audit of the backend (`server.js`, 3953 lines; `database.js`; `sync-inventory.js`) and **every part of the Van App** (the field-crew mode inside `public/index.html`, entered via `enterVan()` / `accessRole==="van"`). Postgres-backed with an in-memory `memoryDb` mirror. Business runs US-Eastern; server assumed UTC (Render default).

**Authoritative schema note:** the schema that actually runs is `migrateDatabase()` in `server.js:451-505` (executed at boot). The separate `migrate-db.js` file is a **stale duplicate that is never invoked** — several `server.js` writes target columns/tables that exist in neither, and fail silently. All schema citations below reference the runtime schema in `server.js`.

Method: seven parallel read-only passes over two sessions (backend security, money/QuickBooks, date-tz/data-integrity, main frontend, van-app frontend, van-facing backend, backend completeness sweep). Every CRITICAL/HIGH finding was hand-verified against source and the runtime schema — file:line cited.

## VERDICT: NOT CLEAN

Four CRITICAL and nine HIGH findings confirmed. Three themes dominate:
1. **No security boundary** — the API has zero authentication, and the Van App's role is a browser-flippable `localStorage` flag, so any field user can become Manager (QB send, delete, invoicing) with a one-line console command and no PIN.
2. **Silent SQL failures** — three separate writes (finish-day service close, finish-day clock-out, daily-setup save) and one INSERT (estimate services) target non-existent columns or have parameter-count mismatches. Each throws and is swallowed by a bare `catch`, so **finishing the day, saving the crew roster, and creating any estimate with services all silently fail to persist**.
3. **Timezone** — evening-ET work is mis-dated to the next day.

---

## CRITICAL

### [C-001] No authentication or authorization on any endpoint
`server.js:151-168` — the middleware chain is only `express.json` + static + `morgan` + a `writeLimiter`. No session/JWT/API-key/role check exists anywhere. All ~85 routes are fully public: `POST /qb/invoice` and `/qb/send-day` (`3606, 3635`) push invoices into the live QuickBooks company; `POST /admin/drive-token` (`2966`) overwrites the Google Drive OAuth token; `/reports/weekly-payroll`, `/time-clock`, `/contractors` (`2062, 3761, 2274`) dump employee GPS + PII. Scenario: `curl -X POST host/qb/send-day` mass-generates real invoices, unauthenticated. Fix: add auth middleware before the route handlers, at minimum on `/admin/*`, `/qb/*`, `/reports/*`, `/time-clock`, and all mutating verbs.

### [C-002] Van role is a self-service `localStorage` flag → privilege escalation to Manager with no PIN
`public/index.html:1645` (`enterVan` sets `localStorage.swyftAccessRole="van"`); on reload `init()` (`5931-5943`) reads the flag back and calls `finishLogin` with **no PIN re-check** (`MANAGER_PIN` is verified only once, in `submitManagerPin` at `1661`). A field worker on the shared iPad can run in the console:
```js
localStorage.setItem("swyftAccessRole","manager");
localStorage.setItem("swyftSelectedApp","manager"); location.reload();
```
and land in the full Manager App. The in-session `setMode` gate (`1773`, `if (accessRole==="van" && mode!=="van") return`) is bypassed because `accessRole` is now `"manager"`, and every sensitive handler (`confirmQuickBooksSend` `5460`, `deleteJob` `5260`, `confirmFinishDay` `5347`) is a plain `fetch` with no server-side role check (per C-001). Fix: real server-side authz; do not treat client role as trusted.

### [C-003] `estimate_services` INSERT has 18 columns but 17 placeholders → every estimate/conversion with services 500s
`server.js:771-775` (`upsertEstimate`): the column list has 18 names, `VALUES ($1..$17)` supplies 17 placeholders, and the params array passes 18 values. Postgres rejects with *"INSERT has more target columns than expressions."* (Contrast the correct sibling `upsertJob` at `731-734` using `$1..$18`.) Effect: `POST /estimates` with any service throws (the header row was already inserted at `762`, leaving an orphan estimate); `POST /estimates/:id/convert` re-runs `upsertEstimate` and throws **before** `upsertJob` runs (`3219`), so the converted job never persists either. Estimates with zero services are unaffected. Fix: add the 18th placeholder (`$18` for `trench_data`).

### [C-004] `todayString()` returns a UTC date → wrong calendar day every evening ET
`server.js:321-323`: `return new Date().toISOString().slice(0,10)`. On a UTC server this rolls at 00:00 UTC = **8pm EDT / 7pm EST**, so from ~8pm ET to midnight it returns *tomorrow*. It is the fallback date for clock-in `entry_date` (`3811`), job creation (`2375`, `/jobs/quick-dump` `2425`), finish-day/daily-invoice date (`3272`), and many `dailySetup.date || todayString()` sites. Mitigated where `dailySetup.date` is set, but that write itself fails (see C-006). Fix: compute the calendar day in `America/New_York` (`Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York'})`).

---

## HIGH

### [C-005] finish-day writes to a non-existent `jobs.services` column → service end-times & `finished_at` never persist
`server.js:3296-3300`: `UPDATE jobs SET services=$1, finished_at=$2 WHERE id=$3`. The `jobs` table (`server.js:453`) has **no `services` column** (services live in `job_services`). Throws, swallowed at `3300`. Finish-day sets `s.endTime=now` and `finishedAt=now` in memory only; nothing persists. The bulk `archived_at` UPDATE (`3319`) uses a real column and succeeds, so on any Postgres reload the job is archived but its service end-times are lost, corrupting hours/payroll/invoice math. (Single-job `/jobs/:id/finish` at `2580` correctly routes through `upsertJob`→`job_services` and is fine — only the bulk finish-day path is broken.)

### [C-006] finish-day auto clock-out targets a non-existent table/column → crews never auto-clock-out
`server.js:3327-3330`: `UPDATE time_entries SET clock_out=$1 WHERE clock_out IS NULL AND date=$2`. The real table is `time_clock_entries` and the column is `entry_date` (`server.js:465`). Throws, swallowed at `3331`. Auto clock-out never happens in DB or memory, so crews stay clocked in indefinitely after finish-day and payroll/crew-count stay wrong. Fix: `UPDATE time_clock_entries SET clock_out=$1 WHERE clock_out IS NULL AND entry_date=$2`.

### [C-007] daily-setup INSERT binds 7 params for 5 placeholders → crew roster & crew size never persist
`server.js:1820-1825`: `INSERT INTO daily_setup (... ) VALUES (1,$1,$2,$3,$4,$5)` but the params array passes **7 values** (adds `JSON.stringify(dumpRuns)` and `activeDumpStart` with no matching placeholder). Postgres rejects the Bind (`08P01: supplies 7 parameters, requires 5`), swallowed at `1826`. The manager's daily setup — crew size, lunch breaks, and the **`assignedEmployeeIds` roster** — is written to `memoryDb` but never to Postgres. On restart the roster is empty, which also silently disables the clock-in roster gate (see C-009). Fix: match placeholders to params (and `JSON.stringify` the jsonb arrays).

### [C-008] Stored XSS: server-rendered invoice + pervasive frontend `innerHTML` with no escaping
No HTML-escaping helper exists in the frontend. User-controlled fields (`serviceAddress`, `notes`, `contractorName`, `employee.name`, photo caption) are rendered raw: server `GET /invoice/:id` (`server.js:3351-3455`) and frontend sinks `renderStandbyList` (`index.html:3442-3457`, id also breaks out of a single-quoted `onclick`), `renderSelectedJob` (`3761-3826`, incl. `job.notes` `3788`), `renderJobList` (`3648-3652`), employee lists (`5840-5846`). Scenario: a job with `serviceAddress="<img src=x onerror=...>"` executes in staff's context whenever the board/invoice is opened — which, per C-001/C-002, can do anything. Fix: `escapeHtml()` around every interpolated server string; escape ids in inline handlers.

### [C-009] Clock-in fully trusts client `employeeId`; roster gate is bypassable
`server.js:3800-3811`: `employeeId` comes straight from `req.body` with no PIN/auth. Anyone can clock any active employee in/out (payroll fraud). The roster gate (`3804-3807`) only enforces when `assignedEmployeeIds.length>0`, but that list fails to persist (C-007), so after any restart it's empty and the gate is a no-op. The PIN removed by commit 96825db was client-side only (`prompt()` in index.html) — server-side identity was never enforced. (Duplicate-clock-in prevention at `3809` and clock-out pairing at `3832` are correct.)

### [C-010] Field clock-in/out silently rewrites billing crew size on all of today's jobs
`syncCrewSizeFromClockIns` (`server.js:3771-3789`), called from clock-in (`3816`) and clock-out (`3840`), sets `crewSize=max(1,#clocked-in)` and `UPDATE job_services SET crew_size=$1` for every service on every non-archived job dated today. `crew_size` is a billing input (labor = hours × crew_size × rate), so a field worker clocking in/out **overwrites the manager's per-job crew sizes** and fights the manager's daily-setup write — last actor wins. Example: a 4-hr cleaning service billed at crew 3 ($384) becomes crew 2 ($256) when someone clocks out. Fix: don't let field clock events mutate billing crew size on already-configured/completed services.

### [C-011] QuickBooks DocNumber generation produces duplicate/wrong invoice numbers
`server.js:1656-1674`. (1) **Race:** no lock/reservation — `/qb/invoice` and the `/qb/send-day` batch can overlap and read the same `lastNum`; even sequential sends re-query QB's eventually-consistent index and regenerate N. (2) **Lexical string sort** (`1662`, `ORDER BY DocNumber DESC`): at the 4→5-digit boundary `"9999">"10000"`, so generation sticks/duplicates at 10000; any legacy invoice with a higher leading digit skews the max. (3) **Fallback** (`1670-1673`): `2000 + sentJobs.length` has no relation to real QB numbers and can collide. Fix: fetch enough rows and sort numerically; serialize creation; fail loudly on query error.

### [C-012] Double-submit races on money/job actions (duplicate jobs, double QB send)
No primary handler disables its button or guards re-entry (the inventory save at `index.html:2447` does — proof the pattern was known): `createJob` (`3500`), `createEstimate` (`3559`), `confirmFinishDay` (`5347`), `confirmQuickBooksSend` (`5460`, **double invoice send**), `clockInEmployee` (`5735`), `jobAction` finish (`4469`). On the target iPad a double-tap fires the POST twice. Compounded: `createJob`/`createEstimate` never check `res.ok` (`3532-3555`, `3569-3603`) — on a 4xx/5xx they still clear the form and alert success, losing the user's input. Fix: disable-in-flight + check `res.ok`/`data.error` before success UI.

### [C-013] Van photo capture loses proof photos silently on bad signal
`public/index.html:4828` (`captureVanPhoto`, the primary van path) and `4792` (`uploadPhoto`) POST with **no `res.ok` check and no try/catch** (`await fetch(.../photos,{method:"POST",body:fd})`). In the truck with weak signal the fetch rejects as an unhandled promise in an async `onchange` — the before/after photo is gone, `refreshAll` never runs, and the worker gets **no error**. `uploadVanPhoto` (`4871`) does catch network rejects but never checks `resp.ok`, so a 500/413 returning JSON without `driveUrl` still shows "✅ Uploaded!". Fix: check `res.ok`, surface failures, and queue/retry offline.

### [C-014] Multer upload has no size or type limits
`server.js:180` (`multer({ storage })`, used at `2858`): no `limits`, no `fileFilter`; the 256kb JSON cap doesn't apply to multipart. Unbounded size → disk/memory exhaustion (worsened by `readFileSync` of the whole file at `2912` and MOV→MP4 conversion), and the saved filename keeps the attacker's extension (`175`) served from `/uploads` static (`154`) — uploading `x.html` yields a same-origin executable URL. Fix: `limits.fileSize` + MIME `fileFilter` + forced safe extension.

---

## MEDIUM

### [C-015] Inventory auto-deduction is a no-op and is never persisted anyway
`server.js:1287-1314`. The deduction map gives a non-null `inventoryKey` only for `contractor_bags` → hardcoded `"contractor_bags_stock"`, but `sync-inventory.js` generates `item_key` `"contractor_bags"` (no `_stock`), so `db.inventory.find(i=>i.key==="contractor_bags_stock")` returns undefined and nothing deducts; `zipper`/`ramboard`/`contractor_paper` have `inventoryKey:null` and never deduct. Even when it matches, finish-day (`3308-3311`) mutates `item.quantity` **in memory only** — no `upsertInventory*` call — so decrements vanish on reload. Net: Postgres inventory never goes down from job usage. Fix: correct the key and persist the decrement.

### [C-016] Job status transitions have no ordering or archived/deleted guard
`/jobs/:id/on-my-way` (`2536`), `/arrived` (`2558`), `/finish` (`2580`) only check existence. No `archivedAt`/`deletedAt`/`finishedAt`/prior-step check, so you can `finish` a never-arrived job or fire transitions on an already-archived job, reopening it and rewriting its times via `upsertJob`. Fix: guard transitions on job state.

### [C-017] Van finish-prompt finishes the wrong job
`public/index.html:5194` (`maybePromptFinish`, fires on `visibilitychange`/`pagehide`) picks the target via `jobs.find(...)` — the **first** in-progress job, ignoring `selectedJobId` — then `confirmFinishFromPrompt` (`5222`) POSTs `/jobs/${id}/finish`. On a day with two simultaneous in-progress jobs, "Yes, Job is Done" finishes whichever sorts first, not necessarily the one on screen. Fix: target `selectedJobId`.

### [C-018] `contractorName` fallback is dead code; null-`contractorId` job can't be invoiced
`server.js:1570-1576`. The fallback never runs: jobs have no `contractorName` field, and it compares `c.name` which contractors don't have (`companyName`/`contactName`). A job with null `contractorId` throws at `1576` and cannot be invoiced. If repaired to match `companyName`, `find` returns the first match, so duplicate names would bill the wrong QB customer.

### [C-019] Standby / reschedule never assigns `sortOrder` → day-board ordering collides at 0
Standby jobs are created with `sortOrder:0` (`server.js:2390`); `POST /jobs/:id/reschedule` (`2630-2659`) sets `serviceDate` but never recomputes `sortOrder` (unlike `PUT /jobs` at `2473`). All standby-scheduled jobs tie at 0, and the UUID tie-break (`Number(uuid)=NaN`) is a no-op, so order is nondeterministic until a manual reorder. Fix: call `nextSortOrderForDate` in reschedule.

### [C-020] Drive-hosted videos render as images (regression from commit 0d33934)
`public/index.html:4064, 4095`: `isVideo = /\.(mp4|mov|avi|webm|hevc)/i.test(p.url)`. Drive `photo.url` is a `webViewLink` with **no extension** (`server.js:2915-2917`), so real Drive videos are rendered via `<img>`. Fix: detect by stored MIME/type.

### [C-021] Photo delete orphans the Google Drive file
`server.js:2943-2961` removes the DB row and `fs.unlink`s `photo.url`, but for Drive photos `photo.url` is an `https://` link so nothing is unlinked, and `photo.driveFileId` (which exists) is never used to delete from Drive. Every deleted photo leaves an orphaned, publicly-shared Drive file. Fix: delete via the Drive API using `driveFileId`.

### [C-022] Concurrent writes lose in-memory updates (no locking/transactions)
`readDb()` deep-clone → mutate → per-entity SQL → `memoryDb = db` whole-object reassign (`2412, 2447, 2938, 3817`). Interleaved requests each start from a stale clone; last assignment wins, dropping the other's change from the API/socket view until a Postgres reload heals it. Multi-statement writes (`syncCrewSizeFromClockIns`) aren't transactional. Fix: serialize mutations or apply to the live `memoryDb`.

### [C-023] `/admin/backfill-addresses` calls undefined `loadDb()` → always 500s
`server.js:3946`: `await loadDb()` — the function is `loadDbFromPostgres` (used correctly by the sibling at `3919`). Geocode UPDATEs commit, then this ReferenceError throws (caught `3948`) → HTTP 500, and `memoryDb` never reloads. Fix: `loadDbFromPostgres`.

### [C-024] Secrets exposed to unauthenticated clients
`GET /maps-config` (`server.js:1731-1733`) returns `GOOGLE_MAPS_API_KEY` to anyone (billable if unrestricted). Hardcoded Drive folder id (`32`) exposed via `/admin/drive-status` (`2987`), with folders shared public (`driveSetPublic`, `92-98`), so photos sit at guessable public URLs. (QB tokens are not leaked.)

### [C-025] Background poller can wipe in-progress form entry
The 30s `refreshAll` poller (`index.html:1751-1757`) re-renders the job panel. `loadDailySetup` guards crew inputs via `document.activeElement` (`1953-1962`), but the van materials inputs (`mat-*`, `4686`) and photo caption have no guard, so a poll landing mid-entry discards typed values. Fix: skip re-render of focused/edited subtrees.

---

## LOW

- **[C-026]** QB concrete line `Qty×UnitPrice ≠ Amount` (`server.js:1622-1624`): `amt=100, lf=3` → `33.33×3=99.99≠100`; `Amount:100` is authoritative so the total is fine — display-only.
- **[C-027]** `syncCrewSizeFromClockIns` retroactively recomputes billing for already-completed services (subset of C-010; flagged separately as it rewrites *historical* amounts).
- **[C-028]** `normalizeDbShape` (`server.js:404`) drops `pin`/`bio_credential` from in-memory employees and `loadDbFromPostgres` never maps `bio_credential`; Postgres stays authoritative so no data loss at rest, but in-memory consumers see `undefined`.
- **[C-029]** `weekBounds` (`2068`) doesn't validate the `week` param; garbage flows to `new Date()`→`toISOString()` `RangeError` caught by the outer try → returns 500 instead of the intended 400.
- **[C-030]** Evening-ET "tomorrow" reschedule default is +2 days via `toISOString()` (`index.html:5147-5149`, manual pick unaffected); weekly-payroll "current week" uses server-UTC date (`server.js:2087, 2188`).
- **[C-031]** Error-text leakage: `res.status(500).json({error: err.message})` pervasive; `/qb/invoice` returns `e.response?.data`; async photo handlers (`2858, 2943`) lack outer try/catch and there is no global Express error middleware.
- **[C-032]** Several van actions don't check `res.ok`: `confirmFinishFromPrompt` (`5222`), `deletePhoto` (`4901`), `archiveJob` (`5248`), `deleteJob` (`5260`) — failures show success and refresh as if they worked.

---

## Verified clean (silence is provable)

| Area | Result |
|---|---|
| SQL injection (Postgres) | **Clean** — all `query()` calls parameterized; the only interpolated SQL uses hardcoded table/column constants. |
| Core per-service money math | **Clean** — every total rounds to cents (`server.js:967, 989, 1010-1024, 1027, 1104`); no un-rounded float reaches an amount. |
| `$45` cleaning-supplies gating (commit e933978) | **Verified good** — `cleaningSuppliesAutoCharge` (`935-939`) exact-matches trimmed/lowercased subtype via `===` to exactly the two eligible services. |
| Reports math (weekly-payroll/activity) | **Clean** — `Math.round((out-in)/60000)`, hours `mins/60`, null-clock-out handled; ISO-week math correct for valid input. |
| Estimate field carry-over on convert | **Clean** — contractorId/address/notes/services map correctly; totals recomputed, not carried stale (only the SQL persistence bug C-003). |
| Inventory CRUD (adjust/move/add) | **Clean** — negative stock clamped (`3082, 3112`); no double-deduction path (only the auto-deduct no-op C-015). |
| Contractors / Employees CRUD | **Clean** — consistent; no contractor DELETE route (so no dangling `contractor_id`); soft-delete via `active=false`. |
| Duplicate clock-in prevention / clock-out pairing | **Clean** — open-entry check (`3809`); clock-out finds the single open entry, `ON CONFLICT` preserves `entry_date` (`3832, 671`). |
| Single-job / per-service status persistence | **Clean** — `/finish`, `/on-my-way`, `/arrived`, `/services/:index/*` persist via `upsertJob`→`job_services` (real columns). |
| Photo INSERT / PDF generation | **Clean** — `job_photos` columns (incl. `drive_file_id`) exist and target the right job; PDFs read pre-hydrated totals (no null crash). |
| Places proxy / Socket.IO | **Clean** — `/places-autocomplete` restricts to US addresses (no SSRF); the only socket handler is a connection ack — no `socket.on` mutations, no sensitive payloads. |
| Van date handling | **Clean** — `todayLocalDate()` (`index.html:1813-1817`) uses local `getFullYear/Month/Date`; van `currentDate()` forces it — no tz bug in the van job feed. |
| Service worker | **Clean** — `public/sw.js` is a no-op (empty fetch handler), caches nothing. |

## Recommended fix order
1. **C-001 / C-002** — establish a real auth + authz boundary (blocks everything else).
2. **C-003, C-005, C-006, C-007** — the four silent-persistence SQL bugs (estimates, finish-day close, finish-day clock-out, daily-setup roster). Add a lint/test that fails on swallowed write errors.
3. **C-004** timezone; **C-009 / C-010** clock-in identity + billing-crew overwrite.
4. **C-008** escapeHtml; **C-011** DocNumber; **C-012 / C-013 / C-014** double-submit + photo data-loss + upload limits.
5. Medium/low as scheduled.
