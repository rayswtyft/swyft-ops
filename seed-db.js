require("dotenv").config();

const fs = require("fs");
const path = require("path");
const { pool } = require("./database");

const DB_FILE = path.join(__dirname, "db.json");

function todayString() {
  return new Date().toISOString().slice(0, 10);
}

function defaultDb() {
  return {
    contractors: [],
    jobs: [],
    estimates: [],
    inventory: [],
    employees: [{ id: 1, name: "Employee 1", active: true }],
    timeClockEntries: [],
    settings: {
      inventoryLink: "",
      quickbooks: {
        connected: false,
        realmId: null,
        accessToken: null,
        refreshToken: null,
        expiresAt: null,
        refreshExpiresAt: null,
        lastConnectedAt: null
      }
    },
    dailySetup: {
      date: todayString(),
      crewSize: 1,
      lunchBreaks: [],
      activeLunchStart: null,
      dailyChecklistState: {}
    }
  };
}

async function insertServiceRow(client, table, parentColumn, parentId, s, index) {
  await client.query(
    `INSERT INTO ${table} (${parentColumn}, service_index, category, subtype, crew_size, hours_manual, linear_feet, junk_load, junk_price, service_date, on_my_way_time, arrived_time, start_time, end_time, materials_used, inventory_deducted_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
    [
      parentId,
      index,
      s.category || "Cleaning",
      s.subtype || "General cleaning",
      Number(s.crewSize || 1),
      Number(s.hoursManual || 0),
      Number(s.linearFeet || 0),
      s.junkLoad || "Quarter load",
      Number(s.junkPrice || 175),
      s.serviceDate || null,
      s.onMyWayTime || null,
      s.arrivedTime || null,
      s.startTime || null,
      s.endTime || null,
      s.materialsUsed || {},
      s.inventoryDeductedAt || null
    ]
  );
}

async function seed() {
  if (!fs.existsSync(DB_FILE)) {
    throw new Error("db.json not found. Place your current db.json in the project root before seeding.");
  }

  const db = { ...defaultDb(), ...JSON.parse(fs.readFileSync(DB_FILE, "utf8")) };
  db.settings ||= defaultDb().settings;
  db.settings.quickbooks ||= defaultDb().settings.quickbooks;
  db.dailySetup ||= defaultDb().dailySetup;

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const tables = [
      "contractor_addresses",
      "job_photos",
      "job_services",
      "estimate_services",
      "jobs",
      "estimates",
      "contractors",
      "inventory",
      "employees",
      "time_clock_entries",
      "daily_checklist_state",
      "daily_setup",
      "settings",
      "quickbooks_tokens"
    ];

    for (const table of tables) {
      await client.query(`DELETE FROM ${table}`);
    }

    for (const c of db.contractors || []) {
      await client.query(
        `INSERT INTO contractors (id, company_name, contact_name, email, phone, billing_address, payment_terms, created_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
        [
          String(c.id),
          c.companyName || "",
          c.contactName || "",
          c.email || "",
          c.phone || "",
          c.billingAddress || "",
          c.paymentTerms || "",
          c.createdAt || new Date().toISOString()
        ]
      );

      for (const addr of c.serviceAddresses || []) {
        await client.query(
          `INSERT INTO contractor_addresses (contractor_id, address) VALUES ($1,$2)`,
          [String(c.id), addr]
        );
      }
    }

    for (const item of db.inventory || []) {
      await client.query(
        `INSERT INTO inventory (id, item_key, name, quantity, unit, reorder_point, active, display)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
        [
          String(item.id || item.key),
          item.key || String(item.id),
          item.name || "",
          item.quantity,
          item.unit || "",
          item.reorderPoint,
          item.active !== false,
          item.display || null
        ]
      );
    }

    for (const job of db.jobs || []) {
      await client.query(
        `INSERT INTO jobs (id, contractor_id, service_address, service_date, notes, created_at, sort_order, deleted_at, archived_at, finished_at, quickbooks_status, quickbooks_invoice_id, from_estimate_id, open_status)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
        [
          String(job.id),
          job.contractorId == null ? null : String(job.contractorId),
          job.serviceAddress || "",
          job.serviceDate || todayString(),
          job.notes || "",
          job.createdAt || new Date().toISOString(),
          Number(job.sortOrder || 0),
          job.deletedAt || null,
          job.archivedAt || null,
          job.finishedAt || null,
          job.quickbooksStatus || "not_sent",
          job.quickbooksInvoiceId || null,
          job.fromEstimateId == null ? null : String(job.fromEstimateId),
          job.openStatus || "single_day"
        ]
      );

      let index = 0;
      for (const s of job.services || []) {
        await insertServiceRow(client, "job_services", "job_id", String(job.id), s, index++);
      }

      for (const p of job.photos || []) {
        await client.query(
          `INSERT INTO job_photos (id, job_id, url, tag, caption, created_at)
           VALUES ($1,$2,$3,$4,$5,$6)`,
          [
            String(p.id),
            String(job.id),
            p.url || "",
            p.tag || "before",
            p.caption || "",
            p.createdAt || new Date().toISOString()
          ]
        );
      }
    }

    for (const est of db.estimates || []) {
      await client.query(
        `INSERT INTO estimates (id, contractor_id, service_address, notes, status, created_at, converted_job_id)
         VALUES ($1,$2,$3,$4,$5,$6,$7)`,
        [
          String(est.id),
          est.contractorId == null ? null : String(est.contractorId),
          est.serviceAddress || "",
          est.notes || "",
          est.status || "open",
          est.createdAt || new Date().toISOString(),
          est.convertedJobId == null ? null : String(est.convertedJobId)
        ]
      );

      let index = 0;
      for (const s of est.services || []) {
        await insertServiceRow(client, "estimate_services", "estimate_id", String(est.id), s, index++);
      }
    }

    for (const e of db.employees || [{ id: 1, name: "Employee 1", active: true }]) {
      await client.query(
        `INSERT INTO employees (id, name, active, created_at) VALUES ($1,$2,$3,$4)`,
        [String(e.id), e.name || "Unnamed Employee", e.active !== false, e.createdAt || new Date().toISOString()]
      );
    }

    for (const t of db.timeClockEntries || []) {
      await client.query(
        `INSERT INTO time_clock_entries (id, employee_id, employee_name, clock_in, clock_out, minutes, entry_date)
         VALUES ($1,$2,$3,$4,$5,$6,$7)`,
        [
          String(t.id),
          t.employeeId == null ? null : String(t.employeeId),
          t.name || t.employeeName || "",
          t.clockIn || new Date().toISOString(),
          t.clockOut || null,
          t.minutes == null ? null : Number(t.minutes),
          t.date || todayString()
        ]
      );
    }

    await client.query(
      `INSERT INTO daily_setup (id, current_date, crew_size, lunch_breaks, active_lunch_start)
       VALUES (1,$1,$2,$3,$4)`,
      [
        db.dailySetup.date || todayString(),
        Number(db.dailySetup.crewSize || 1),
        db.dailySetup.lunchBreaks || [],
        db.dailySetup.activeLunchStart || null
      ]
    );

    for (const [date, state] of Object.entries(db.dailySetup.dailyChecklistState || {})) {
      for (const [item, checked] of Object.entries(state || {})) {
        await client.query(
          `INSERT INTO daily_checklist_state (check_date, item, checked) VALUES ($1,$2,$3)`,
          [date, item, !!checked]
        );
      }
    }

    await client.query(
      `INSERT INTO settings (key, value) VALUES ($1,$2)`,
      ["inventoryLink", { value: db.settings.inventoryLink || "" }]
    );

    const qb = db.settings.quickbooks || {};
    await client.query(
      `INSERT INTO quickbooks_tokens (id, connected, realm_id, access_token, refresh_token, expires_at, refresh_expires_at, last_connected_at)
       VALUES (1,$1,$2,$3,$4,$5,$6,$7)`,
      [
        !!qb.connected,
        qb.realmId || null,
        qb.accessToken || null,
        qb.refreshToken || null,
        qb.expiresAt || null,
        qb.refreshExpiresAt || null,
        qb.lastConnectedAt || null
      ]
    );

    await client.query("COMMIT");
    console.log("Seed complete.");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
    await pool.end();
  }
}

seed().catch(err => {
  console.error("Seed failed:", err);
  process.exit(1);
});
