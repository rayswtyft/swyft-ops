require("dotenv").config();

const express = require("express");
const path = require("path");
const fs = require("fs");
const http = require("http");
const multer = require("multer");
const PDFDocument = require("pdfkit");
const archiver = require("archiver");
const axios = require("axios");
const qs = require("qs");
const { v4: uuidv4 } = require("uuid");
const { Server } = require("socket.io");
const { pool, query } = require("./database");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = process.env.PORT || 3000;
const PUBLIC_DIR = path.join(__dirname, "public");
const DB_FILE = path.join(__dirname, "db.json");
const UPLOADS_DIR = path.join(__dirname, "uploads");
const EXPORTS_DIR = path.join(__dirname, "exports");

if (!fs.existsSync(PUBLIC_DIR)) fs.mkdirSync(PUBLIC_DIR, { recursive: true });
if (!fs.existsSync(UPLOADS_DIR)) fs.mkdirSync(UPLOADS_DIR, { recursive: true });
if (!fs.existsSync(EXPORTS_DIR)) fs.mkdirSync(EXPORTS_DIR, { recursive: true });

app.use(express.json({ limit: "25mb" }));
app.use(express.urlencoded({ extended: true }));
app.use(express.static(PUBLIC_DIR));
app.use("/uploads", express.static(UPLOADS_DIR));
app.use("/exports", express.static(EXPORTS_DIR));

const storage = multer.diskStorage({
  destination: function (_req, _file, cb) {
    cb(null, UPLOADS_DIR);
  },
  filename: function (_req, file, cb) {
    const ext = path.extname(file.originalname || "") || ".jpg";
    cb(null, `${Date.now()}-${uuidv4()}${ext}`);
  }
});

const upload = multer({ storage });

function broadcastUpdate(type, payload = {}) {
  io.emit("swyft:update", {
    type,
    payload,
    at: new Date().toISOString()
  });
}

io.on("connection", socket => {
  socket.emit("swyft:connected", {
    ok: true,
    at: new Date().toISOString()
  });
});

const SERVICE_OPTIONS = {
  Cleaning: [
    "Between tenant cleaning",
    "Deep clean",
    "Post construction cleanup",
    "Site cleanup",
    "General cleaning",
    "Janitorial cleaning"
  ],
  Construction: [
    "Demo",
    "Concrete cutting",
    "Concrete fill with rebar",
    "Concrete fill without rebar"
  ],
  Labor: ["General labor"],
  Junk: ["Junk removal"]
};

const MATERIAL_DEFS = [
  { key: "contractor_bags", label: "Contractor bags", unitLabel: "qty", price: 1, wholeOnly: true },
  { key: "zipper", label: "Zipper", unitLabel: "qty", price: 12, wholeOnly: true },
  { key: "heavy_ramboard", label: "Heavy duty ramboard", unitLabel: "quarter rolls", price: 18.75, wholeOnly: false },
  { key: "medium_ramboard", label: "Medium duty ramboard", unitLabel: "quarter rolls", price: 13.75, wholeOnly: false },
  { key: "contractor_paper", label: "Contractor paper", unitLabel: "quarter rolls", price: 7.5, wholeOnly: false }
];

const CHECKLISTS = {
  "Cleaning|Between tenant cleaning": [
    "bleach powder", "box of latex gloves", "broom", "buckets", "carpet vacuum",
    "clean mop heads", "clorox bathroom spray", "contractor bags", "dish soap",
    "dish sponge", "dustpan", "floor cleaner", "garbage bags", "goo gone",
    "goof off", "green scrub pad", "magic erasers", "metal razors", "metal sponges",
    "mop sticks", "oven cleaner", "plastic razors", "rags", "rolls of paper towel",
    "scrub brush", "scrub daddy", "shoulder vacuum", "soap and water bottles",
    "squeegees", "toilet brush", "water pics"
  ],
  "Cleaning|Deep clean": [
    "bleach powder", "box of latex gloves", "broom", "buckets", "carpet vacuum",
    "clean mop heads", "clorox bathroom spray", "contractor bags", "dish soap",
    "dish sponge", "dustpan", "floor cleaner", "garbage bags", "goo gone",
    "goof off", "green scrub pad", "magic erasers", "metal razors", "metal sponges",
    "mop sticks", "oven cleaner", "plastic razors", "rags", "rolls of paper towel",
    "scrub brush", "scrub daddy", "shoulder vacuum", "soap and water bottles",
    "squeegees", "toilet brush", "water pics"
  ],
  "Cleaning|General cleaning": [
    "bleach powder", "box of latex gloves", "broom", "buckets", "carpet vacuum",
    "clean mop heads", "clorox bathroom spray", "contractor bags", "dish soap",
    "dish sponge", "dustpan", "floor cleaner", "garbage bags", "goo gone",
    "goof off", "green scrub pad", "magic erasers", "metal razors", "metal sponges",
    "mop sticks", "oven cleaner", "plastic razors", "rags", "rolls of paper towel",
    "scrub brush", "scrub daddy", "shoulder vacuum", "soap and water bottles",
    "squeegees", "toilet brush", "water pics"
  ],
  "Cleaning|Janitorial cleaning": [
    "bleach powder", "box of latex gloves", "broom", "buckets", "carpet vacuum",
    "clean mop heads", "clorox bathroom spray", "contractor bags", "dish soap",
    "dish sponge", "dustpan", "floor cleaner", "garbage bags", "goo gone",
    "goof off", "green scrub pad", "magic erasers", "metal razors", "metal sponges",
    "mop sticks", "oven cleaner", "plastic razors", "rags", "rolls of paper towel",
    "scrub brush", "scrub daddy", "shoulder vacuum", "soap and water bottles",
    "squeegees", "toilet brush", "water pics"
  ],
  "Cleaning|Post construction cleanup": ["construction broom", "contractor bags", "garbage bags", "shovel"],
  "Cleaning|Site cleanup": ["construction broom", "contractor bags", "garbage bags", "shovel"],
  "Construction|Demo": ["crowbars", "drill", "hammers", "multi tool", "recip saw"],
  "Construction|Concrete cutting": ["chisel", "concrete saw", "ear protection", "goggles", "jackhammer", "masks", "shovel"],
  "Construction|Concrete fill with rebar": ["10 mil vapor barrier", "buckets", "concrete", "dirt compactor", "mud mixer", "rebar", "shovel", "trowel"],
  "Construction|Concrete fill without rebar": ["10 mil vapor barrier", "buckets", "concrete", "dirt compactor", "mud mixer", "shovel", "trowel"],
  "Labor|General labor": ["work gloves", "broom", "shovel", "basic hand tools", "safety glasses"],
  "Junk|Junk removal": ["truck readiness", "straps and tie-downs", "safety gloves", "contractor bags", "broom"]
};

function defaultInventory() {
  return [
    { id: uuidv4(), key: "bleach_powder", name: "Bleach powder", quantity: 12, unit: "containers", reorderPoint: 4, active: true },
    { id: uuidv4(), key: "latex_gloves", name: "Box of latex gloves", quantity: 2, unit: "boxes", reorderPoint: 1, active: true },
    { id: uuidv4(), key: "broom", name: "Broom", quantity: 6, unit: "pieces", reorderPoint: 3, active: true },
    { id: uuidv4(), key: "buckets", name: "Buckets", quantity: 4, unit: "pieces", reorderPoint: null, active: true },
    { id: uuidv4(), key: "carpet_vacuum", name: "Carpet vacuum", quantity: 2, unit: "pieces", reorderPoint: null, active: true },
    { id: uuidv4(), key: "clean_mop_heads", name: "Clean mop heads", quantity: 8, unit: "pieces", reorderPoint: 6, active: true },
    { id: uuidv4(), key: "clorox_bathroom_spray", name: "Clorox bathroom spray", quantity: 6, unit: "bottles", reorderPoint: 3, active: true },
    {
      id: uuidv4(),
      key: "contractor_bags_stock",
      name: "Contractor bags",
      quantity: 550,
      unit: "bags",
      reorderPoint: 500,
      active: true,
      display: { mode: "boxesOf50", label: "boxes", perBox: 50 }
    },
    { id: uuidv4(), key: "dish_soap", name: "Dish soap", quantity: 5, unit: "bottles", reorderPoint: 3, active: true },
    { id: uuidv4(), key: "dish_sponge", name: "Dish sponge", quantity: 6, unit: "pieces", reorderPoint: 3, active: true },
    { id: uuidv4(), key: "dustpan", name: "Dustpan", quantity: 6, unit: "pieces", reorderPoint: 3, active: true },
    { id: uuidv4(), key: "floor_cleaner", name: "Floor cleaner", quantity: 11, unit: "bottles", reorderPoint: null, active: true },
    { id: uuidv4(), key: "goo_gone", name: "Goo Gone", quantity: 5, unit: "bottles", reorderPoint: 3, active: true },
    { id: uuidv4(), key: "goof_off", name: "Goof Off", quantity: 6, unit: "bottles", reorderPoint: 3, active: true },
    { id: uuidv4(), key: "green_scrub_pad", name: "Green scrub pad", quantity: 3, unit: "pieces", reorderPoint: 2, active: true },
    { id: uuidv4(), key: "magic_erasers", name: "Magic erasers", quantity: 12, unit: "pieces", reorderPoint: 3, active: true },
    { id: uuidv4(), key: "metal_razors", name: "Metal razors", quantity: 50, unit: "pieces", reorderPoint: 10, active: true },
    { id: uuidv4(), key: "metal_sponges", name: "Metal sponges", quantity: 6, unit: "pieces", reorderPoint: 3, active: true },
    { id: uuidv4(), key: "mop_sticks", name: "Mop sticks", quantity: 3, unit: "pieces", reorderPoint: null, active: true },
    { id: uuidv4(), key: "oven_cleaner", name: "Oven cleaner", quantity: 6, unit: "bottles", reorderPoint: 5, active: true },
    { id: uuidv4(), key: "plastic_razors", name: "Plastic razors", quantity: 3, unit: "pieces", reorderPoint: 3, active: true },
    { id: uuidv4(), key: "rags", name: "Rags", quantity: 48, unit: "pieces", reorderPoint: 20, active: true },
    { id: uuidv4(), key: "paper_towel_rolls", name: "Rolls of paper towel", quantity: 8, unit: "rolls", reorderPoint: 12, active: true },
    { id: uuidv4(), key: "scrub_brush", name: "Scrub brush", quantity: 3, unit: "pieces", reorderPoint: 3, active: true },
    { id: uuidv4(), key: "scrub_daddy", name: "Scrub Daddy", quantity: 6, unit: "pieces", reorderPoint: 3, active: true },
    { id: uuidv4(), key: "shoulder_vacuum", name: "Shoulder vacuum", quantity: 2, unit: "pieces", reorderPoint: null, active: true },
    { id: uuidv4(), key: "soap_water_bottles", name: "Soap and water bottles", quantity: 3, unit: "bottles", reorderPoint: 2, active: true },
    { id: uuidv4(), key: "squeegees", name: "Squeegees", quantity: 4, unit: "pieces", reorderPoint: 3, active: true },
    { id: uuidv4(), key: "toilet_brush", name: "Toilet brush", quantity: 12, unit: "pieces", reorderPoint: 6, active: true },
    { id: uuidv4(), key: "water_pics", name: "Water pics", quantity: null, unit: "pieces", reorderPoint: null, active: true }
  ];
}

function todayString() {
  return new Date().toISOString().slice(0, 10);
}


function defaultState() {
  return {
    contractors: [],
    jobs: [],
    estimates: [],
    inventory: defaultInventory(),
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

function cloneDb(db) {
  return JSON.parse(JSON.stringify(db));
}

function normalizeDbShape(db = {}) {
  const base = defaultState();
  db.contractors ||= [];
  db.jobs ||= [];
  db.estimates ||= [];
  db.inventory ||= base.inventory;
  db.employees ||= base.employees;
  db.timeClockEntries ||= [];
  db.settings ||= {};
  db.settings.inventoryLink ||= "";
  db.settings.quickbooks ||= base.settings.quickbooks;
  db.dailySetup ||= base.dailySetup;
  db.dailySetup.date ||= todayString();
  db.dailySetup.crewSize ||= 1;
  db.dailySetup.lunchBreaks ||= [];
  db.dailySetup.dailyChecklistState ||= {};
  db.contractors = db.contractors.map(c => ({ ...c, serviceAddresses: Array.isArray(c.serviceAddresses) ? c.serviceAddresses : [] }));
  db.jobs = db.jobs.map(j => ({
    ...j,
    services: Array.isArray(j.services) ? j.services : [],
    photos: Array.isArray(j.photos) ? j.photos : [],
    deletedAt: j.deletedAt || null,
    archivedAt: j.archivedAt || null,
    finishedAt: j.finishedAt || null,
    quickbooksStatus: j.quickbooksStatus || "not_sent",
    quickbooksInvoiceId: j.quickbooksInvoiceId || null,
    sortOrder: Number(j.sortOrder || 0)
  }));
  db.estimates = db.estimates.map(e => ({ ...e, services: Array.isArray(e.services) ? e.services : [], status: e.status || "open" }));
  db.inventory = db.inventory.map(i => ({ ...i, active: i.active !== false }));
  db.employees = db.employees.map(e => ({ id: e.id, name: e.name || e.fullName || "Unnamed Employee", active: e.active !== false, createdAt: e.createdAt || null }));
  db.timeClockEntries = db.timeClockEntries.map(t => ({ ...t, employeeId: t.employeeId || null, name: t.name || t.employeeName || "", date: t.date || todayString() }));
  return db;
}

function ensureDb() {
  if (!memoryDb) memoryDb = normalizeDbShape(defaultState());
}

function readDb() {
  ensureDb();
  return cloneDb(memoryDb);
}

function numericIfPossible(v) {
  const n = Number(v);
  return !Number.isNaN(n) && /^\d+$/.test(String(v)) ? n : v;
}

function serviceRowToObject(s) {
  return {
    category: s.category || "Cleaning",
    subtype: s.subtype || "General cleaning",
    crewSize: Number(s.crew_size || 1),
    hoursManual: Number(s.hours_manual || 0),
    linearFeet: Number(s.linear_feet || 0),
    junkLoad: s.junk_load || "Quarter load",
    junkPrice: Number(s.junk_price || 175),
    serviceDate: s.service_date || null,
    onMyWayTime: s.on_my_way_time || null,
    arrivedTime: s.arrived_time || null,
    startTime: s.start_time || null,
    endTime: s.end_time || null,
    materialsUsed: s.materials_used || {},
    inventoryDeductedAt: s.inventory_deducted_at || null
  };
}

async function migrateDatabase() {
  await query(`CREATE TABLE IF NOT EXISTS contractors (id TEXT PRIMARY KEY, company_name TEXT, contact_name TEXT, email TEXT, phone TEXT, billing_address TEXT, payment_terms TEXT, created_at TEXT)`);
  await query(`CREATE TABLE IF NOT EXISTS contractor_addresses (id SERIAL PRIMARY KEY, contractor_id TEXT REFERENCES contractors(id) ON DELETE CASCADE, address TEXT)`);
  await query(`CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, contractor_id TEXT, service_address TEXT, service_date TEXT, notes TEXT, created_at TEXT, sort_order INTEGER DEFAULT 0, deleted_at TEXT, archived_at TEXT, finished_at TEXT, quickbooks_status TEXT DEFAULT 'not_sent', quickbooks_invoice_id TEXT, from_estimate_id TEXT, open_status TEXT DEFAULT 'single_day')`);
  await query(`CREATE TABLE IF NOT EXISTS job_services (id SERIAL PRIMARY KEY, job_id TEXT REFERENCES jobs(id) ON DELETE CASCADE, service_index INTEGER DEFAULT 0, category TEXT, subtype TEXT, crew_size NUMERIC, hours_manual NUMERIC, linear_feet NUMERIC, junk_load TEXT, junk_price NUMERIC, service_date TEXT, on_my_way_time TEXT, arrived_time TEXT, start_time TEXT, end_time TEXT, materials_used JSONB DEFAULT '{}'::jsonb, inventory_deducted_at TEXT)`);
  await query(`CREATE TABLE IF NOT EXISTS job_photos (id TEXT PRIMARY KEY, job_id TEXT REFERENCES jobs(id) ON DELETE CASCADE, url TEXT, tag TEXT, caption TEXT, created_at TEXT)`);
  await query(`CREATE TABLE IF NOT EXISTS estimates (id TEXT PRIMARY KEY, contractor_id TEXT, service_address TEXT, notes TEXT, status TEXT DEFAULT 'open', created_at TEXT, converted_job_id TEXT)`);
  await query(`CREATE TABLE IF NOT EXISTS estimate_services (id SERIAL PRIMARY KEY, estimate_id TEXT REFERENCES estimates(id) ON DELETE CASCADE, service_index INTEGER DEFAULT 0, category TEXT, subtype TEXT, crew_size NUMERIC, hours_manual NUMERIC, linear_feet NUMERIC, junk_load TEXT, junk_price NUMERIC, service_date TEXT, on_my_way_time TEXT, arrived_time TEXT, start_time TEXT, end_time TEXT, materials_used JSONB DEFAULT '{}'::jsonb, inventory_deducted_at TEXT)`);
  await query(`CREATE TABLE IF NOT EXISTS inventory (id TEXT PRIMARY KEY, item_key TEXT UNIQUE, name TEXT, quantity NUMERIC, unit TEXT, reorder_point NUMERIC, active BOOLEAN DEFAULT true, display JSONB)`);
  await query(`CREATE TABLE IF NOT EXISTS employees (id TEXT PRIMARY KEY, name TEXT NOT NULL, active BOOLEAN DEFAULT true, created_at TEXT)`);
  await query(`CREATE TABLE IF NOT EXISTS time_clock_entries (id TEXT PRIMARY KEY, employee_id TEXT, employee_name TEXT, clock_in TEXT, clock_out TEXT, minutes NUMERIC, entry_date TEXT)`);
await query(`
  CREATE TABLE IF NOT EXISTS daily_setup (
    id SERIAL PRIMARY KEY,
    setup_date TEXT,
    crew_size INTEGER
  )
`);
  await query(`CREATE TABLE IF NOT EXISTS daily_checklist_state (id SERIAL PRIMARY KEY, check_date TEXT, item TEXT, checked BOOLEAN DEFAULT false, UNIQUE(check_date, item))`);
  await query(`CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value JSONB)`);
  await query(`CREATE TABLE IF NOT EXISTS quickbooks_tokens (id INTEGER PRIMARY KEY, connected BOOLEAN DEFAULT false, realm_id TEXT, access_token TEXT, refresh_token TEXT, expires_at TEXT, refresh_expires_at TEXT, last_connected_at TEXT)`);
}

async function loadDbFromPostgres() {
  const db = defaultState();
  const contractorsRes = await query(`SELECT * FROM contractors ORDER BY id`);
  const contractorAddressesRes = await query(`SELECT * FROM contractor_addresses ORDER BY id`);
  db.contractors = contractorsRes.rows.map(c => ({
    id: numericIfPossible(c.id),
    companyName: c.company_name || "",
    contactName: c.contact_name || "",
    email: c.email || "",
    phone: c.phone || "",
    billingAddress: c.billing_address || "",
    paymentTerms: c.payment_terms || "",
    serviceAddresses: contractorAddressesRes.rows.filter(a => String(a.contractor_id) === String(c.id)).map(a => a.address).filter(Boolean),
    createdAt: c.created_at || null
  }));

  const inventoryRes = await query(`SELECT * FROM inventory ORDER BY name`);
  db.inventory = inventoryRes.rows.map(i => ({
    id: i.id,
    key: i.item_key,
    name: i.name,
    quantity: i.quantity === null ? null : Number(i.quantity),
    unit: i.unit,
    reorderPoint: i.reorder_point === null ? null : Number(i.reorder_point),
    active: i.active !== false,
    display: i.display || null
  }));

  const jobsRes = await query(`SELECT * FROM jobs ORDER BY service_date, sort_order, id`);
  const servicesRes = await query(`SELECT * FROM job_services ORDER BY service_index, id`);
  const photosRes = await query(`SELECT * FROM job_photos ORDER BY created_at, id`);
  db.jobs = jobsRes.rows.map(j => ({
    id: numericIfPossible(j.id),
    contractorId: j.contractor_id,
    serviceAddress: j.service_address || "",
    serviceDate: j.service_date || todayString(),
    notes: j.notes || "",
    createdAt: j.created_at || null,
    sortOrder: Number(j.sort_order || 0),
    deletedAt: j.deleted_at || null,
    archivedAt: j.archived_at || null,
    finishedAt: j.finished_at || null,
    quickbooksStatus: j.quickbooks_status || "not_sent",
    quickbooksInvoiceId: j.quickbooks_invoice_id || null,
    fromEstimateId: j.from_estimate_id ? numericIfPossible(j.from_estimate_id) : undefined,
    openStatus: j.open_status || "single_day",
    services: servicesRes.rows.filter(s => String(s.job_id) === String(j.id)).map(serviceRowToObject),
    photos: photosRes.rows.filter(p => String(p.job_id) === String(j.id)).map(p => ({ id: p.id, url: p.url, tag: p.tag, caption: p.caption || "", createdAt: p.created_at || null }))
  }));

  const estimatesRes = await query(`SELECT * FROM estimates ORDER BY id`);
  const estimateServicesRes = await query(`SELECT * FROM estimate_services ORDER BY service_index, id`);
  db.estimates = estimatesRes.rows.map(e => ({
    id: numericIfPossible(e.id),
    contractorId: e.contractor_id,
    serviceAddress: e.service_address || "",
    notes: e.notes || "",
    status: e.status || "open",
    createdAt: e.created_at || null,
    convertedJobId: e.converted_job_id ? numericIfPossible(e.converted_job_id) : undefined,
    services: estimateServicesRes.rows.filter(s => String(s.estimate_id) === String(e.id)).map(serviceRowToObject)
  }));

  const employeesRes = await query(`SELECT * FROM employees ORDER BY active DESC, name`);
  db.employees = employeesRes.rows.map(e => ({ id: numericIfPossible(e.id), name: e.name, active: e.active !== false, createdAt: e.created_at || null }));

  const timeRes = await query(`SELECT * FROM time_clock_entries ORDER BY clock_in DESC`);
  db.timeClockEntries = timeRes.rows.map(t => ({ id: numericIfPossible(t.id), employeeId: t.employee_id ? numericIfPossible(t.employee_id) : null, name: t.employee_name, clockIn: t.clock_in, clockOut: t.clock_out, minutes: t.minutes === null ? null : Number(t.minutes), date: t.entry_date }));

  const setupRes = await query(`SELECT * FROM daily_setup WHERE id = 1`);
  if (setupRes.rows[0]) {
    const d = setupRes.rows[0];
    db.dailySetup = { date: d.current_date || todayString(), crewSize: Number(d.crew_size || 1), lunchBreaks: d.lunch_breaks || [], activeLunchStart: d.active_lunch_start || null, dailyChecklistState: {} };
  }

  const checklistRes = await query(`SELECT * FROM daily_checklist_state`);
  for (const row of checklistRes.rows) {
    db.dailySetup.dailyChecklistState[row.check_date] ||= {};
    db.dailySetup.dailyChecklistState[row.check_date][row.item] = !!row.checked;
  }

  const settingsRes = await query(`SELECT * FROM settings`);
  for (const row of settingsRes.rows) {
    if (row.key === "inventoryLink") db.settings.inventoryLink = row.value?.value || "";
  }

  const qbRes = await query(`SELECT * FROM quickbooks_tokens WHERE id = 1`);
  if (qbRes.rows[0]) {
    const q = qbRes.rows[0];
    db.settings.quickbooks = {
      connected: !!q.connected,
      realmId: q.realm_id || null,
      accessToken: q.access_token || null,
      refreshToken: q.refresh_token || null,
      expiresAt: q.expires_at || null,
      refreshExpiresAt: q.refresh_expires_at || null,
      lastConnectedAt: q.last_connected_at || null
    };
  }

  return normalizeDbShape(db);
}

async function insertServiceRow(client, table, parentColumn, parentId, s, index) {
  await client.query(
    `INSERT INTO ${table} (${parentColumn}, service_index, category, subtype, crew_size, hours_manual, linear_feet, junk_load, junk_price, service_date, on_my_way_time, arrived_time, start_time, end_time, materials_used, inventory_deducted_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
    [parentId, index, s.category || "Cleaning", s.subtype || "General cleaning", Number(s.crewSize || 1), Number(s.hoursManual || 0), Number(s.linearFeet || 0), s.junkLoad || "Quarter load", Number(s.junkPrice || 175), s.serviceDate || null, s.onMyWayTime || null, s.arrivedTime || null, s.startTime || null, s.endTime || null, s.materialsUsed || {}, s.inventoryDeductedAt || null]
  );
}

async function persistDbToPostgres(db) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const tables = ["contractor_addresses","job_photos","job_services","estimate_services","jobs","estimates","contractors","inventory","employees","time_clock_entries","daily_checklist_state","daily_setup","settings","quickbooks_tokens"];
    for (const table of tables) await client.query(`DELETE FROM ${table}`);

    for (const c of db.contractors || []) {
      await client.query(`INSERT INTO contractors (id, company_name, contact_name, email, phone, billing_address, payment_terms, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, [String(c.id), c.companyName || "", c.contactName || "", c.email || "", c.phone || "", c.billingAddress || "", c.paymentTerms || "", c.createdAt || new Date().toISOString()]);
      for (const addr of c.serviceAddresses || []) await client.query(`INSERT INTO contractor_addresses (contractor_id, address) VALUES ($1,$2)`, [String(c.id), addr]);
    }

    for (const item of db.inventory || []) {
      await client.query(`INSERT INTO inventory (id, item_key, name, quantity, unit, reorder_point, active, display) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, [String(item.id || item.key), item.key || String(item.id), item.name || "", item.quantity, item.unit || "", item.reorderPoint, item.active !== false, item.display || null]);
    }

    for (const job of db.jobs || []) {
      await client.query(`INSERT INTO jobs (id, contractor_id, service_address, service_date, notes, created_at, sort_order, deleted_at, archived_at, finished_at, quickbooks_status, quickbooks_invoice_id, from_estimate_id, open_status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, [String(job.id), job.contractorId == null ? null : String(job.contractorId), job.serviceAddress || "", job.serviceDate || todayString(), job.notes || "", job.createdAt || new Date().toISOString(), Number(job.sortOrder || 0), job.deletedAt || null, job.archivedAt || null, job.finishedAt || null, job.quickbooksStatus || "not_sent", job.quickbooksInvoiceId || null, job.fromEstimateId == null ? null : String(job.fromEstimateId), job.openStatus || "single_day"]);
      let index = 0;
      for (const s of job.services || []) await insertServiceRow(client, "job_services", "job_id", String(job.id), s, index++);
      for (const p of job.photos || []) await client.query(`INSERT INTO job_photos (id, job_id, url, tag, caption, created_at) VALUES ($1,$2,$3,$4,$5,$6)`, [String(p.id || uuidv4()), String(job.id), p.url || "", p.tag || "before", p.caption || "", p.createdAt || new Date().toISOString()]);
    }

    for (const est of db.estimates || []) {
      await client.query(`INSERT INTO estimates (id, contractor_id, service_address, notes, status, created_at, converted_job_id) VALUES ($1,$2,$3,$4,$5,$6,$7)`, [String(est.id), est.contractorId == null ? null : String(est.contractorId), est.serviceAddress || "", est.notes || "", est.status || "open", est.createdAt || new Date().toISOString(), est.convertedJobId == null ? null : String(est.convertedJobId)]);
      let index = 0;
      for (const s of est.services || []) await insertServiceRow(client, "estimate_services", "estimate_id", String(est.id), s, index++);
    }

    for (const e of db.employees || []) await client.query(`INSERT INTO employees (id, name, active, created_at) VALUES ($1,$2,$3,$4)`, [String(e.id), e.name || "Unnamed Employee", e.active !== false, e.createdAt || new Date().toISOString()]);
    for (const t of db.timeClockEntries || []) await client.query(`INSERT INTO time_clock_entries (id, employee_id, employee_name, clock_in, clock_out, minutes, entry_date) VALUES ($1,$2,$3,$4,$5,$6,$7)`, [String(t.id), t.employeeId == null ? null : String(t.employeeId), t.name || t.employeeName || "", t.clockIn || new Date().toISOString(), t.clockOut || null, t.minutes == null ? null : Number(t.minutes), t.date || todayString()]);

    await client.query(`INSERT INTO daily_setup (id, current_date, crew_size, lunch_breaks, active_lunch_start) VALUES (1,$1,$2,$3,$4)`, [db.dailySetup.date || todayString(), Number(db.dailySetup.crewSize || 1), db.dailySetup.lunchBreaks || [], db.dailySetup.activeLunchStart || null]);
    for (const [date, state] of Object.entries(db.dailySetup.dailyChecklistState || {})) {
      for (const [item, checked] of Object.entries(state || {})) {
        await client.query(`INSERT INTO daily_checklist_state (check_date, item, checked) VALUES ($1,$2,$3)`, [date, item, !!checked]);
      }
    }
    await client.query(`INSERT INTO settings (key, value) VALUES ($1,$2)`, ["inventoryLink", { value: db.settings.inventoryLink || "" }]);
    const qb = db.settings.quickbooks || {};
    await client.query(`INSERT INTO quickbooks_tokens (id, connected, realm_id, access_token, refresh_token, expires_at, refresh_expires_at, last_connected_at) VALUES (1,$1,$2,$3,$4,$5,$6,$7)`, [!!qb.connected, qb.realmId || null, qb.accessToken || null, qb.refreshToken || null, qb.expiresAt || null, qb.refreshExpiresAt || null, qb.lastConnectedAt || null]);

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

function writeDb(db, type = "db_updated", payload = {}) {
  memoryDb = normalizeDbShape(cloneDb(db));
  persistInFlight = persistInFlight
    .then(() => persistDbToPostgres(memoryDb))
    .catch(err => console.error("Postgres persist failed:", err));
  broadcastUpdate(type, payload);
}

async function initializeDatabaseBackedState() {
  await migrateDatabase();
  let loaded = await loadDbFromPostgres();
  const hasData = (loaded.contractors && loaded.contractors.length) || (loaded.inventory && loaded.inventory.length) || (loaded.jobs && loaded.jobs.length);
  if (!hasData && fs.existsSync(DB_FILE)) {
    try {
      const raw = JSON.parse(fs.readFileSync(DB_FILE, "utf8"));
      loaded = normalizeDbShape({ ...defaultState(), ...raw });
      await persistDbToPostgres(loaded);
      console.log("Seeded Postgres from db.json");
    } catch (err) {
      console.error("Could not seed from db.json:", err.message);
    }
  }
  if (!loaded.inventory || !loaded.inventory.length) loaded.inventory = defaultInventory();
  memoryDb = normalizeDbShape(loaded);
}

function nextNumericId(items) {
  const nums = items.map(x => Number(x.id)).filter(n => !Number.isNaN(n));
  return nums.length ? Math.max(...nums) + 1 : 1;
}

function cleanString(v) {
  return String(v || "").trim();
}

function isHourly(service) {
  return (
    service.category === "Cleaning" ||
    (service.category === "Construction" && service.subtype === "Demo") ||
    service.category === "Labor"
  );
}

function isConcrete(service) {
  return service.category === "Construction" && (
    service.subtype === "Concrete cutting" ||
    service.subtype === "Concrete fill with rebar" ||
    service.subtype === "Concrete fill without rebar"
  );
}

function rateForService(service) {
  if (service.category === "Cleaning") return 32;
  if (service.category === "Construction" && service.subtype === "Demo") return 35;
  if (service.category === "Labor") return 35;
  return 0;
}

function cleaningSuppliesAutoCharge(service) {
  return service.category === "Cleaning" && service.subtype !== "Site cleanup" ? 45 : 0;
}

function normalizeMaterials(raw = {}) {
  const out = {};

  for (const def of MATERIAL_DEFS) {
    let qty = Number(raw[def.key] || 0);
    if (def.wholeOnly) qty = Math.floor(qty);
    out[def.key] = qty;
  }

  return out;
}

function materialsBreakdown(service) {
  const mats = normalizeMaterials(service.materialsUsed || {});

  const rows = MATERIAL_DEFS
    .map(def => {
      const qty = Number(mats[def.key] || 0);
      if (!qty) return null;

      return {
        key: def.key,
        label: def.label,
        qty,
        unitLabel: def.unitLabel,
        price: def.price,
        total: Number((qty * def.price).toFixed(2))
      };
    })
    .filter(Boolean);

  const autoCharge = cleaningSuppliesAutoCharge(service);
  if (autoCharge) {
    rows.push({
      key: "cleaning_supplies_auto",
      label: "Cleaning supplies",
      qty: 1,
      unitLabel: "flat",
      price: 45,
      total: 45,
      auto: true
    });
  }

  return rows;
}

function materialsTotal(service) {
  return Number(materialsBreakdown(service).reduce((sum, x) => sum + x.total, 0).toFixed(2));
}

function calcHoursWorked(service) {
  if (service.startTime && service.endTime) {
    const ms = new Date(service.endTime) - new Date(service.startTime);
    return Number((Math.max(0, ms) / 1000 / 60 / 60).toFixed(2));
  }

  return Number(service.hoursManual || 0);
}

function calcTravelMinutes(service) {
  if (service.onMyWayTime && service.arrivedTime) {
    const ms = new Date(service.arrivedTime) - new Date(service.onMyWayTime);
    return Number((Math.max(0, ms) / 1000 / 60).toFixed(2));
  }

  return 0;
}

function calcBaseServiceTotal(service) {
  if (isHourly(service)) {
    return Number((calcHoursWorked(service) * Number(service.crewSize || 1) * rateForService(service)).toFixed(2));
  }

  if (service.category === "Junk") {
    return Number(service.junkPrice || 0);
  }

  if (isConcrete(service)) {
    return Number((Number(service.linearFeet || 0) * 25).toFixed(2));
  }

  return 0;
}

function calcServiceTotal(service) {
  return Number((calcBaseServiceTotal(service) + materialsTotal(service)).toFixed(2));
}

function normalizeService(raw = {}, defaultCrewSize = 1) {
  const s = {
    category: raw.category || "Cleaning",
    subtype: raw.subtype || "General cleaning",
    crewSize: Number(raw.crewSize ?? defaultCrewSize ?? 1),
    hoursManual: Number(raw.hoursManual || 0),
    linearFeet: Number(raw.linearFeet || 0),
    junkLoad: raw.junkLoad || "Quarter load",
    junkPrice: Number(raw.junkPrice || 175),
    onMyWayTime: raw.onMyWayTime || null,
    arrivedTime: raw.arrivedTime || null,
    startTime: raw.startTime || null,
    endTime: raw.endTime || null,
    materialsUsed: normalizeMaterials(raw.materialsUsed || {}),
    inventoryDeductedAt: raw.inventoryDeductedAt || null
  };

  s.travelMinutes = calcTravelMinutes(s);
  s.hoursWorked = calcHoursWorked(s);
  s.materialsBreakdown = materialsBreakdown(s);
  s.materialsTotal = materialsTotal(s);
  s.baseTotal = calcBaseServiceTotal(s);
  s.total = calcServiceTotal(s);

  return s;
}

function inferJobStatus(job) {
  if (job.archivedAt) return "archived";
  if (job.deletedAt) return "deleted";
  if (job.finishedAt) return "finished";

  const services = Array.isArray(job.services) ? job.services : [];

  const anyInProgress = services.some(s => s.startTime && !s.endTime);
  if (anyInProgress) return "in_progress";

  const anyOnTheWay = services.some(s => s.onMyWayTime && !s.arrivedTime);
  if (anyOnTheWay) return "on_the_way";

  const anyStarted = services.some(s => s.startTime || s.endTime || s.arrivedTime);
  if (anyStarted) return "active";

  return "scheduled";
}

function hydrateJob(job, db) {
  const contractor = db.contractors.find(c => String(c.id) === String(job.contractorId)) || null;
  const services = (job.services || []).map(s => normalizeService(s, db.dailySetup.crewSize || 1));

  const totalHours = Number(services.reduce((sum, s) => sum + s.hoursWorked, 0).toFixed(2));
  const totalTravelMinutes = Number(services.reduce((sum, s) => sum + s.travelMinutes, 0).toFixed(2));
  const totalCost = Number(services.reduce((sum, s) => sum + s.total, 0).toFixed(2));

  return {
    ...job,
    contractor,
    services,
    totalHours,
    totalTravelMinutes,
    totalCost,
    status: inferJobStatus({ ...job, services })
  };
}

function hydrateEstimate(est, db) {
  const contractor = db.contractors.find(c => String(c.id) === String(est.contractorId)) || null;
  const services = (est.services || []).map(s => normalizeService(s, db.dailySetup.crewSize || 1));

  const totalCost = Number(services.reduce((sum, s) => sum + s.total, 0).toFixed(2));

  return {
    ...est,
    contractor,
    services,
    totalCost
  };
}

function activeJobsForDate(db, date) {
  return db.jobs
    .filter(j => j.serviceDate === date && !j.deletedAt && !j.archivedAt)
    .sort((a, b) => (a.sortOrder || 0) - (b.sortOrder || 0) || Number(a.id) - Number(b.id));
}

function nextSortOrderForDate(db, date) {
  const jobs = activeJobsForDate(db, date);
  return jobs.length ? Math.max(...jobs.map(j => Number(j.sortOrder || 0))) + 1 : 1;
}

function getNextJob(db, date, currentId = null) {
  const jobs = activeJobsForDate(db, date)
    .map(j => hydrateJob(j, db))
    .filter(j => j.status !== "finished");

  if (!jobs.length) return null;
  if (!currentId) return jobs[0];

  const index = jobs.findIndex(j => String(j.id) === String(currentId));
  if (index < 0) return jobs[0];

  return jobs[index + 1] || null;
}

function sumRangesMinutes(ranges = []) {
  return Number(
    ranges.reduce((sum, r) => {
      if (!r.start || !r.end) return sum;
      return sum + ((new Date(r.end) - new Date(r.start)) / 1000 / 60);
    }, 0).toFixed(2)
  );
}

function dailyChecklistForDate(date, db) {
  const items = new Set();

  db.jobs
    .filter(job => job.serviceDate === date && !job.deletedAt && !job.archivedAt)
    .forEach(job => {
      (job.services || []).forEach(service => {
        const key = `${service.category}|${service.subtype}`;
        (CHECKLISTS[key] || []).forEach(item => items.add(item));
      });
    });

  return Array.from(items).sort((a, b) => a.localeCompare(b));
}

function buildChecklistWithState(date, db) {
  const state = db.dailySetup.dailyChecklistState?.[date] || {};

  return dailyChecklistForDate(date, db).map(item => ({
    item,
    checked: !!state[item]
  }));
}

function contractorDisplayName(c) {
  return cleanString(c.companyName) || cleanString(c.contactName) || "Unnamed Contractor";
}

function getInventoryViewItem(item) {
  const isBoxed = item.display?.mode === "boxesOf50";

  const displayQuantity = isBoxed
    ? Number((Number(item.quantity || 0) / Number(item.display.perBox || 50)).toFixed(2))
    : item.quantity;

  const displayReorderPoint =
    isBoxed && item.reorderPoint != null
      ? Number((Number(item.reorderPoint) / Number(item.display.perBox || 50)).toFixed(2))
      : item.reorderPoint;

  return {
    ...item,
    displayQuantity,
    displayUnit: isBoxed ? item.display.label : item.unit,
    displayReorderPoint
  };
}

function lowInventoryItems(db) {
  return db.inventory
    .filter(i => i.active !== false && i.reorderPoint != null && i.quantity != null && Number(i.quantity) <= Number(i.reorderPoint))
    .map(getInventoryViewItem);
}

function inventoryDeductionMap() {
  return {
    contractor_bags: { inventoryKey: "contractor_bags_stock", multiplier: 1 },
    zipper: { inventoryKey: null, multiplier: 1 },
    heavy_ramboard: { inventoryKey: null, multiplier: 0.25 },
    medium_ramboard: { inventoryKey: null, multiplier: 0.25 },
    contractor_paper: { inventoryKey: null, multiplier: 0.25 }
  };
}

function deductInventoryForService(db, service) {
  const map = inventoryDeductionMap();
  const mats = normalizeMaterials(service.materialsUsed || {});

  for (const def of MATERIAL_DEFS) {
    const qty = Number(mats[def.key] || 0);
    if (!qty) continue;

    const rule = map[def.key];
    if (!rule || !rule.inventoryKey) continue;

    const item = db.inventory.find(i => i.key === rule.inventoryKey);
    if (!item || item.quantity == null) continue;

    item.quantity = Number((Number(item.quantity) - qty * Number(rule.multiplier || 1)).toFixed(2));
    if (item.quantity < 0) item.quantity = 0;
  }
}

function deductInventoryForJobIfNeeded(db, job) {
  let changed = false;

  for (const service of job.services || []) {
    if (service.inventoryDeductedAt) continue;

    const hadMaterials = Object.values(normalizeMaterials(service.materialsUsed || {})).some(v => Number(v) > 0);
    const hadAutoCleaning = cleaningSuppliesAutoCharge(service) > 0;

    if (!hadMaterials && !hadAutoCleaning) continue;

    deductInventoryForService(db, service);
    service.inventoryDeductedAt = new Date().toISOString();
    changed = true;
  }

  return changed;
}

function writeInvoicePdf(job, filePath) {
  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({ margin: 50 });
    const stream = fs.createWriteStream(filePath);

    stream.on("finish", resolve);
    stream.on("error", reject);
    doc.on("error", reject);

    doc.pipe(stream);

    doc.fontSize(22).text("Swyft Demolition and Cleaning");
    doc.moveDown(0.5);
    doc.fontSize(12).text(`Invoice for Job #${job.id}`);
    doc.text(`Date: ${job.serviceDate}`);
    doc.moveDown();

    doc.fontSize(14).text("Bill To", { underline: true });
    doc.fontSize(12).text(job.contractor?.companyName || "");
    doc.text(job.contractor?.contactName || "");
    doc.text(job.contractor?.email || "");
    doc.text(job.contractor?.phone || "");
    doc.text(job.contractor?.billingAddress || "");
    doc.moveDown();

    doc.fontSize(14).text("Service Address", { underline: true });
    doc.fontSize(12).text(job.serviceAddress || "");
    doc.moveDown();

    doc.fontSize(14).text("Services", { underline: true });
    doc.moveDown(0.5);

    job.services.forEach(s => {
      let detail = "";
      if (isHourly(s)) detail = `${s.hoursWorked} hrs × ${s.crewSize} crew @ $${rateForService(s)}/hr`;
      if (s.category === "Junk") detail = `${s.junkLoad}`;
      if (isConcrete(s)) detail = `${s.linearFeet} linear ft @ $25/ft`;

      doc.fontSize(12).text(`${s.category} — ${s.subtype}`);
      doc.fontSize(11).fillColor("#666").text(detail);
      doc.fillColor("#000").text(`Service: $${s.baseTotal.toFixed(2)}`);

      if (s.materialsBreakdown.length) {
        doc.fontSize(11).fillColor("#444").text("Materials:");
        s.materialsBreakdown.forEach(m => {
          doc.text(`- ${m.label}: ${m.qty} ${m.unitLabel} = $${m.total.toFixed(2)}`);
        });
      }

      doc.fillColor("#000").text(`Total: $${s.total.toFixed(2)}`);
      doc.moveDown(0.5);
    });

    doc.moveDown();
    doc.fontSize(12).text(`Travel: ${job.totalTravelMinutes} min`);
    doc.moveDown();
    doc.fontSize(18).text(`Invoice Total: $${job.totalCost.toFixed(2)}`, { align: "right" });

    doc.end();
  });
}

function writeEstimatePdf(est, filePath) {
  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({ margin: 50 });
    const stream = fs.createWriteStream(filePath);

    stream.on("finish", resolve);
    stream.on("error", reject);
    doc.on("error", reject);

    doc.pipe(stream);

    doc.fontSize(22).text("Swyft Demolition and Cleaning");
    doc.moveDown(0.5);
    doc.fontSize(12).text(`Estimate #${est.id}`);
    doc.text(`Date: ${est.createdAt ? String(est.createdAt).slice(0, 10) : todayString()}`);
    doc.moveDown();

    doc.fontSize(14).text("Estimate For", { underline: true });
    doc.fontSize(12).text(est.contractor?.companyName || "");
    doc.text(est.contractor?.contactName || "");
    doc.text(est.contractor?.email || "");
    doc.text(est.contractor?.phone || "");
    doc.moveDown();

    doc.fontSize(14).text("Project Address", { underline: true });
    doc.fontSize(12).text(est.serviceAddress || "");
    doc.moveDown();

    doc.fontSize(14).text("Estimated Services", { underline: true });
    doc.moveDown(0.5);

    est.services.forEach(s => {
      let detail = "";
      if (isHourly(s)) detail = `${s.hoursManual} hrs × ${s.crewSize} crew @ $${rateForService(s)}/hr`;
      if (s.category === "Junk") detail = `${s.junkLoad}`;
      if (isConcrete(s)) detail = `${s.linearFeet} linear ft @ $25/ft`;

      doc.fontSize(12).text(`${s.category} — ${s.subtype}`);
      doc.fontSize(11).fillColor("#666").text(detail);
      doc.fillColor("#000").text(`Estimate: $${s.total.toFixed(2)}`);
      doc.moveDown(0.5);
    });

    doc.moveDown();
    doc.fontSize(18).text(`Estimate Total: $${est.totalCost.toFixed(2)}`, { align: "right" });

    doc.end();
  });
}

function qbConfigured() {
  return !!(
    process.env.QB_CLIENT_ID &&
    process.env.QB_CLIENT_SECRET &&
    process.env.QB_REDIRECT_URI
  );
}

function qbSettings(db) {
  return db.settings.quickbooks;
}

function qbAuthBase() {
  return "https://appcenter.intuit.com/connect/oauth2";
}

function qbApiBase(db) {
  const realmId = qbSettings(db).realmId;
  return `https://quickbooks.api.intuit.com/v3/company/${realmId}`;
}

async function qbRefreshIfNeeded(db) {
  const qb = qbSettings(db);
  if (!qb.refreshToken) throw new Error("QuickBooks not connected");

  const expiresAt = qb.expiresAt ? new Date(qb.expiresAt).getTime() : 0;
  const now = Date.now() + 60 * 1000;

  if (qb.accessToken && expiresAt > now) {
    return qb.accessToken;
  }

  const response = await axios.post(
    "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
    qs.stringify({
      grant_type: "refresh_token",
      refresh_token: qb.refreshToken
    }),
    {
      headers: {
        Authorization: "Basic " + Buffer.from(
          `${process.env.QB_CLIENT_ID}:${process.env.QB_CLIENT_SECRET}`
        ).toString("base64"),
        "Content-Type": "application/x-www-form-urlencoded"
      }
    }
  );

  qb.accessToken = response.data.access_token;
  qb.refreshToken = response.data.refresh_token || qb.refreshToken;
  qb.expiresAt = new Date(Date.now() + Number(response.data.expires_in || 3600) * 1000).toISOString();
  qb.refreshExpiresAt = response.data.x_refresh_token_expires_in
    ? new Date(Date.now() + Number(response.data.x_refresh_token_expires_in) * 1000).toISOString()
    : qb.refreshExpiresAt;
  qb.connected = true;
  qb.lastConnectedAt = new Date().toISOString();

  writeDb(db, "quickbooks_refreshed");
  return qb.accessToken;
}

async function qbApiRequest(db, method, endpoint, data = null, params = null) {
  if (!qbConfigured()) throw new Error("QuickBooks environment variables are missing");

  const accessToken = await qbRefreshIfNeeded(db);

  const response = await axios({
    method,
    url: `${qbApiBase(db)}${endpoint}`,
    data,
    params,
    headers: {
      Authorization: `Bearer ${accessToken}`,
      Accept: "application/json",
      "Content-Type": "application/json"
    }
  });

  return response.data;
}

async function qbFindCustomerByDisplayName(db, displayName) {
  const safeName = String(displayName).replace(/'/g, "\\'");
  const query = `select * from Customer where DisplayName = '${safeName}'`;

  const data = await qbApiRequest(db, "get", "/query", null, {
    query,
    minorversion: 73
  });

  const rows = data?.QueryResponse?.Customer || [];
  return rows[0] || null;
}

async function qbCreateCustomer(db, contractor) {
  const displayName = contractorDisplayName(contractor);
  const existing = await qbFindCustomerByDisplayName(db, displayName);
  if (existing) return existing;

  const payload = {
    DisplayName: displayName,
    PrimaryEmailAddr: contractor.email ? { Address: contractor.email } : undefined,
    PrimaryPhone: contractor.phone ? { FreeFormNumber: contractor.phone } : undefined,
    BillAddr: contractor.billingAddress ? { Line1: contractor.billingAddress } : undefined
  };

  const created = await qbApiRequest(db, "post", "/customer?minorversion=73", payload);
  return created.Customer || created;
}

async function qbCreateInvoiceForJob(db, job) {
  const contractor = job.contractor;
  if (!contractor) throw new Error(`Job ${job.id} has no contractor`);

  const customer = await qbCreateCustomer(db, contractor);
  const lines = [];

  job.services.forEach(s => {
    if (Number(s.baseTotal || 0) > 0) {
      lines.push({
        DetailType: "SalesItemLineDetail",
        Amount: Number(s.baseTotal || 0),
        Description: `${s.category} - ${s.subtype}`,
        SalesItemLineDetail: { Qty: 1, UnitPrice: Number(s.baseTotal || 0) }
      });
    }

    if (Number(s.materialsTotal || 0) > 0) {
      lines.push({
        DetailType: "SalesItemLineDetail",
        Amount: Number(s.materialsTotal || 0),
        Description: `Materials for ${s.category} - ${s.subtype}`,
        SalesItemLineDetail: { Qty: 1, UnitPrice: Number(s.materialsTotal || 0) }
      });
    }
  });

  if (!lines.length) {
    lines.push({
      DetailType: "SalesItemLineDetail",
      Amount: Number(job.totalCost || 0),
      Description: `Job ${job.id}`,
      SalesItemLineDetail: { Qty: 1, UnitPrice: Number(job.totalCost || 0) }
    });
  }

  const payload = {
    CustomerRef: { value: customer.Id },
    TxnDate: job.serviceDate,
    PrivateNote: `Job #${job.id} - ${job.serviceAddress || ""}`,
    Line: lines
  };

  const created = await qbApiRequest(db, "post", "/invoice?minorversion=73", payload);
  return created.Invoice || created;
}

function quickbooksReviewForDate(db, date) {
  const jobs = activeJobsForDate(db, date)
    .map(j => hydrateJob(j, db))
    .filter(j => j.status === "finished" || j.status === "archived");

  return jobs.map(job => ({
    id: job.id,
    address: job.serviceAddress,
    contractor: contractorDisplayName(job.contractor || {}),
    total: job.totalCost,
    status: job.quickbooksStatus || "not_sent",
    ready: !!job.contractor && Number(job.totalCost || 0) > 0
  }));
}

/* ---------- META / SETTINGS ---------- */

app.get("/meta", (_req, res) => {
  res.json({
    serviceOptions: SERVICE_OPTIONS,
    materialDefs: MATERIAL_DEFS
  });
});

app.get("/settings", (_req, res) => {
  const db = readDb();

  res.json({
    inventoryLink: db.settings.inventoryLink || "",
    quickbooks: {
      configured: qbConfigured(),
      connected: !!db.settings.quickbooks.connected,
      realmId: db.settings.quickbooks.realmId || null,
      lastConnectedAt: db.settings.quickbooks.lastConnectedAt || null
    }
  });
});

app.post("/settings/inventory-link", (req, res) => {
  const db = readDb();
  db.settings.inventoryLink = cleanString(req.body.inventoryLink);
  writeDb(db, "settings_updated");
  res.json({ inventoryLink: db.settings.inventoryLink });
});

/* ---------- DAILY SETUP / CHECKLIST ---------- */

app.get("/daily-setup", (_req, res) => {
  const db = readDb();
  res.json({
    ...db.dailySetup,
    lunchMinutes: sumRangesMinutes(db.dailySetup.lunchBreaks || [])
  });
});

app.post("/daily-setup", (req, res) => {
  let db = readDb();
if (!db) db = {};
if (!db.dailySetup) db.dailySetup = {};
if (!Array.isArray(db.jobs)) db.jobs = [];
  db.dailySetup.date = cleanString(req.body.date) || todayString();
  db.dailySetup.crewSize = Number(req.body.crewSize || 1);
  // Update all unfinished jobs for that day
for (const job of db.jobs || []) {
  // Update all unfinished jobs for that day
if (Array.isArray(db.jobs)) {
  for (const job of db.jobs) {
    if (job.serviceDate === db.dailySetup.date) {
      if (Array.isArray(job.services)) {
        for (const service of job.services) {
          if (!service.endTime) {
            service.crewSize = db.dailySetup.crewSize;
          }
        }
      }
    }
  }
}
  writeDb(db, "daily_setup_updated");

  res.json({
    ...db.dailySetup,
    lunchMinutes: sumRangesMinutes(db.dailySetup.lunchBreaks || [])
  });
});

app.post("/daily-lunch-start", (_req, res) => {
  const db = readDb();
  if (db.dailySetup.activeLunchStart) {
    return res.status(400).json({ error: "Lunch already started" });
  }

  db.dailySetup.activeLunchStart = new Date().toISOString();
  writeDb(db, "lunch_started");

  res.json({
    ...db.dailySetup,
    lunchMinutes: sumRangesMinutes(db.dailySetup.lunchBreaks || [])
  });
});

app.post("/daily-lunch-end", (_req, res) => {
  const db = readDb();
  if (!db.dailySetup.activeLunchStart) {
    return res.status(400).json({ error: "Lunch not started" });
  }

  db.dailySetup.lunchBreaks.push({
    start: db.dailySetup.activeLunchStart,
    end: new Date().toISOString()
  });

  db.dailySetup.activeLunchStart = null;
  writeDb(db, "lunch_ended");

  res.json({
    ...db.dailySetup,
    lunchMinutes: sumRangesMinutes(db.dailySetup.lunchBreaks || [])
  });
});

app.get("/daily-checklist", (req, res) => {
  const db = readDb();
  const date = cleanString(req.query.date) || db.dailySetup.date || todayString();

  res.json({
    date,
    items: buildChecklistWithState(date, db)
  });
});

app.post("/daily-checklist/toggle", (req, res) => {
  const db = readDb();

  const date = cleanString(req.body.date) || db.dailySetup.date || todayString();
  const item = cleanString(req.body.item);
  const checked = !!req.body.checked;

  db.dailySetup.dailyChecklistState[date] ||= {};
  db.dailySetup.dailyChecklistState[date][item] = checked;

  writeDb(db, "checklist_updated", { date, item, checked });
  res.json({ ok: true });
});

/* ---------- DASHBOARD / REPORTING ---------- */

app.get("/dashboard", (req, res) => {
  const db = readDb();
  const date = cleanString(req.query.date) || db.dailySetup.date || todayString();
  const jobs = activeJobsForDate(db, date).map(j => hydrateJob(j, db));

  res.json({
    date,
    totalJobs: jobs.length,
    totalTravelMinutes: Number(jobs.reduce((sum, j) => sum + j.totalTravelMinutes, 0).toFixed(2)),
    totalBillableHours: Number(jobs.reduce((sum, j) => sum + j.totalHours, 0).toFixed(2)),
    lunchMinutes: sumRangesMinutes(db.dailySetup.lunchBreaks || []),
    scheduled: jobs.filter(j => j.status === "scheduled").length,
    onTheWay: jobs.filter(j => j.status === "on_the_way").length,
    inProgress: jobs.filter(j => j.status === "in_progress" || j.status === "active").length,
    finished: jobs.filter(j => j.status === "finished").length,
    lowInventoryCount: lowInventoryItems(db).length
  });
});

app.get("/reporting", (_req, res) => {
  const db = readDb();
  const hydrated = db.jobs.filter(j => !j.deletedAt).map(j => hydrateJob(j, db));
  const byDate = {};

  hydrated.forEach(job => {
    byDate[job.serviceDate] ||= { date: job.serviceDate, travelMinutes: 0, billableHours: 0, totalRevenue: 0 };
    byDate[job.serviceDate].travelMinutes += job.totalTravelMinutes || 0;
    byDate[job.serviceDate].billableHours += job.totalHours || 0;
    byDate[job.serviceDate].totalRevenue += job.totalCost || 0;
  });

  res.json(
    Object.values(byDate)
      .map(x => ({
        ...x,
        travelMinutes: Number(x.travelMinutes.toFixed(2)),
        billableHours: Number(x.billableHours.toFixed(2)),
        totalRevenue: Number(x.totalRevenue.toFixed(2))
      }))
      .sort((a, b) => b.date.localeCompare(a.date))
  );
});

app.get("/history", (_req, res) => {
  const db = readDb();

  const rows = db.jobs
    .filter(j => j.archivedAt || j.finishedAt)
    .map(j => hydrateJob(j, db))
    .sort((a, b) => {
      const ad = a.archivedAt || a.finishedAt || a.createdAt || "";
      const bd = b.archivedAt || b.finishedAt || b.createdAt || "";
      return String(bd).localeCompare(String(ad));
    });

  res.json(rows);
});

/* ---------- CONTRACTORS ---------- */

app.get("/contractors", (req, res) => {
  const db = readDb();
  const search = cleanString(req.query.search).toLowerCase();

  let list = db.contractors;
  if (search) {
    list = list.filter(c =>
      (c.companyName || "").toLowerCase().includes(search) ||
      (c.contactName || "").toLowerCase().includes(search) ||
      (c.email || "").toLowerCase().includes(search) ||
      (c.phone || "").toLowerCase().includes(search)
    );
  }

  res.json(
    [...list].sort((a, b) => contractorDisplayName(a).localeCompare(contractorDisplayName(b)))
  );
});

app.post("/contractors", (req, res) => {
  const db = readDb();

  const contractor = {
    id: nextNumericId(db.contractors),
    companyName: cleanString(req.body.companyName),
    contactName: cleanString(req.body.contactName),
    email: cleanString(req.body.email),
    phone: cleanString(req.body.phone),
    billingAddress: cleanString(req.body.billingAddress),
    paymentTerms: cleanString(req.body.paymentTerms),
    serviceAddresses: Array.isArray(req.body.serviceAddresses)
      ? req.body.serviceAddresses.map(cleanString).filter(Boolean)
      : [],
    createdAt: new Date().toISOString()
  };

  db.contractors.push(contractor);
  writeDb(db, "contractor_added");
  res.json(contractor);
});

app.put("/contractors/:id", (req, res) => {
  const db = readDb();
  const contractor = db.contractors.find(c => String(c.id) === String(req.params.id));

  if (!contractor) return res.status(404).json({ error: "Contractor not found" });

  contractor.companyName = cleanString(req.body.companyName);
  contractor.contactName = cleanString(req.body.contactName);
  contractor.email = cleanString(req.body.email);
  contractor.phone = cleanString(req.body.phone);
  contractor.billingAddress = cleanString(req.body.billingAddress);
  contractor.paymentTerms = cleanString(req.body.paymentTerms);
  contractor.serviceAddresses = Array.isArray(req.body.serviceAddresses)
    ? req.body.serviceAddresses.map(cleanString).filter(Boolean)
    : contractor.serviceAddresses;

  writeDb(db, "contractor_updated");
  res.json(contractor);
});

/* ---------- JOBS ---------- */

app.get("/jobs", (req, res) => {
  const db = readDb();
  const date = cleanString(req.query.date) || db.dailySetup.date || todayString();
  const jobs = activeJobsForDate(db, date).map(j => hydrateJob(j, db));
  res.json(jobs);
});

app.get("/jobs/next", (req, res) => {
  const db = readDb();
  const date = cleanString(req.query.date) || db.dailySetup.date || todayString();
  const currentId = req.query.currentId || null;
  const next = getNextJob(db, date, currentId);
  res.json(next);
});

app.post("/jobs", (req, res) => {
  const db = readDb();
  const serviceDate = cleanString(req.body.serviceDate) || db.dailySetup.date || todayString();

  const job = {
    id: nextNumericId(db.jobs),
    contractorId: String(req.body.contractorId || ""),
    serviceAddress: cleanString(req.body.serviceAddress),
    serviceDate,
    notes: cleanString(req.body.notes),
    createdAt: new Date().toISOString(),
    services: Array.isArray(req.body.services)
      ? req.body.services.map(s => normalizeService(s, db.dailySetup.crewSize))
      : [],
    photos: [],
    sortOrder: nextSortOrderForDate(db, serviceDate),
    deletedAt: null,
    archivedAt: null,
    finishedAt: null,
    quickbooksStatus: "not_sent",
    quickbooksInvoiceId: null
  };

  db.jobs.push(job);
  writeDb(db, "job_created", { id: job.id });
  res.json(hydrateJob(job, db));
});

app.put("/jobs/:id", (req, res) => {
  const db = readDb();
  const job = db.jobs.find(j => String(j.id) === String(req.params.id));

  if (!job) return res.status(404).json({ error: "Job not found" });

  const oldDate = job.serviceDate;
  const newDate = cleanString(req.body.serviceDate) || oldDate;

  job.contractorId = req.body.contractorId !== undefined ? String(req.body.contractorId) : job.contractorId;
  job.serviceAddress = req.body.serviceAddress !== undefined ? cleanString(req.body.serviceAddress) : job.serviceAddress;
  job.serviceDate = newDate;
  job.notes = req.body.notes !== undefined ? cleanString(req.body.notes) : job.notes;

  if (Array.isArray(req.body.services)) {
    job.services = req.body.services.map((s, index) => {
      const existing = job.services[index] || {};
      return normalizeService({ ...existing, ...s }, db.dailySetup.crewSize);
    });
  }

  if (oldDate !== newDate) {
    job.sortOrder = nextSortOrderForDate(db, newDate);
  }

  writeDb(db, "job_updated", { id: job.id });
  res.json(hydrateJob(job, db));
});

app.post("/jobs/reorder", (req, res) => {
  const db = readDb();
  const orderedIds = Array.isArray(req.body.orderedIds) ? req.body.orderedIds.map(String) : [];

  orderedIds.forEach((id, index) => {
    const job = db.jobs.find(j => String(j.id) === id);
    if (job) job.sortOrder = index + 1;
  });

  writeDb(db, "jobs_reordered");
  res.json({ ok: true });
});

app.delete("/jobs/:id", (req, res) => {
  const db = readDb();
  const job = db.jobs.find(j => String(j.id) === String(req.params.id));

  if (!job) return res.status(404).json({ error: "Job not found" });

  job.deletedAt = new Date().toISOString();
  writeDb(db, "job_deleted", { id: job.id });
  res.json({ ok: true });
});

app.post("/jobs/:id/archive", (req, res) => {
  const db = readDb();
  const job = db.jobs.find(j => String(j.id) === String(req.params.id));

  if (!job) return res.status(404).json({ error: "Job not found" });

  job.archivedAt = new Date().toISOString();
  writeDb(db, "job_archived", { id: job.id });
  res.json({ ok: true });
});

/* ---------- SERVICE FLOW ---------- */

function getService(req, res) {
  const db = readDb();
  const job = db.jobs.find(j => String(j.id) === String(req.params.id));

  if (!job) {
    res.status(404).json({ error: "Job not found" });
    return {};
  }

  const index = Number(req.params.index || 0);
  const service = job.services[index];

  if (!service) {
    res.status(404).json({ error: "Service not found" });
    return {};
  }

  return { db, job, service, index };
}

app.post("/jobs/:id/services/:index/on-my-way", (req, res) => {
  const { db, job, service } = getService(req, res);
  if (!service) return;

  service.onMyWayTime = new Date().toISOString();
  writeDb(db, "job_updated", { id: job.id });
  res.json(hydrateJob(job, db));
});

app.post("/jobs/:id/services/:index/arrived", (req, res) => {
  const { db, job, service } = getService(req, res);
  if (!service) return;

  service.arrivedTime = new Date().toISOString();
  writeDb(db, "job_updated", { id: job.id });
  res.json(hydrateJob(job, db));
});

app.post("/jobs/:id/services/:index/start", (req, res) => {
  const { db, job, service } = getService(req, res);
  if (!service) return;

  if (!isHourly(service)) {
    return res.status(400).json({ error: "This service does not use a timer" });
  }

  service.startTime = new Date().toISOString();
  service.endTime = null;

  writeDb(db, "job_updated", { id: job.id });
  res.json(hydrateJob(job, db));
});

app.post("/jobs/:id/services/:index/stop", (req, res) => {
  const { db, job, service } = getService(req, res);
  if (!service) return;

  if (isHourly(service) && !service.startTime) {
    return res.status(400).json({ error: "Start the service first" });
  }

  service.endTime = new Date().toISOString();
  service.materialsUsed = normalizeMaterials(req.body.materialsUsed || {});

  deductInventoryForJobIfNeeded(db, job);

  if (job.services.length && job.services.every(s => s.endTime || !isHourly(s))) {
    job.finishedAt = new Date().toISOString();
  }

  const next = getNextJob(db, job.serviceDate, job.id);

  writeDb(db, "job_updated", { id: job.id, nextJob: next ? next.id : null });
  res.json({ ok: true, job: hydrateJob(job, db), nextJob: next ? next.id : null });
});

/* ---------- PHOTOS ---------- */

app.post("/jobs/:id/photos", upload.single("photo"), (req, res) => {
  const db = readDb();
  const job = db.jobs.find(j => String(j.id) === String(req.params.id));

  if (!job) return res.status(404).json({ error: "Job not found" });
  if (!req.file) return res.status(400).json({ error: "No photo uploaded" });

  job.photos ||= [];
  const photo = {
    id: uuidv4(),
    url: `/uploads/${req.file.filename}`,
    tag: cleanString(req.body.tag) || "before",
    caption: cleanString(req.body.caption),
    createdAt: new Date().toISOString()
  };

  job.photos.push(photo);
  writeDb(db, "photo_added", { jobId: job.id, photoId: photo.id });
  res.json(photo);
});

app.delete("/jobs/:jobId/photos/:photoId", (req, res) => {
  const db = readDb();
  const job = db.jobs.find(j => String(j.id) === String(req.params.jobId));

  if (!job) return res.status(404).json({ error: "Job not found" });

  const photo = (job.photos || []).find(p => String(p.id) === String(req.params.photoId));
  if (!photo) return res.status(404).json({ error: "Photo not found" });

  const filePath = path.join(__dirname, photo.url.replace(/^\//, ""));
  if (fs.existsSync(filePath)) fs.unlinkSync(filePath);

  job.photos = (job.photos || []).filter(p => String(p.id) !== String(req.params.photoId));
  writeDb(db, "photo_deleted", { jobId: job.id, photoId: req.params.photoId });
  res.json({ ok: true });
});

/* ---------- INVENTORY ---------- */

app.get("/inventory", (_req, res) => {
  const db = readDb();

  res.json({
    items: db.inventory.filter(i => i.active !== false).map(getInventoryViewItem),
    lowStock: lowInventoryItems(db)
  });
});

app.put("/inventory/:id", (req, res) => {
  const db = readDb();
  const item = db.inventory.find(i => String(i.id) === String(req.params.id));

  if (!item) return res.status(404).json({ error: "Inventory item not found" });

  item.name = cleanString(req.body.name) || item.name;
  item.quantity = req.body.quantity === null || req.body.quantity === "" ? null : Number(req.body.quantity);
  item.unit = cleanString(req.body.unit) || item.unit;
  item.reorderPoint = req.body.reorderPoint === null || req.body.reorderPoint === "" ? null : Number(req.body.reorderPoint);
  item.active = req.body.active === undefined ? item.active : !!req.body.active;

  writeDb(db, "inventory_updated");
  res.json(getInventoryViewItem(item));
});

app.post("/inventory/adjust", (req, res) => {
  const db = readDb();
  const item = db.inventory.find(i => String(i.id) === String(req.body.id));

  if (!item) return res.status(404).json({ error: "Item not found" });
  if (item.quantity == null) return res.status(400).json({ error: "This item does not track quantity" });

  item.quantity = Number((Number(item.quantity || 0) + Number(req.body.delta || 0)).toFixed(2));
  if (item.quantity < 0) item.quantity = 0;

  writeDb(db, "inventory_updated");
  res.json(getInventoryViewItem(item));
});

app.post("/inventory/add", (req, res) => {
  const db = readDb();

  const item = {
    id: uuidv4(),
    key: cleanString(req.body.key) || uuidv4(),
    name: cleanString(req.body.name),
    quantity: req.body.quantity === null || req.body.quantity === "" ? null : Number(req.body.quantity),
    unit: cleanString(req.body.unit) || "pieces",
    reorderPoint: req.body.reorderPoint === null || req.body.reorderPoint === "" ? null : Number(req.body.reorderPoint),
    active: true
  };

  db.inventory.push(item);
  writeDb(db, "inventory_added");
  res.json(getInventoryViewItem(item));
});

/* ---------- ESTIMATES ---------- */

app.get("/estimates", (_req, res) => {
  const db = readDb();
  res.json(db.estimates.map(e => hydrateEstimate(e, db)).sort((a, b) => Number(b.id) - Number(a.id)));
});

app.post("/estimates", (req, res) => {
  const db = readDb();

  const estimate = {
    id: nextNumericId(db.estimates),
    contractorId: String(req.body.contractorId || ""),
    serviceAddress: cleanString(req.body.serviceAddress),
    notes: cleanString(req.body.notes),
    status: "open",
    createdAt: new Date().toISOString(),
    services: Array.isArray(req.body.services)
      ? req.body.services.map(s => normalizeService(s, db.dailySetup.crewSize))
      : []
  };

  db.estimates.push(estimate);
  writeDb(db, "estimate_created", { id: estimate.id });
  res.json(hydrateEstimate(estimate, db));
});

app.post("/estimates/:id/convert", (req, res) => {
  const db = readDb();
  const estimate = db.estimates.find(e => String(e.id) === String(req.params.id));
  if (!estimate) return res.status(404).json({ error: "Estimate not found" });

  const serviceDate = cleanString(req.body.serviceDate) || db.dailySetup.date || todayString();

  const job = {
    id: nextNumericId(db.jobs),
    contractorId: String(estimate.contractorId),
    serviceAddress: estimate.serviceAddress,
    serviceDate,
    notes: estimate.notes || "",
    createdAt: new Date().toISOString(),
    services: (estimate.services || []).map(s => normalizeService(s, db.dailySetup.crewSize)),
    photos: [],
    sortOrder: nextSortOrderForDate(db, serviceDate),
    deletedAt: null,
    archivedAt: null,
    finishedAt: null,
    quickbooksStatus: "not_sent",
    quickbooksInvoiceId: null,
    fromEstimateId: estimate.id
  };

  estimate.status = "converted";
  estimate.convertedJobId = job.id;
  db.jobs.push(job);

  writeDb(db, "estimate_converted", { estimateId: estimate.id, jobId: job.id });
  res.json(hydrateJob(job, db));
});

app.get("/estimate/:id/pdf", async (req, res) => {
  const db = readDb();
  const raw = db.estimates.find(e => String(e.id) === String(req.params.id));
  if (!raw) return res.status(404).send("Estimate not found");

  const est = hydrateEstimate(raw, db);
  const filePath = path.join(EXPORTS_DIR, `estimate-${est.id}.pdf`);
  await writeEstimatePdf(est, filePath);
  res.download(filePath);
});

/* ---------- FINISH DAY ---------- */

app.get("/finish-day-summary", (req, res) => {
  const db = readDb();
  const date = cleanString(req.query.date) || db.dailySetup.date || todayString();
  const jobs = activeJobsForDate(db, date).map(j => hydrateJob(j, db));

  res.json({
    date,
    totalJobs: jobs.length,
    totalTravelMinutes: Number(jobs.reduce((sum, j) => sum + j.totalTravelMinutes, 0).toFixed(2)),
    lunchMinutes: sumRangesMinutes(db.dailySetup.lunchBreaks || []),
    totalPhotos: jobs.reduce((sum, j) => sum + (j.photos?.length || 0), 0),
    totalMaterials: Number(
      jobs.reduce((sum, j) => sum + j.services.reduce((svcSum, s) => svcSum + (s.materialsTotal || 0), 0), 0).toFixed(2)
    ),
    quickbooksReadyCount: quickbooksReviewForDate(db, date).filter(x => x.ready).length,
    jobs: jobs.map(j => ({
      id: j.id,
      address: j.serviceAddress,
      status: j.status,
      totalHours: j.totalHours,
      totalCost: j.totalCost,
      photos: j.photos?.length || 0
    }))
  });
});

app.post("/finish-day", async (req, res) => {
  const db = readDb();
  const date = cleanString(req.body.date) || db.dailySetup.date || todayString();

  const rawJobs = activeJobsForDate(db, date);
  const finishedRawJobs = rawJobs.filter(j => inferJobStatus(j) === "finished");

  if (!finishedRawJobs.length) {
    return res.json({ finishedJobs: 0, downloadUrl: null });
  }

  for (const raw of finishedRawJobs) {
    deductInventoryForJobIfNeeded(db, raw);
  }

  writeDb(db, "finish_day_started");

  const hydratedJobs = finishedRawJobs.map(j => hydrateJob(j, db));

  const zipName = `invoices-${date}-${Date.now()}.zip`;
  const zipPath = path.join(EXPORTS_DIR, zipName);
  const output = fs.createWriteStream(zipPath);
  const archive = archiver("zip", { zlib: { level: 9 } });

  output.on("close", () => {
    const db2 = readDb();
    finishedRawJobs.forEach(j => {
      const dbJob = db2.jobs.find(x => String(x.id) === String(j.id));
      if (dbJob) dbJob.archivedAt = new Date().toISOString();
    });

    writeDb(db2, "day_finished");

    res.json({
      finishedJobs: finishedRawJobs.length,
      downloadUrl: `/exports/${zipName}`
    });
  });

  archive.on("error", err => {
    res.status(500).json({ error: err.message });
  });

  archive.pipe(output);

  for (const job of hydratedJobs) {
    const filePath = path.join(EXPORTS_DIR, `invoice-${job.id}.pdf`);
    await writeInvoicePdf(job, filePath);
    archive.file(filePath, { name: `invoice-${job.id}.pdf` });
  }

  archive.finalize();
});

/* ---------- INVOICE HTML ---------- */

app.get("/invoice/:id", (req, res) => {
  const db = readDb();
  const raw = db.jobs.find(j => String(j.id) === String(req.params.id));
  if (!raw) return res.status(404).send("Job not found");

  const job = hydrateJob(raw, db);

  const rows = job.services.map(s => {
    let detail = "";
    if (isHourly(s)) detail = `${s.hoursWorked} hrs × ${s.crewSize} crew @ $${rateForService(s)}/hr`;
    if (s.category === "Junk") detail = `${s.junkLoad}`;
    if (isConcrete(s)) detail = `${s.linearFeet} linear ft @ $25/ft`;

    const materialsHtml = s.materialsBreakdown.length
      ? `<div style="margin-top:6px;color:#555;">${s.materialsBreakdown.map(m => `${m.label}: ${m.qty} ${m.unitLabel} ($${m.total.toFixed(2)})`).join("<br>")}</div>`
      : "";

    return `
      <tr>
        <td>${s.category}</td>
        <td>${s.subtype}</td>
        <td>${detail}${materialsHtml}</td>
        <td>$${s.total.toFixed(2)}</td>
      </tr>
    `;
  }).join("");

  res.send(`
    <html>
      <head>
        <title>Invoice ${job.id}</title>
        <style>
          body { font-family: Arial, sans-serif; padding: 24px; color: #222; }
          .wrap { max-width: 900px; margin: 0 auto; }
          .header { display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:24px; }
          .brand { font-size: 28px; font-weight: bold; }
          .box { border:1px solid #ddd; border-radius:12px; padding:16px; margin:12px 0; }
          table { width:100%; border-collapse: collapse; }
          th, td { border-bottom:1px solid #eee; padding:10px; text-align:left; vertical-align:top; }
          th { background:#f7f7f7; }
          .total { font-size: 24px; font-weight: bold; text-align:right; margin-top:18px; }
          .photos img { width: 180px; border-radius: 10px; margin: 8px 8px 0 0; border:1px solid #ddd; }
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="header">
            <div>
              <div class="brand">Swyft Demolition and Cleaning</div>
              <div>Invoice for Job #${job.id}</div>
            </div>
            <div><strong>Date:</strong> ${job.serviceDate}</div>
          </div>

          <div class="box">
            <h3>Bill To</h3>
            <div>${job.contractor?.companyName || ""}</div>
            <div>${job.contractor?.contactName || ""}</div>
            <div>${job.contractor?.email || ""}</div>
            <div>${job.contractor?.phone || ""}</div>
            <div>${job.contractor?.billingAddress || ""}</div>
            <div><strong>Terms:</strong> ${job.contractor?.paymentTerms || ""}</div>
          </div>

          <div class="box">
            <h3>Service Address</h3>
            <div>${job.serviceAddress}</div>
          </div>

          <div class="box">
            <h3>Services</h3>
            <table>
              <thead>
                <tr>
                  <th>Category</th>
                  <th>Service</th>
                  <th>Detail</th>
                  <th>Total</th>
                </tr>
              </thead>
              <tbody>${rows}</tbody>
            </table>
            <div style="margin-top:16px;">Travel: ${job.totalTravelMinutes} min</div>
            <div class="total">Total: $${job.totalCost.toFixed(2)}</div>
          </div>

          ${(job.photos || []).length ? `
            <div class="box photos">
              <h3>Job Photos</h3>
              ${(job.photos || []).map(p => `
                <div style="display:inline-block; margin-right:12px;">
                  <div><strong>${p.tag}</strong>${p.caption ? " - " + p.caption : ""}</div>
                  <img src="${p.url}" alt="${p.tag}" />
                </div>
              `).join("")}
            </div>
          ` : ""}
        </div>
      </body>
    </html>
  `);
});

/* ---------- QUICKBOOKS ---------- */

app.get("/qb/status", (_req, res) => {
  const db = readDb();
  const qb = qbSettings(db);

  res.json({
    configured: qbConfigured(),
    connected: !!qb.connected,
    realmId: qb.realmId || null,
    lastConnectedAt: qb.lastConnectedAt || null,
    expiresAt: qb.expiresAt || null
  });
});

app.get("/qb/connect", (_req, res) => {
  if (!qbConfigured()) return res.status(400).send("QuickBooks not configured");

  const state = uuidv4();
  const url = `${qbAuthBase()}?` + qs.stringify({
    client_id: process.env.QB_CLIENT_ID,
    response_type: "code",
    scope: "com.intuit.quickbooks.accounting",
    redirect_uri: process.env.QB_REDIRECT_URI,
    state
  });

  res.redirect(url);
});

app.get("/qb/callback", async (req, res) => {
  const db = readDb();

  if (!req.query.code || !req.query.realmId) {
    return res.status(400).send("Missing QuickBooks callback parameters");
  }

  try {
    const response = await axios.post(
      "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
      qs.stringify({
        grant_type: "authorization_code",
        code: req.query.code,
        redirect_uri: process.env.QB_REDIRECT_URI
      }),
      {
        headers: {
          Authorization: "Basic " + Buffer.from(
            `${process.env.QB_CLIENT_ID}:${process.env.QB_CLIENT_SECRET}`
          ).toString("base64"),
          "Content-Type": "application/x-www-form-urlencoded"
        }
      }
    );

    db.settings.quickbooks = {
      connected: true,
      realmId: String(req.query.realmId),
      accessToken: response.data.access_token,
      refreshToken: response.data.refresh_token,
      expiresAt: new Date(Date.now() + Number(response.data.expires_in || 3600) * 1000).toISOString(),
      refreshExpiresAt: response.data.x_refresh_token_expires_in
        ? new Date(Date.now() + Number(response.data.x_refresh_token_expires_in) * 1000).toISOString()
        : null,
      lastConnectedAt: new Date().toISOString()
    };

    writeDb(db, "qb_connected");
    res.redirect("/");
  } catch (e) {
    console.error(e.response?.data || e.message);
    res.status(500).send("QB connection failed");
  }
});

app.get("/qb/review", (req, res) => {
  const db = readDb();
  const date = cleanString(req.query.date) || db.dailySetup.date || todayString();

  res.json({
    configured: qbConfigured(),
    connected: !!db.settings.quickbooks.connected,
    invoices: quickbooksReviewForDate(db, date)
  });
});

app.post("/qb/send-day", async (req, res) => {
  const db = readDb();
  const date = cleanString(req.body.date) || db.dailySetup.date || todayString();

  if (!db.settings.quickbooks.connected) {
    return res.status(400).json({ error: "QuickBooks is not connected" });
  }

  const jobs = activeJobsForDate(db, date)
    .map(j => hydrateJob(j, db))
    .filter(j => (j.status === "finished" || j.status === "archived") && j.quickbooksStatus !== "sent");

  let sent = 0;
  let failed = 0;
  const results = [];

  for (const job of jobs) {
    try {
      const invoice = await qbCreateInvoiceForJob(db, job);

      const dbJob = db.jobs.find(x => String(x.id) === String(job.id));
      if (dbJob) {
        dbJob.quickbooksStatus = "sent";
        dbJob.quickbooksInvoiceId = invoice.Id || null;
      }

      sent++;
      results.push({ id: job.id, success: true, invoiceId: invoice.Id || null });
    } catch (e) {
      const dbJob = db.jobs.find(x => String(x.id) === String(job.id));
      if (dbJob) dbJob.quickbooksStatus = "error";

      failed++;
      results.push({ id: job.id, success: false, error: e.response?.data || e.message });
    }
  }

  writeDb(db, "qb_sent");
  res.json({ sent, failed, results });
});


/* ---------- EMPLOYEES / TIME CLOCK ---------- */

app.get("/employees", (_req, res) => {
  const db = readDb();
  res.json((db.employees || []).filter(e => e.active !== false));
});

app.post("/employees", (req, res) => {
  const db = readDb();
  const employee = { id: nextNumericId(db.employees || []), name: cleanString(req.body.name), active: true, createdAt: new Date().toISOString() };
  if (!employee.name) return res.status(400).json({ error: "Employee name is required" });
  db.employees ||= [];
  db.employees.push(employee);
  writeDb(db, "employee_added");
  res.json(employee);
});

app.put("/employees/:id", (req, res) => {
  const db = readDb();
  const employee = (db.employees || []).find(e => String(e.id) === String(req.params.id));
  if (!employee) return res.status(404).json({ error: "Employee not found" });
  if (req.body.name !== undefined) employee.name = cleanString(req.body.name);
  if (req.body.active !== undefined) employee.active = !!req.body.active;
  writeDb(db, "employee_updated");
  res.json(employee);
});

app.delete("/employees/:id", (req, res) => {
  const db = readDb();
  const employee = (db.employees || []).find(e => String(e.id) === String(req.params.id));
  if (!employee) return res.status(404).json({ error: "Employee not found" });
  employee.active = false;
  writeDb(db, "employee_removed");
  res.json({ ok: true });
});

app.get("/time-clock", (req, res) => {
  const db = readDb();
  const date = cleanString(req.query.date);
  let rows = db.timeClockEntries || [];
  if (date) rows = rows.filter(r => r.date === date);
  res.json(rows);
});

app.post("/time-clock/clock-in", (req, res) => {
  const db = readDb();
  const employeeId = String(req.body.employeeId || "");
  const employee = (db.employees || []).find(e => String(e.id) === employeeId && e.active !== false);
  if (!employee) return res.status(404).json({ error: "Employee not found" });
  db.timeClockEntries ||= [];
  const active = db.timeClockEntries.find(r => String(r.employeeId) === employeeId && !r.clockOut);
  if (active) return res.status(400).json({ error: `${employee.name} is already clocked in` });
  const row = { id: nextNumericId(db.timeClockEntries), employeeId: employee.id, name: employee.name, clockIn: new Date().toISOString(), clockOut: null, minutes: null, date: cleanString(req.body.date) || db.dailySetup.date || todayString() };
  db.timeClockEntries.push(row);
  writeDb(db, "employee_clocked_in");
  res.json(row);
});

app.post("/time-clock/clock-out", (req, res) => {
  const db = readDb();
  const employeeId = String(req.body.employeeId || "");
  const employee = (db.employees || []).find(e => String(e.id) === employeeId);
  if (!employee) return res.status(404).json({ error: "Employee not found" });
  const row = (db.timeClockEntries || []).find(r => String(r.employeeId) === employeeId && !r.clockOut);
  if (!row) return res.status(400).json({ error: `${employee.name} is not currently clocked in` });
  row.clockOut = new Date().toISOString();
  row.minutes = Math.round((new Date(row.clockOut) - new Date(row.clockIn)) / 60000);
  writeDb(db, "employee_clocked_out");
  res.json(row);
});

/* ---------- ROOT / HEALTH ---------- */

app.get("/health", (_req, res) => {
  res.json({ ok: true, time: new Date().toISOString() });
});

initializeDatabaseBackedState()
  .then(() => {
    server.listen(PORT, "0.0.0.0", () => {
      console.log(`Swyft Ops V5 Postgres server running on port ${PORT}`);
    });
  })
  .catch(err => {
    console.error("Failed to initialize database-backed app:", err);
    process.exit(1);
  });
