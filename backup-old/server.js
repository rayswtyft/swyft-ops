const express = require("express");
const path = require("path");
const fs = require("fs");
const multer = require("multer");
const PDFDocument = require("pdfkit");
const archiver = require("archiver");

const app = express();
const DB_FILE = path.join(__dirname, "db.json");
const UPLOADS_DIR = path.join(__dirname, "uploads");
const EXPORTS_DIR = path.join(__dirname, "exports");
const PUBLIC_DIR = path.join(__dirname, "public");

if (!fs.existsSync(UPLOADS_DIR)) fs.mkdirSync(UPLOADS_DIR, { recursive: true });
if (!fs.existsSync(EXPORTS_DIR)) fs.mkdirSync(EXPORTS_DIR, { recursive: true });
if (!fs.existsSync(PUBLIC_DIR)) fs.mkdirSync(PUBLIC_DIR, { recursive: true });

const storage = multer.diskStorage({
  destination: function (_req, _file, cb) {
    cb(null, UPLOADS_DIR);
  },
  filename: function (_req, file, cb) {
    const safeName = (file.originalname || "photo").replace(/[^a-zA-Z0-9.\-_]/g, "_");
    cb(null, `${Date.now()}-${safeName}`);
  }
});
const upload = multer({ storage });

app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true }));
app.use(express.static(PUBLIC_DIR));
app.use("/uploads", express.static(UPLOADS_DIR));
app.use("/exports", express.static(EXPORTS_DIR));

const CHECKLISTS = {
  "Cleaning|Between tenant cleaning": [
    "bleach powder","box of latex gloves","Broom","buckets","Carpet vacuum",
    "clean mop heads","Clorox bathroom spray","Commit toilet cleaner","contractor bags",
    "Dish soap","dish sponge","Dustpan","Floor cleaner","garbage bags",
    "goo gone","goof off","green score pad","magic erasers","metal razor",
    "metal sponges","mop sticks","Oven cleaner","plastic razor","rags","roll of paper towel",
    "Scrub brush","scrub daddy","Shoulder vacuum","soap, water bottles","Squeegee",
    "Toilet brush","water pics"
  ],
  "Cleaning|Deep clean": [
    "bleach powder","box of latex gloves","Broom","buckets","Carpet vacuum",
    "clean mop heads","Clorox bathroom spray","Commit toilet cleaner","contractor bags",
    "Dish soap","dish sponge","Dustpan","Floor cleaner","garbage bags",
    "goo gone","goof off","green score pad","magic erasers","metal razor",
    "metal sponges","mop sticks","Oven cleaner","plastic razor","rags","roll of paper towel",
    "Scrub brush","scrub daddy","Shoulder vacuum","soap, water bottles","Squeegee",
    "Toilet brush","water pics"
  ],
  "Cleaning|General cleaning": [
    "Bleach powder","Broom","Carpet vacuum","clean mop heads","Clorox bathroom spray",
    "Dustpan","Floor cleaner","garbage bags","Mop sticks","rags",
    "roll of paper towel","Shoulder vacuum","Toilet brush"
  ],
  "Cleaning|Janitorial cleaning": [
    "Bleach powder","Broom","Carpet vacuum","clean mop heads","Clorox bathroom spray",
    "Dustpan","Floor cleaner","garbage bags","Mop sticks","rags",
    "roll of paper towel","Shoulder vacuum","Toilet brush"
  ],
  "Cleaning|Post construction cleanup": [
    "Construction broom","Contractor bags","garbage bags","Shovel"
  ],
  "Cleaning|Site cleanup": [
    "Construction broom","Contractor bags","garbage bags","Shovel"
  ],
  "Construction|Demo": [
    "crowbars","Drill","Hammers","Multi tool","Recip saw"
  ],
  "Construction|Concrete cutting": [
    "Chisel","Concrete Saw","Ear protection","Goggles","Jackhammer","Masks","Shovel"
  ],
  "Construction|Concrete fill with rebar": [
    "10 mil vapor barrier","Buckets","Concrete","Dirt compactor","Mud mixer","Rebar","Shovel","Trowel"
  ],
  "Construction|Concrete fill without rebar": [
    "10 mil vapor barrier","Buckets","Concrete","Dirt compactor","Mud mixer","Shovel","Trowel"
  ],
  "Labor|General labor": [
    "work gloves","broom","shovel","basic hand tools","safety glasses"
  ],
  "Junk|Junk removal": [
    "truck readiness","straps and tie-downs","safety gloves","contractor bags","broom"
  ]
};

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
  { key: "contractor_bags", label: "Contractor bags", unitLabel: "qty", price: 1, wholeOnly: true, inventoryName: "Contractor Bags" },
  { key: "zipper", label: "Zipper", unitLabel: "qty", price: 12, wholeOnly: true, inventoryName: "Zippers" },
  { key: "heavy_ramboard", label: "Heavy duty ramboard", unitLabel: "quarter rolls", price: 18.75, wholeOnly: false, inventoryName: "Heavy Ramboard" },
  { key: "medium_ramboard", label: "Medium duty ramboard", unitLabel: "quarter rolls", price: 13.75, wholeOnly: false, inventoryName: "Medium Ramboard" },
  { key: "contractor_paper", label: "Contractor paper", unitLabel: "quarter rolls", price: 7.5, wholeOnly: false, inventoryName: "Contractor Paper" }
];

const DEFAULT_INVENTORY = [
  { id: 1, name: "Bleach Powder", quantity: 12, unit: "containers", reorderPoint: 4 },
  { id: 2, name: "Box of Latex Gloves", quantity: 2, unit: "boxes", reorderPoint: 1 },
  { id: 3, name: "Broom", quantity: 6, unit: "pieces", reorderPoint: 3 },
  { id: 4, name: "Buckets", quantity: 4, unit: "pieces", reorderPoint: 0 },
  { id: 5, name: "Carpet Vacuum", quantity: 2, unit: "pieces", reorderPoint: 0 },
  { id: 6, name: "Clean Mop Heads", quantity: 8, unit: "pieces", reorderPoint: 6 },
  { id: 7, name: "Clorox Bathroom Spray", quantity: 6, unit: "bottles", reorderPoint: 3 },
  { id: 8, name: "Contractor Bags", quantity: 550, unit: "bags", reorderPoint: 500, displayUnit: "boxes", unitsPerDisplay: 50 },
  { id: 9, name: "Dish Soap", quantity: 5, unit: "bottles", reorderPoint: 3 },
  { id: 10, name: "Dish Sponge", quantity: 6, unit: "pieces", reorderPoint: 3 },
  { id: 11, name: "Dustpan", quantity: 6, unit: "pieces", reorderPoint: 3 },
  { id: 12, name: "Floor Cleaner", quantity: 11, unit: "bottles", reorderPoint: 0 },
  { id: 13, name: "Goo Gone", quantity: 5, unit: "bottles", reorderPoint: 3 },
  { id: 14, name: "Goof Off", quantity: 6, unit: "bottles", reorderPoint: 3 },
  { id: 15, name: "Green Scrub Pad", quantity: 3, unit: "pieces", reorderPoint: 2 },
  { id: 16, name: "Magic Erasers", quantity: 12, unit: "pieces", reorderPoint: 3 },
  { id: 17, name: "Metal Razors", quantity: 50, unit: "pieces", reorderPoint: 10 },
  { id: 18, name: "Metal Sponges", quantity: 6, unit: "pieces", reorderPoint: 3 },
  { id: 19, name: "Mop Sticks", quantity: 3, unit: "pieces", reorderPoint: 0 },
  { id: 20, name: "Oven Cleaner", quantity: 6, unit: "bottles", reorderPoint: 5 },
  { id: 21, name: "Plastic Razors", quantity: 3, unit: "pieces", reorderPoint: 3 },
  { id: 22, name: "Rags", quantity: 48, unit: "pieces", reorderPoint: 20 },
  { id: 23, name: "Roll of Paper Towel", quantity: 8, unit: "rolls", reorderPoint: 12 },
  { id: 24, name: "Scrub Brush", quantity: 3, unit: "pieces", reorderPoint: 3 },
  { id: 25, name: "Scrub Daddy", quantity: 6, unit: "pieces", reorderPoint: 3 },
  { id: 26, name: "Shoulder Vacuum", quantity: 2, unit: "pieces", reorderPoint: 0 },
  { id: 27, name: "Soap, Water Bottles", quantity: 3, unit: "sets", reorderPoint: 2 },
  { id: 28, name: "Squeegee", quantity: 4, unit: "pieces", reorderPoint: 3 },
  { id: 29, name: "Toilet Brush", quantity: 12, unit: "pieces", reorderPoint: 6 },
  { id: 30, name: "Water Pics", quantity: 0, unit: "pieces", reorderPoint: 0 },
  { id: 31, name: "Zippers", quantity: 0, unit: "pieces", reorderPoint: 0 },
  { id: 32, name: "Heavy Ramboard", quantity: 0, unit: "quarter rolls", reorderPoint: 0 },
  { id: 33, name: "Medium Ramboard", quantity: 0, unit: "quarter rolls", reorderPoint: 0 },
  { id: 34, name: "Contractor Paper", quantity: 0, unit: "quarter rolls", reorderPoint: 0 }
];

function todayString() {
  return new Date().toISOString().slice(0, 10);
}

function ensureDb() {
  if (!fs.existsSync(DB_FILE)) {
    fs.writeFileSync(DB_FILE, JSON.stringify({
      contractors: [],
      jobs: [],
      estimates: [],
      inventory: DEFAULT_INVENTORY,
      inventoryLink: "",
      dailySetup: {
        date: todayString(),
        crewSize: 1,
        lunchBreaks: [],
        activeLunchStart: null,
        dailyChecklistState: {}
      }
    }, null, 2));
  }
}

function readDb() {
  ensureDb();
  const raw = fs.readFileSync(DB_FILE, "utf8");
  const db = JSON.parse(raw || "{}");

  db.contractors ||= [];
  db.jobs ||= [];
  db.estimates ||= [];
  db.inventory ||= DEFAULT_INVENTORY;
  db.inventoryLink ||= "";
  db.dailySetup ||= {};
  db.dailySetup.date ||= todayString();
  db.dailySetup.crewSize ||= 1;
  db.dailySetup.lunchBreaks ||= [];
  db.dailySetup.activeLunchStart ||= null;
  db.dailySetup.dailyChecklistState ||= {};

  db.contractors = db.contractors.map(c => ({
    ...c,
    serviceAddresses: Array.isArray(c.serviceAddresses) ? c.serviceAddresses : []
  }));

  db.jobs = db.jobs.map(job => ({
    ...job,
    status: job.status || inferJobStatus(job),
    finishedAt: job.finishedAt || null,
    archivedAt: job.archivedAt || null,
    deletedAt: job.deletedAt || null,
    sortOrder: Number(job.sortOrder || 0),
    services: Array.isArray(job.services) ? job.services : [],
    photos: Array.isArray(job.photos) ? job.photos : []
  }));

  db.estimates = db.estimates.map(est => ({
    ...est,
    status: est.status || "open",
    services: Array.isArray(est.services) ? est.services : []
  }));

  db.inventory = db.inventory.map(item => ({
    id: item.id || Date.now(),
    name: item.name || "",
    quantity: Number(item.quantity || 0),
    unit: item.unit || "pieces",
    reorderPoint: Number(item.reorderPoint || 0),
    displayUnit: item.displayUnit || null,
    unitsPerDisplay: item.unitsPerDisplay ? Number(item.unitsPerDisplay) : null
  }));

  return db;
}

function writeDb(db) {
  fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));
}

function nextId(items) {
  if (!items.length) return 1;
  return Math.max(...items.map(i => Number(i.id) || 0)) + 1;
}

function rateForService(service) {
  if (service.category === "Cleaning") return 32;
  if (service.category === "Construction" && service.subtype === "Demo") return 35;
  if (service.category === "Labor") return 35;
  return 0;
}

function isHourly(service) {
  return (
    service.category === "Cleaning" ||
    (service.category === "Construction" && service.subtype === "Demo") ||
    service.category === "Labor"
  );
}

function isConcrete(service) {
  return service.category === "Construction" &&
    (
      service.subtype === "Concrete cutting" ||
      service.subtype === "Concrete fill with rebar" ||
      service.subtype === "Concrete fill without rebar"
    );
}

function cleaningSuppliesAutoCharge(service) {
  return service.category === "Cleaning" && service.subtype !== "Site cleanup" ? 45 : 0;
}

function normalizeMaterials(rawMaterials = {}) {
  const out = {};
  for (const def of MATERIAL_DEFS) {
    let val = Number(rawMaterials[def.key] || 0);
    if (def.wholeOnly) val = Math.floor(val);
    out[def.key] = val;
  }
  return out;
}

function materialsTotal(materials = {}, service = null) {
  let total = 0;
  for (const def of MATERIAL_DEFS) {
    total += Number(materials[def.key] || 0) * def.price;
  }
  if (service) total += cleaningSuppliesAutoCharge(service);
  return Number(total.toFixed(2));
}

function materialBreakdown(service) {
  const materials = normalizeMaterials(service.materialsUsed || {});
  const rows = MATERIAL_DEFS
    .map(def => {
      const qty = Number(materials[def.key] || 0);
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

  const autoCleaning = cleaningSuppliesAutoCharge(service);
  if (autoCleaning > 0) {
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

function sumRangesMinutes(ranges = []) {
  return Number(
    ranges.reduce((sum, r) => {
      if (!r.start || !r.end) return sum;
      return sum + ((new Date(r.end) - new Date(r.start)) / 1000 / 60);
    }, 0).toFixed(2)
  );
}

function calcHoursWorked(service) {
  if (service.startTime && service.endTime) {
    const grossMinutes = (new Date(service.endTime) - new Date(service.startTime)) / 1000 / 60;
    return Number((Math.max(0, grossMinutes) / 60).toFixed(2));
  }
  return Number(service.hoursManual || 0);
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
  const base = calcBaseServiceTotal(service);
  const materials = materialsTotal(service.materialsUsed || {}, service);
  return Number((base + materials).toFixed(2));
}

function normalizePhoto(photo) {
  return {
    id: photo.id || Date.now(),
    tag: photo.tag || "before",
    caption: photo.caption || "",
    url: photo.url || "",
    createdAt: photo.createdAt || new Date().toISOString()
  };
}

function normalizeService(raw, defaultCrewSize) {
  const service = {
    category: raw.category || "Cleaning",
    subtype: raw.subtype || "General cleaning",
    crewSize: Number(raw.crewSize || defaultCrewSize || 1),
    hoursManual: Number(raw.hoursManual || 0),
    onMyWayTime: raw.onMyWayTime || null,
    arrivedTime: raw.arrivedTime || null,
    startTime: raw.startTime || null,
    endTime: raw.endTime || null,
    downtimes: Array.isArray(raw.downtimes) ? raw.downtimes : [],
    activeDowntimeStart: raw.activeDowntimeStart || null,
    junkLoad: raw.junkLoad || "",
    junkPrice: Number(raw.junkPrice || 0),
    linearFeet: Number(raw.linearFeet || 0),
    materialsUsed: normalizeMaterials(raw.materialsUsed || {})
  };

  service.travelMinutes = service.onMyWayTime && service.arrivedTime
    ? Number((((new Date(service.arrivedTime) - new Date(service.onMyWayTime)) / 1000 / 60)).toFixed(2))
    : 0;

  service.hoursWorked = calcHoursWorked(service);
  service.materialsBreakdown = materialBreakdown(service);
  service.materialsTotal = materialsTotal(service.materialsUsed, service);
  service.baseTotal = calcBaseServiceTotal(service);
  service.total = calcServiceTotal(service);

  return service;
}

function inferJobStatus(job) {
  if (job.archivedAt) return "archived";
  if (job.deletedAt) return "deleted";
  if (job.finishedAt) return "finished";

  const services = Array.isArray(job.services) ? job.services : [];
  const anyInProgress = services.some(s => s.startTime && !s.endTime);
  if (anyInProgress) return "in_progress";

  const anyOnWay = services.some(s => s.onMyWayTime && !s.arrivedTime);
  if (anyOnWay) return "on_the_way";

  const anyStarted = services.some(s => s.startTime || s.endTime || s.arrivedTime);
  if (anyStarted) return "active";

  return "scheduled";
}

function hydrateInventoryItem(item) {
  const lowStock = Number(item.reorderPoint || 0) > 0 && Number(item.quantity || 0) <= Number(item.reorderPoint || 0);

  let displayQuantity = item.quantity;
  let displayLabel = item.unit;

  if (item.displayUnit && item.unitsPerDisplay) {
    displayQuantity = Number((item.quantity / item.unitsPerDisplay).toFixed(2));
    displayLabel = item.displayUnit;
  }

  return {
    ...item,
    lowStock,
    displayQuantity,
    displayLabel
  };
}

function hydrateJob(job, db) {
  const contractor = db.contractors.find(c => c.id === job.contractorId) || null;
  const defaultCrewSize = db.dailySetup.crewSize || 1;
  const services = (job.services || []).map(s => normalizeService(s, defaultCrewSize));
  const photos = (job.photos || []).map(normalizePhoto);

  const totalHours = Number(services.reduce((sum, s) => sum + s.hoursWorked, 0).toFixed(2));
  const totalCost = Number(services.reduce((sum, s) => sum + s.total, 0).toFixed(2));
  const totalTravelMinutes = Number(services.reduce((sum, s) => sum + (s.travelMinutes || 0), 0).toFixed(2));
  const status = inferJobStatus({ ...job, services });

  return {
    ...job,
    contractor,
    services,
    photos,
    status,
    totalHours,
    totalCost,
    totalTravelMinutes
  };
}

function hydrateEstimate(est, db) {
  const contractor = db.contractors.find(c => c.id === est.contractorId) || null;
  const defaultCrewSize = db.dailySetup.crewSize || 1;
  const services = (est.services || []).map(s => normalizeService(s, defaultCrewSize));
  const totalCost = Number(services.reduce((sum, s) => sum + s.total, 0).toFixed(2));

  return {
    ...est,
    contractor,
    services,
    totalCost
  };
}

function getDailySetupHydrated(db) {
  return {
    ...db.dailySetup,
    lunchMinutes: sumRangesMinutes(db.dailySetup.lunchBreaks || [])
  };
}

function dailyChecklistForDate(date, db) {
  const items = new Set();
  db.jobs
    .filter(job => job.serviceDate === date && !job.archivedAt && !job.deletedAt)
    .forEach(job => {
      (job.services || []).forEach(service => {
        const key = `${service.category}|${service.subtype}`;
        (CHECKLISTS[key] || []).forEach(item => items.add(item));
      });
    });
  return Array.from(items).sort((a, b) => a.localeCompare(b));
}

function buildChecklistWithState(date, db) {
  const items = dailyChecklistForDate(date, db);
  const state = db.dailySetup.dailyChecklistState?.[date] || {};
  return items.map(item => ({
    item,
    checked: !!state[item]
  }));
}

function activeJobsForDate(db, date) {
  return db.jobs
    .filter(job => job.serviceDate === date && !job.archivedAt && !job.deletedAt)
    .sort((a, b) => (a.sortOrder || 0) - (b.sortOrder || 0) || b.id - a.id);
}

function nextSortOrderForDate(db, date) {
  const jobs = activeJobsForDate(db, date);
  if (!jobs.length) return 1;
  return Math.max(...jobs.map(j => Number(j.sortOrder || 0))) + 1;
}

function deductInventoryFromMaterials(db, materialsUsed) {
  const normalized = normalizeMaterials(materialsUsed || {});
  for (const def of MATERIAL_DEFS) {
    const qty = Number(normalized[def.key] || 0);
    if (!qty) continue;

    const item = db.inventory.find(i => (i.name || "").toLowerCase() === def.inventoryName.toLowerCase());
    if (item) {
      item.quantity = Number((item.quantity - qty).toFixed(2));
      if (item.quantity < 0) item.quantity = 0;
    }
  }
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

    job.services.forEach((s) => {
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
    doc.text(`Date: ${est.createdAt ? est.createdAt.slice(0, 10) : todayString()}`);
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

    est.services.forEach((s) => {
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

app.get("/meta", (_req, res) => {
  res.json({
    serviceOptions: SERVICE_OPTIONS,
    materialDefs: MATERIAL_DEFS
  });
});

app.get("/settings", (_req, res) => {
  const db = readDb();
  res.json({
    inventoryLink: db.inventoryLink || ""
  });
});

app.post("/settings/inventory-link", (req, res) => {
  const db = readDb();
  db.inventoryLink = String(req.body.inventoryLink || "").trim();
  writeDb(db);
  res.json({ inventoryLink: db.inventoryLink });
});

app.get("/daily-setup", (_req, res) => {
  const db = readDb();
  res.json(getDailySetupHydrated(db));
});

app.post("/daily-setup", (req, res) => {
  const db = readDb();
  db.dailySetup.date = req.body.date || todayString();
  db.dailySetup.crewSize = Number(req.body.crewSize || 1);
  writeDb(db);
  res.json(getDailySetupHydrated(db));
});

app.post("/daily-lunch-start", (_req, res) => {
  const db = readDb();
  if (db.dailySetup.activeLunchStart) {
    return res.status(400).json({ error: "Lunch already started" });
  }
  db.dailySetup.activeLunchStart = new Date().toISOString();
  writeDb(db);
  res.json(getDailySetupHydrated(db));
});

app.post("/daily-lunch-end", (_req, res) => {
  const db = readDb();
  if (!db.dailySetup.activeLunchStart) {
    return res.status(400).json({ error: "Lunch not started" });
  }
  db.dailySetup.lunchBreaks ||= [];
  db.dailySetup.lunchBreaks.push({
    start: db.dailySetup.activeLunchStart,
    end: new Date().toISOString()
  });
  db.dailySetup.activeLunchStart = null;
  writeDb(db);
  res.json(getDailySetupHydrated(db));
});

app.get("/daily-checklist", (req, res) => {
  const db = readDb();
  const date = req.query.date || db.dailySetup.date || todayString();
  res.json({
    date,
    crewSize: db.dailySetup.crewSize,
    lunchMinutes: sumRangesMinutes(db.dailySetup.lunchBreaks || []),
    items: buildChecklistWithState(date, db)
  });
});

app.post("/daily-checklist/toggle", (req, res) => {
  const db = readDb();
  const date = req.body.date || db.dailySetup.date || todayString();
  const item = req.body.item;
  const checked = !!req.body.checked;

  db.dailySetup.dailyChecklistState ||= {};
  db.dailySetup.dailyChecklistState[date] ||= {};
  db.dailySetup.dailyChecklistState[date][item] = checked;

  writeDb(db);
  res.json({ ok: true });
});

app.get("/dashboard", (req, res) => {
  const db = readDb();
  const date = req.query.date || db.dailySetup.date || todayString();
  const jobs = activeJobsForDate(db, date).map(job => hydrateJob(job, db));
  const lowStockCount = db.inventory.filter(i => Number(i.reorderPoint || 0) > 0 && Number(i.quantity || 0) <= Number(i.reorderPoint || 0)).length;

  res.json({
    date,
    totalJobs: jobs.length,
    totalBillableHours: Number(jobs.reduce((sum, j) => sum + j.totalHours, 0).toFixed(2)),
    totalTravelMinutes: Number(jobs.reduce((sum, j) => sum + j.totalTravelMinutes, 0).toFixed(2)),
    lunchMinutes: sumRangesMinutes(db.dailySetup.lunchBreaks || []),
    scheduled: jobs.filter(j => j.status === "scheduled").length,
    onTheWay: jobs.filter(j => j.status === "on_the_way").length,
    inProgress: jobs.filter(j => j.status === "in_progress" || j.status === "active").length,
    finished: jobs.filter(j => j.status === "finished").length,
    lowStockCount
  });
});

app.get("/reporting", (_req, res) => {
  const db = readDb();
  const jobs = db.jobs
    .filter(job => !job.deletedAt)
    .map(job => hydrateJob(job, db));

  const byDate = {};
  jobs.forEach(job => {
    byDate[job.serviceDate] ||= { travelMinutes: 0 };
    byDate[job.serviceDate].travelMinutes += job.totalTravelMinutes || 0;
  });

  const rows = Object.entries(byDate)
    .map(([date, vals]) => ({
      date,
      travelMinutes: Number(vals.travelMinutes.toFixed(2))
    }))
    .sort((a, b) => b.date.localeCompare(a.date));

  res.json(rows);
});

app.get("/history", (_req, res) => {
  const db = readDb();
  const rows = db.jobs
    .filter(job => job.archivedAt || job.finishedAt)
    .map(job => hydrateJob(job, db))
    .sort((a, b) => {
      const da = a.archivedAt || a.finishedAt || a.createdAt || "";
      const dbb = b.archivedAt || b.finishedAt || b.createdAt || "";
      return dbb.localeCompare(da);
    });

  res.json(rows);
});

app.get("/inventory", (_req, res) => {
  const db = readDb();
  res.json(db.inventory.map(hydrateInventoryItem));
});

app.get("/inventory/low-stock", (_req, res) => {
  const db = readDb();
  const lowStockItems = db.inventory
    .map(hydrateInventoryItem)
    .filter(item => item.lowStock);

  res.json(lowStockItems);
});

app.post("/inventory/update", (req, res) => {
  const db = readDb();
  const item = db.inventory.find(i => Number(i.id) === Number(req.body.id));
  if (!item) return res.status(404).json({ error: "Item not found" });

  item.quantity = Number(req.body.quantity || 0);
  writeDb(db);
  res.json(hydrateInventoryItem(item));
});

app.post("/inventory", (req, res) => {
  const db = readDb();
  const newItem = {
    id: nextId(db.inventory),
    name: req.body.name || "",
    quantity: Number(req.body.quantity || 0),
    unit: req.body.unit || "pieces",
    reorderPoint: Number(req.body.reorderPoint || 0),
    displayUnit: req.body.displayUnit || null,
    unitsPerDisplay: req.body.unitsPerDisplay ? Number(req.body.unitsPerDisplay) : null
  };

  db.inventory.push(newItem);
  writeDb(db);
  res.json(hydrateInventoryItem(newItem));
});

app.get("/contractors", (_req, res) => {
  const db = readDb();
  res.json([...db.contractors].sort((a, b) => b.id - a.id));
});

app.post("/contractors", (req, res) => {
  const db = readDb();
  const contractor = {
    id: nextId(db.contractors),
    companyName: req.body.companyName || "",
    contactName: req.body.contactName || "",
    email: req.body.email || "",
    phone: req.body.phone || "",
    billingAddress: req.body.billingAddress || "",
    paymentTerms: req.body.paymentTerms || "",
    serviceAddresses: Array.isArray(req.body.serviceAddresses) ? req.body.serviceAddresses.filter(Boolean) : [],
    createdAt: new Date().toISOString()
  };
  db.contractors.push(contractor);
  writeDb(db);
  res.json(contractor);
});

app.put("/contractors/:id", (req, res) => {
  const db = readDb();
  const contractor = db.contractors.find(c => c.id === Number(req.params.id));
  if (!contractor) return res.status(404).json({ error: "Contractor not found" });

  contractor.companyName = req.body.companyName || "";
  contractor.contactName = req.body.contactName || "";
  contractor.email = req.body.email || "";
  contractor.phone = req.body.phone || "";
  contractor.billingAddress = req.body.billingAddress || "";
  contractor.paymentTerms = req.body.paymentTerms || "";
  contractor.serviceAddresses = Array.isArray(req.body.serviceAddresses)
    ? req.body.serviceAddresses.filter(Boolean)
    : contractor.serviceAddresses || [];

  writeDb(db);
  res.json(contractor);
});

app.get("/jobs", (req, res) => {
  const db = readDb();
  const date = req.query.date || db.dailySetup.date || todayString();

  const jobs = activeJobsForDate(db, date)
    .map(job => hydrateJob(job, db))
    .sort((a, b) => (a.sortOrder || 0) - (b.sortOrder || 0) || b.id - a.id);

  res.json(jobs);
});

app.post("/jobs", (req, res) => {
  const db = readDb();
  const services = Array.isArray(req.body.services) ? req.body.services : [];
  const serviceDate = req.body.serviceDate || db.dailySetup.date || todayString();

  const job = {
    id: nextId(db.jobs),
    contractorId: Number(req.body.contractorId),
    serviceAddress: req.body.serviceAddress || "",
    serviceDate,
    notes: req.body.notes || "",
    status: "scheduled",
    finishedAt: null,
    archivedAt: null,
    deletedAt: null,
    sortOrder: nextSortOrderForDate(db, serviceDate),
    services: services.map(s => normalizeService(s, db.dailySetup.crewSize)),
    photos: [],
    createdAt: new Date().toISOString()
  };

  db.jobs.push(job);
  writeDb(db);
  res.json(hydrateJob(job, db));
});

app.put("/jobs/:id", (req, res) => {
  const db = readDb();
  const job = db.jobs.find(j => j.id === Number(req.params.id));
  if (!job) return res.status(404).json({ error: "Job not found" });

  const oldDate = job.serviceDate;
  const newDate = req.body.serviceDate || job.serviceDate;

  job.serviceAddress = req.body.serviceAddress || job.serviceAddress;
  job.serviceDate = newDate;
  job.notes = req.body.notes || "";

  if (Array.isArray(req.body.services)) {
    job.services = req.body.services.map(s => normalizeService(s, db.dailySetup.crewSize));
  }

  if (oldDate !== newDate) {
    job.sortOrder = nextSortOrderForDate(db, newDate);
  }

  writeDb(db);
  res.json(hydrateJob(job, db));
});

app.post("/jobs/reorder", (req, res) => {
  const db = readDb();
  const date = req.body.date;
  const orderedIds = Array.isArray(req.body.orderedIds) ? req.body.orderedIds.map(Number) : [];

  activeJobsForDate(db, date).forEach(job => {
    const idx = orderedIds.indexOf(job.id);
    if (idx >= 0) job.sortOrder = idx + 1;
  });

  writeDb(db);
  res.json({ ok: true });
});

app.post("/jobs/:id/archive", (req, res) => {
  const db = readDb();
  const job = db.jobs.find(j => j.id === Number(req.params.id));
  if (!job) return res.status(404).json({ error: "Job not found" });

  job.archivedAt = new Date().toISOString();
  writeDb(db);
  res.json({ ok: true });
});

app.delete("/jobs/:id", (req, res) => {
  const db = readDb();
  const job = db.jobs.find(j => j.id === Number(req.params.id));
  if (!job) return res.status(404).json({ error: "Job not found" });

  job.deletedAt = new Date().toISOString();
  writeDb(db);
  res.json({ ok: true });
});

app.post("/jobs/:id/photos", upload.single("photo"), (req, res) => {
  const db = readDb();
  const job = db.jobs.find(j => j.id === Number(req.params.id));
  if (!job) return res.status(404).json({ error: "Job not found" });
  if (!req.file) return res.status(400).json({ error: "No photo uploaded" });

  job.photos ||= [];
  job.photos.push({
    id: Date.now(),
    tag: req.body.tag || "before",
    caption: req.body.caption || "",
    url: `/uploads/${req.file.filename}`,
    createdAt: new Date().toISOString()
  });

  writeDb(db);
  res.json(hydrateJob(job, db));
});

app.delete("/jobs/:jobId/photos/:photoId", (req, res) => {
  const db = readDb();
  const job = db.jobs.find(j => j.id === Number(req.params.jobId));
  if (!job) return res.status(404).json({ error: "Job not found" });

  const photo = (job.photos || []).find(p => Number(p.id) === Number(req.params.photoId));
  if (!photo) return res.status(404).json({ error: "Photo not found" });

  const filePath = path.join(__dirname, photo.url.replace(/^\//, ""));
  if (fs.existsSync(filePath)) fs.unlinkSync(filePath);

  job.photos = job.photos.filter(p => Number(p.id) !== Number(req.params.photoId));
  writeDb(db);
  res.json(hydrateJob(job, db));
});

app.post("/jobs/:id/services/:index/on-my-way", (req, res) => {
  const db = readDb();
  const job = db.jobs.find(j => j.id === Number(req.params.id));
  if (!job) return res.status(404).json({ error: "Job not found" });

  const service = job.services[Number(req.params.index)];
  if (!service) return res.status(404).json({ error: "Service not found" });

  service.onMyWayTime = new Date().toISOString();
  writeDb(db);
  res.json(hydrateJob(job, db));
});

app.post("/jobs/:id/services/:index/arrived", (req, res) => {
  const db = readDb();
  const job = db.jobs.find(j => j.id === Number(req.params.id));
  if (!job) return res.status(404).json({ error: "Job not found" });

  const service = job.services[Number(req.params.index)];
  if (!service) return res.status(404).json({ error: "Service not found" });

  service.arrivedTime = new Date().toISOString();
  writeDb(db);
  res.json(hydrateJob(job, db));
});

app.post("/jobs/:id/services/:index/start", (req, res) => {
  const db = readDb();
  const job = db.jobs.find(j => j.id === Number(req.params.id));
  if (!job) return res.status(404).json({ error: "Job not found" });

  const service = job.services[Number(req.params.index)];
  if (!service) return res.status(404).json({ error: "Service not found" });
  if (!isHourly(service)) return res.status(400).json({ error: "This service does not use a timer" });

  service.startTime = new Date().toISOString();
  service.endTime = null;
  writeDb(db);
  res.json(hydrateJob(job, db));
});

app.post("/jobs/:id/services/:index/stop", (req, res) => {
  const db = readDb();
  const job = db.jobs.find(j => j.id === Number(req.params.id));
  if (!job) return res.status(404).json({ error: "Job not found" });

  const service = job.services[Number(req.params.index)];
  if (!service) return res.status(404).json({ error: "Service not found" });
  if (!isHourly(service)) return res.status(400).json({ error: "This service does not use a timer" });
  if (!service.startTime) return res.status(400).json({ error: "Start the service first" });

  service.endTime = new Date().toISOString();

  if (req.body && req.body.materialsUsed) {
    service.materialsUsed = normalizeMaterials(req.body.materialsUsed);
    deductInventoryFromMaterials(db, service.materialsUsed);
  }

  writeDb(db);
  res.json(hydrateJob(job, db));
});

app.get("/estimates", (_req, res) => {
  const db = readDb();
  res.json(db.estimates.map(est => hydrateEstimate(est, db)).sort((a, b) => b.id - a.id));
});

app.post("/estimates", (req, res) => {
  const db = readDb();
  const services = Array.isArray(req.body.services) ? req.body.services : [];

  const estimate = {
    id: nextId(db.estimates),
    contractorId: Number(req.body.contractorId),
    serviceAddress: req.body.serviceAddress || "",
    notes: req.body.notes || "",
    status: "open",
    services: services.map(s => normalizeService(s, db.dailySetup.crewSize)),
    createdAt: new Date().toISOString()
  };

  db.estimates.push(estimate);
  writeDb(db);
  res.json(hydrateEstimate(estimate, db));
});

app.post("/estimates/:id/convert", (req, res) => {
  const db = readDb();
  const estimate = db.estimates.find(e => e.id === Number(req.params.id));
  if (!estimate) return res.status(404).json({ error: "Estimate not found" });

  const serviceDate = req.body.serviceDate || db.dailySetup.date || todayString();

  const job = {
    id: nextId(db.jobs),
    contractorId: estimate.contractorId,
    serviceAddress: estimate.serviceAddress,
    serviceDate,
    notes: estimate.notes || "",
    status: "scheduled",
    finishedAt: null,
    archivedAt: null,
    deletedAt: null,
    sortOrder: nextSortOrderForDate(db, serviceDate),
    services: (estimate.services || []).map(s => normalizeService(s, db.dailySetup.crewSize)),
    photos: [],
    createdAt: new Date().toISOString(),
    fromEstimateId: estimate.id
  };

  estimate.status = "converted";
  estimate.convertedJobId = job.id;

  db.jobs.push(job);
  writeDb(db);
  res.json(hydrateJob(job, db));
});

app.get("/estimate/:id/pdf", async (req, res) => {
  const db = readDb();
  const raw = db.estimates.find(e => e.id === Number(req.params.id));
  if (!raw) return res.status(404).send("Estimate not found");

  const est = hydrateEstimate(raw, db);
  const filePath = path.join(EXPORTS_DIR, `estimate-${est.id}.pdf`);
  await writeEstimatePdf(est, filePath);
  res.download(filePath);
});

app.get("/finish-day-summary", (req, res) => {
  const db = readDb();
  const date = req.query.date || db.dailySetup.date || todayString();
  const jobs = activeJobsForDate(db, date).map(job => hydrateJob(job, db));

  const summary = {
    date,
    totalJobs: jobs.length,
    totalTravelMinutes: Number(jobs.reduce((sum, j) => sum + j.totalTravelMinutes, 0).toFixed(2)),
    lunchMinutes: sumRangesMinutes(db.dailySetup.lunchBreaks || []),
    totalPhotos: jobs.reduce((sum, j) => sum + (j.photos?.length || 0), 0),
    totalMaterials: Number(
      jobs.reduce((sum, j) => sum + j.services.reduce((s, svc) => s + (svc.materialsTotal || 0), 0), 0).toFixed(2)
    ),
    jobs: jobs.map(j => ({
      id: j.id,
      address: j.serviceAddress,
      status: j.status,
      totalHours: j.totalHours,
      photos: j.photos?.length || 0
    }))
  };

  res.json(summary);
});

app.post("/finish-day", async (req, res) => {
  const db = readDb();
  const date = req.body.date || db.dailySetup.date || todayString();
  const jobs = activeJobsForDate(db, date).map(job => hydrateJob(job, db));

  jobs.forEach(job => {
    const raw = db.jobs.find(j => j.id === job.id);
    if (raw) {
      raw.status = "finished";
      raw.finishedAt = new Date().toISOString();
    }
  });
  writeDb(db);

  const zipName = `invoices-${date}-${Date.now()}.zip`;
  const zipPath = path.join(EXPORTS_DIR, zipName);
  const output = fs.createWriteStream(zipPath);
  const archive = archiver("zip", { zlib: { level: 9 } });

  output.on("close", () => {
    res.json({
      date,
      finishedJobs: jobs.length,
      lunchMinutes: sumRangesMinutes(db.dailySetup.lunchBreaks || []),
      downloadUrl: `/exports/${zipName}`
    });
  });

  archive.on("error", err => {
    res.status(500).json({ error: err.message });
  });

  archive.pipe(output);

  for (const job of jobs) {
    const pdfPath = path.join(EXPORTS_DIR, `invoice-job-${job.id}.pdf`);
    await writeInvoicePdf(job, pdfPath);
    archive.file(pdfPath, { name: `invoice-job-${job.id}.pdf` });
  }

  archive.finalize();
});

app.get("/invoice/:id", (req, res) => {
  const db = readDb();
  const rawJob = db.jobs.find(j => j.id === Number(req.params.id));
  if (!rawJob) return res.send("Job not found");

  const job = hydrateJob(rawJob, db);
  const contractor = job.contractor;

  const rows = job.services.map((s) => {
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
            <div>${contractor?.companyName || ""}</div>
            <div>${contractor?.contactName || ""}</div>
            <div>${contractor?.email || ""}</div>
            <div>${contractor?.phone || ""}</div>
            <div>${contractor?.billingAddress || ""}</div>
            <div><strong>Terms:</strong> ${contractor?.paymentTerms || ""}</div>
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
            <div style="margin-top:16px;">
              <div>Travel: ${job.totalTravelMinutes} min</div>
            </div>
            <div class="total">Total: $${job.totalCost.toFixed(2)}</div>
          </div>

          ${job.photos.length ? `
            <div class="box photos">
              <h3>Job Photos</h3>
              ${job.photos.map(p => `
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

const port = process.env.PORT || 3000;
app.listen(port, "0.0.0.0", () => {
  console.log(`App running on port ${port}`);
});