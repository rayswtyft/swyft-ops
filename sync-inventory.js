// sync-inventory.js — run once on Render to sync inventory to Postgres
// Items with same name are ONE inventory row, with per-location stock entries
require('dotenv').config();
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');

const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

// ── Raw inventory — list every item/location combo ────────────────────────────
const rawItems = [
  // VAN TOOLS
  { name: "Screwdrivers",       category: "tool",    location: "van",       quantity: 4,    unit: "pieces" },
  { name: "Metal scraper",      category: "tool",    location: "van",       quantity: 3,    unit: "pieces" },
  { name: "9-in-1 tools",       category: "tool",    location: "van",       quantity: 3,    unit: "pieces" },
  { name: "Toilet brushes",     category: "tool",    location: "van",       quantity: 3,    unit: "pieces" },
  { name: "Buckets",            category: "tool",    location: "van",       quantity: 4,    unit: "pieces" },
  { name: "Goggles",            category: "tool",    location: "van",       quantity: 6,    unit: "pairs"  },
  { name: "Squeegees",          category: "tool",    location: "van",       quantity: 5,    unit: "pieces" },
  { name: "Shoe protectors",    category: "tool",    location: "van",       quantity: 2,    unit: "boxes",  reorder_point: 1 },
  { name: "Staple gun",         category: "tool",    location: "van",       quantity: 1,    unit: "pieces" },
  { name: "Impact driver",      category: "tool",    location: "van",       quantity: 1,    unit: "pieces" },
  { name: "Drill",              category: "tool",    location: "van",       quantity: 1,    unit: "pieces" },
  { name: "Recip saws",         category: "tool",    location: "van",       quantity: 2,    unit: "pieces" },
  { name: "Crowbars",           category: "tool",    location: "van",       quantity: 4,    unit: "pieces" },
  { name: "Pliers",             category: "tool",    location: "van",       quantity: 4,    unit: "pieces" },
  { name: "Rags",               category: "tool",    location: "van",       quantity: 12,   unit: "pieces" },
  { name: "Brooms",             category: "tool",    location: "van",       quantity: 4,    unit: "pieces" },
  { name: "Standing dustpan",   category: "tool",    location: "van",       quantity: 1,    unit: "pieces" },
  { name: "Handheld dustpan",   category: "tool",    location: "van",       quantity: 2,    unit: "pieces" },
  { name: "Mop sticks",         category: "tool",    location: "van",       quantity: 2,    unit: "pieces" },
  { name: "Mop heads",          category: "tool",    location: "van",       quantity: 4,    unit: "pieces" },
  { name: "Shoulder vacuum",    category: "tool",    location: "van",       quantity: 1,    unit: "pieces" },
  { name: "Carpet vacuum",      category: "tool",    location: "van",       quantity: 1,    unit: "pieces" },

  // VAN SUPPLIES
  { name: "Floor cleaner",      category: "supply",  location: "van",       quantity: 3,    unit: "bottles",    reorder_point: 5 },
  { name: "Masks",              category: "supply",  location: "van",       quantity: 2,    unit: "boxes",      reorder_point: 1 },
  { name: "Latex gloves",       category: "supply",  location: "van",       quantity: 3,    unit: "boxes",      reorder_point: 2 },
  { name: "Ear protection",     category: "supply",  location: "van",       quantity: 1,    unit: "boxes",      reorder_point: 1 },
  { name: "Zippers",            category: "supply",  location: "van",       quantity: 12,   unit: "pieces",     reorder_point: 10 },
  { name: "Clorox",             category: "supply",  location: "van",       quantity: 1,    unit: "bottles",    reorder_point: 2 },
  { name: "Goof Off Hardwood",  category: "supply",  location: "van",       quantity: 1,    unit: "bottles",    reorder_point: 2 },
  { name: "Goo Gone",           category: "supply",  location: "van",       quantity: 4,    unit: "bottles",    reorder_point: 5 },
  { name: "Goof Off",           category: "supply",  location: "van",       quantity: 2,    unit: "bottles",    reorder_point: 5 },
  { name: "Tile grout cleaner", category: "supply",  location: "van",       quantity: 2,    unit: "bottles",    reorder_point: 1 },
  { name: "Mixed fuel",         category: "supply",  location: "van",       quantity: 1,    unit: "bottles",    reorder_point: 1 },
  { name: "Dish soap",          category: "supply",  location: "van",       quantity: 1,    unit: "bottles",    reorder_point: 2 },
  { name: "Garbage bags",       category: "supply",  location: "van",       quantity: 3,    unit: "boxes",      reorder_point: 2 },
  { name: "Green scrub pads",   category: "supply",  location: "van",       quantity: 2,    unit: "pieces",     reorder_point: 50 },
  { name: "Spray bottles",      category: "supply",  location: "van",       quantity: 5,    unit: "pieces",     reorder_point: 5 },
  { name: "Windex",             category: "supply",  location: "van",       quantity: 1,    unit: "bottles",    reorder_point: 4 },
  { name: "Bleach powder",      category: "supply",  location: "van",       quantity: 1,    unit: "containers", reorder_point: 6 },
  { name: "White tape",         category: "supply",  location: "van",       quantity: 2,    unit: "rolls" },
  { name: "10 mil plastic",     category: "supply",  location: "van",       quantity: 1,    unit: "rolls",      reorder_point: 0.5 },
  { name: "4 mil plastic",      category: "supply",  location: "van",       quantity: 0.31, unit: "rolls",      reorder_point: 0.5 },
  { name: "Guardrail tape",     category: "supply",  location: "van",       quantity: 3,    unit: "rolls",      reorder_point: 5 },
  { name: "Metal razor",        category: "supply",  location: "van",       quantity: 1,    unit: "pieces",     reorder_point: 10 },
  { name: "Paper towel",        category: "supply",  location: "van",       quantity: 2,    unit: "rolls",      reorder_point: 2 },
  { name: "Magic erasers",      category: "supply",  location: "van",       quantity: 12,   unit: "pieces",     reorder_point: 5 },
  { name: "Contractor paper",   category: "supply",  location: "van",       quantity: 1,    unit: "rolls" },
  { name: "Dish sponge",        category: "supply",  location: "van",       quantity: 0,    unit: "pieces" },
  { name: "Oven cleaner",       category: "supply",  location: "van",       quantity: 0,    unit: "bottles" },
  { name: "Scrub Daddies",      category: "supply",  location: "van",       quantity: 2,    unit: "pieces",     reorder_point: 2 },

  // TRUCK TOOLS
  { name: "Hammer",             category: "tool",    location: "truck",     quantity: 1,    unit: "pieces" },
  { name: "Jackhammer",         category: "tool",    location: "truck",     quantity: 1,    unit: "pieces" },
  { name: "Shovels",            category: "tool",    location: "truck",     quantity: 2,    unit: "pieces" },
  { name: "Straps and tie-downs",category:"tool",    location: "truck",     quantity: 6,    unit: "pieces" },

  // WAREHOUSE TOOLS
  { name: "Electric chainsaw",         category: "tool", location: "warehouse", quantity: 1, unit: "pieces" },
  { name: "Gas chainsaw",              category: "tool", location: "warehouse", quantity: 1, unit: "pieces" },
  { name: "Angle grinder",             category: "tool", location: "warehouse", quantity: 1, unit: "pieces" },
  { name: "Medium jackhammer (red)",   category: "tool", location: "warehouse", quantity: 1, unit: "pieces" },
  { name: "Bosch mini jackhammer",     category: "tool", location: "warehouse", quantity: 1, unit: "pieces" },
  { name: "Big jackhammer",            category: "tool", location: "warehouse", quantity: 1, unit: "pieces" },
  { name: "Electric concrete cutter",  category: "tool", location: "warehouse", quantity: 1, unit: "pieces" },
  { name: "Fans",                      category: "tool", location: "warehouse", quantity: 3, unit: "pieces" },
  { name: "Wheelbarrow",               category: "tool", location: "warehouse", quantity: 1, unit: "pieces" },
  { name: "Trowels",                   category: "tool", location: "warehouse", quantity: 2, unit: "pieces" },
  { name: "Rags",                      category: "tool", location: "warehouse", quantity: 12, unit: "pieces" },
  { name: "Thick plastic roll",        category: "tool", location: "warehouse", quantity: 1,  unit: "rolls" },
  { name: "Shovels",                   category: "tool", location: "warehouse", quantity: 2,  unit: "pieces" },
  { name: "Hose",                      category: "tool", location: "warehouse", quantity: 1,  unit: "pieces" },
  { name: "Standing dustpan",          category: "tool", location: "warehouse", quantity: 2,  unit: "pieces" },
  { name: "Handheld dustpan",          category: "tool", location: "warehouse", quantity: 2,  unit: "pieces" },
  { name: "Squeegees",                 category: "tool", location: "warehouse", quantity: 6,  unit: "pieces" },
  { name: "Steamer",                   category: "tool", location: "warehouse", quantity: 1,  unit: "pieces" },
  { name: "Bauer batteries",           category: "tool", location: "warehouse", quantity: 4,  unit: "pieces" },
  { name: "Bauer chargers",            category: "tool", location: "warehouse", quantity: 2,  unit: "pieces" },
  { name: "Multi tools",               category: "tool", location: "warehouse", quantity: 2,  unit: "pieces" },

  // WAREHOUSE SUPPLIES
  { name: "Contractor bags",     category: "supply", location: "warehouse", quantity: 400, unit: "bags",    reorder_point: 500 },
  { name: "Medium duty ramboard",category: "supply", location: "warehouse", quantity: 7,   unit: "rolls",   reorder_point: 5 },
  { name: "Heavy duty ramboard", category: "supply", location: "warehouse", quantity: 7,   unit: "rolls",   reorder_point: 5 },
  { name: "Contractor paper",    category: "supply", location: "warehouse", quantity: 5,   unit: "rolls",   reorder_point: 5 },
  { name: "Guardrail tape",      category: "supply", location: "warehouse", quantity: 4,   unit: "rolls",   reorder_point: 5 },
  { name: "Spray bottles",       category: "supply", location: "warehouse", quantity: 5,   unit: "pieces",  reorder_point: 5 },
  { name: "Windex",              category: "supply", location: "warehouse", quantity: 9,   unit: "bottles", reorder_point: 4 },
  { name: "White tape",          category: "supply", location: "warehouse", quantity: 52,  unit: "rolls",   reorder_point: 5 },
  { name: "Duct tape",           category: "supply", location: "warehouse", quantity: 10,  unit: "rolls",   reorder_point: 5 },
  { name: "Floor cleaner",       category: "supply", location: "warehouse", quantity: 12,  unit: "bottles", reorder_point: 5 },
  { name: "Clorox",              category: "supply", location: "warehouse", quantity: 12,  unit: "bottles", reorder_point: 2 },
  { name: "Dish soap",           category: "supply", location: "warehouse", quantity: 6,   unit: "bottles", reorder_point: 2 },
  { name: "Goo Gone",            category: "supply", location: "warehouse", quantity: 12,  unit: "bottles", reorder_point: 5 },
  { name: "Goof Off",            category: "supply", location: "warehouse", quantity: 6,   unit: "bottles", reorder_point: 5 },
  { name: "Goof Off Hardwood",   category: "supply", location: "warehouse", quantity: 4,   unit: "bottles", reorder_point: 2 },
  { name: "Garbage bags",        category: "supply", location: "warehouse", quantity: 10,  unit: "boxes",   reorder_point: 2 },
  { name: "Latex gloves",        category: "supply", location: "warehouse", quantity: 12,  unit: "boxes",   reorder_point: 2 },
  { name: "Green scrub pads",    category: "supply", location: "warehouse", quantity: 100, unit: "pieces",  reorder_point: 50 },
  { name: "Magic erasers",       category: "supply", location: "warehouse", quantity: 48,  unit: "pieces",  reorder_point: 5 },
  { name: "Bleach powder",       category: "supply", location: "warehouse", quantity: 6,   unit: "containers", reorder_point: 6 },
  { name: "10 mil plastic",      category: "supply", location: "warehouse", quantity: 3,   unit: "rolls",   reorder_point: 0.5 },
  { name: "Zippers",             category: "supply", location: "warehouse", quantity: 50,  unit: "pieces",  reorder_point: 10 },
  { name: "Mixed fuel",          category: "supply", location: "warehouse", quantity: 2,   unit: "bottles", reorder_point: 1 },
  { name: "Concrete bags",       category: "supply", location: "warehouse", quantity: 20,  unit: "bags" },
  { name: "Recip saw blades",    category: "supply", location: "warehouse", quantity: 10,  unit: "pieces",  reorder_point: 5 },
];

// ── Consolidate by name — one inventory row, multiple stock rows ──────────────
function consolidate(rawItems) {
  const map = new Map();
  for (const item of rawItems) {
    const key = item.name.toLowerCase().trim();
    if (!map.has(key)) {
      map.set(key, {
        id: uuidv4(),
        name: item.name,
        item_key: key.replace(/[^a-z0-9]+/g, '_'),
        category: item.category,
        item_type: item.category === 'tool' ? 'tool' : 'consumable',
        unit: item.unit,
        reorder_point: item.reorder_point ?? null,
        // primary location = first seen
        primary_location: item.location,
        stock: {}
      });
    }
    const entry = map.get(key);
    // Accumulate stock per location (in case same name+location appears twice)
    entry.stock[item.location] = (entry.stock[item.location] || 0) + item.quantity;
    // Prefer non-null reorder_point
    if (item.reorder_point != null && entry.reorder_point == null) {
      entry.reorder_point = item.reorder_point;
    }
  }
  return Array.from(map.values());
}

async function run() {
  const client = await pool.connect();
  try {
    const items = consolidate(rawItems);
    console.log(`📦 Consolidated ${rawItems.length} raw entries → ${items.length} unique items`);

    // Clear existing
    await client.query(`DELETE FROM inventory_stock`);
    await client.query(`DELETE FROM inventory`);

    for (const item of items) {
      const totalQty = Object.values(item.stock).reduce((a, b) => a + b, 0);

      await client.query(
        `INSERT INTO inventory (id, item_key, name, category, location, item_type, quantity, unit, reorder_point, active)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,true)
         ON CONFLICT (item_key) DO UPDATE SET name=$3, category=$4, location=$5, item_type=$6, quantity=$7, unit=$8, reorder_point=$9, active=true`,
        [item.id, item.item_key, item.name, item.category, item.primary_location, item.item_type, totalQty, item.unit, item.reorder_point]
      );

      for (const [loc, qty] of Object.entries(item.stock)) {
        await client.query(
          `INSERT INTO inventory_stock (item_id, location, quantity) VALUES ($1,$2,$3)
           ON CONFLICT (item_id, location) DO UPDATE SET quantity=$3`,
          [item.id, loc, qty]
        );
      }
    }

    console.log(`✅ Synced ${items.length} inventory items (${rawItems.length} raw entries merged)`);

    // Tell the live server to reload from Postgres
    const APP_URL = process.env.APP_URL || "https://swyft-ops-1.onrender.com";
    try {
      const https = require("https");
      await new Promise((resolve) => {
        const req = https.request(APP_URL + "/admin/reload-db", { method: "POST", headers: { "Content-Type": "application/json" } }, (res) => {
          let body = "";
          res.on("data", d => body += d);
          res.on("end", () => { console.log("🔄 Server DB reloaded:", body); resolve(); });
        });
        req.on("error", (e) => { console.warn("⚠️  Could not ping reload endpoint:", e.message); resolve(); });
        req.end();
      });
    } catch(e) { console.warn("⚠️  Reload ping failed:", e.message); }

  } finally {
    client.release();
    await pool.end();
  }
}

run().catch(e => { console.error(e); process.exit(1); });
