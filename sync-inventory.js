// sync-inventory.js — run once on Render to sync Base44 inventory to Postgres
// Usage: node sync-inventory.js
require('dotenv').config();
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');

const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

const items = [
  // VAN TOOLS
  { name: "Screwdrivers", category: "tool", location: "van", quantity: 4, unit: "pieces", reorder_point: null },
  { name: "Metal scraper", category: "tool", location: "van", quantity: 3, unit: "pieces", reorder_point: null },
  { name: "9-in-1 tools", category: "tool", location: "van", quantity: 3, unit: "pieces", reorder_point: null },
  { name: "Toilet brushes", category: "tool", location: "van", quantity: 3, unit: "pieces", reorder_point: null },
  { name: "Buckets", category: "tool", location: "van", quantity: 4, unit: "pieces", reorder_point: null },
  { name: "Goggles", category: "tool", location: "van", quantity: 6, unit: "pairs", reorder_point: null },
  { name: "Squeegees", category: "tool", location: "van", quantity: 5, unit: "pieces", reorder_point: null },
  { name: "Shoe protectors", category: "tool", location: "van", quantity: 2, unit: "boxes", reorder_point: 1 },
  { name: "Staple gun", category: "tool", location: "van", quantity: 1, unit: "pieces", reorder_point: null },
  { name: "Impact driver", category: "tool", location: "van", quantity: 1, unit: "pieces", reorder_point: null },
  { name: "Drill", category: "tool", location: "van", quantity: 1, unit: "pieces", reorder_point: null },
  { name: "Recip saws", category: "tool", location: "van", quantity: 2, unit: "pieces", reorder_point: null },
  { name: "Crowbars", category: "tool", location: "van", quantity: 4, unit: "pieces", reorder_point: null },
  { name: "Pliers", category: "tool", location: "van", quantity: 4, unit: "pieces", reorder_point: null },
  { name: "Rags", category: "tool", location: "van", quantity: 12, unit: "pieces", reorder_point: null },
  { name: "Brooms", category: "tool", location: "van", quantity: 4, unit: "pieces", reorder_point: null },
  { name: "Standing dustpan", category: "tool", location: "van", quantity: 1, unit: "pieces", reorder_point: null },
  { name: "Handheld dustpan", category: "tool", location: "van", quantity: 2, unit: "pieces", reorder_point: null },
  { name: "Mop sticks", category: "tool", location: "van", quantity: 2, unit: "pieces", reorder_point: null },
  { name: "Mop heads", category: "tool", location: "van", quantity: 4, unit: "pieces", reorder_point: null },
  { name: "Shoulder vacuum", category: "tool", location: "van", quantity: 1, unit: "pieces", reorder_point: null },
  { name: "Carpet vacuum", category: "tool", location: "van", quantity: 1, unit: "pieces", reorder_point: null },
  { name: "Buckets", category: "tool", location: "van", quantity: 4, unit: "pieces", reorder_point: null },

  // VAN SUPPLIES
  { name: "Floor cleaner", category: "supply", location: "van", quantity: 3, unit: "bottles", reorder_point: 5 },
  { name: "Masks", category: "supply", location: "van", quantity: 2, unit: "boxes", reorder_point: 1 },
  { name: "Latex gloves", category: "supply", location: "van", quantity: 3, unit: "boxes", reorder_point: 2 },
  { name: "Ear protection", category: "supply", location: "van", quantity: 1, unit: "boxes", reorder_point: 1 },
  { name: "Zippers", category: "supply", location: "van", quantity: 12, unit: "pieces", reorder_point: 10 },
  { name: "Clorox", category: "supply", location: "van", quantity: 1, unit: "bottles", reorder_point: 2 },
  { name: "Goof Off Hardwood", category: "supply", location: "van", quantity: 1, unit: "bottles", reorder_point: 2 },
  { name: "Goo Gone", category: "supply", location: "van", quantity: 4, unit: "bottles", reorder_point: 5 },
  { name: "Goof Off", category: "supply", location: "van", quantity: 2, unit: "bottles", reorder_point: 5 },
  { name: "Tile grout cleaner", category: "supply", location: "van", quantity: 2, unit: "bottles", reorder_point: 1 },
  { name: "Mixed fuel", category: "supply", location: "van", quantity: 1, unit: "bottles", reorder_point: 1 },
  { name: "Dish soap", category: "supply", location: "van", quantity: 1, unit: "bottles", reorder_point: 2 },
  { name: "Garbage bags", category: "supply", location: "van", quantity: 3, unit: "boxes", reorder_point: 2 },
  { name: "Green scrub pads", category: "supply", location: "van", quantity: 2, unit: "pieces", reorder_point: 50 },
  { name: "Spray bottles", category: "supply", location: "van", quantity: 5, unit: "pieces", reorder_point: 5 },
  { name: "Windex", category: "supply", location: "van", quantity: 1, unit: "bottles", reorder_point: 4 },
  { name: "Bleach powder", category: "supply", location: "van", quantity: 1, unit: "containers", reorder_point: 6 },
  { name: "White tape", category: "supply", location: "van", quantity: 2, unit: "rolls", reorder_point: null },
  { name: "10 mil plastic", category: "supply", location: "van", quantity: 1, unit: "rolls", reorder_point: 0.5 },
  { name: "4 mil plastic", category: "supply", location: "van", quantity: 0.31, unit: "rolls", reorder_point: 0.5 },
  { name: "Guardrail tape", category: "supply", location: "van", quantity: 3, unit: "rolls", reorder_point: 5 },
  { name: "Metal razor", category: "supply", location: "van", quantity: 1, unit: "pieces", reorder_point: 10 },
  { name: "Paper towel", category: "supply", location: "van", quantity: 2, unit: "rolls", reorder_point: 2 },
  { name: "Magic erasers", category: "supply", location: "van", quantity: 12, unit: "pieces", reorder_point: 5 },
  { name: "Contractor paper", category: "supply", location: "van", quantity: 1, unit: "rolls", reorder_point: null },
  { name: "Dish sponge", category: "supply", location: "van", quantity: 0, unit: "pieces", reorder_point: null },
  { name: "Oven cleaner", category: "supply", location: "van", quantity: 0, unit: "bottles", reorder_point: null },

  // TRUCK TOOLS
  { name: "Hammer", category: "tool", location: "truck", quantity: 1, unit: "pieces", reorder_point: null },
  { name: "Jackhammer", category: "tool", location: "truck", quantity: 1, unit: "pieces", reorder_point: null },
  { name: "Shovels", category: "tool", location: "truck", quantity: 2, unit: "pieces", reorder_point: null },
  { name: "Straps and tie-downs", category: "tool", location: "truck", quantity: 6, unit: "pieces", reorder_point: null },

  // WAREHOUSE TOOLS
  { name: "Electric chainsaw", category: "tool", location: "warehouse", quantity: 1, unit: "pieces", reorder_point: null },
  { name: "Gas chainsaw", category: "tool", location: "warehouse", quantity: 1, unit: "pieces", reorder_point: null },
  { name: "Angle grinder", category: "tool", location: "warehouse", quantity: 1, unit: "pieces", reorder_point: null },
  { name: "Medium jackhammer (red)", category: "tool", location: "warehouse", quantity: 1, unit: "pieces", reorder_point: null },
  { name: "Bosch mini jackhammer", category: "tool", location: "warehouse", quantity: 1, unit: "pieces", reorder_point: null },
  { name: "Big jackhammer", category: "tool", location: "warehouse", quantity: 1, unit: "pieces", reorder_point: null },
  { name: "Electric concrete cutter", category: "tool", location: "warehouse", quantity: 1, unit: "pieces", reorder_point: null },
  { name: "Fans", category: "tool", location: "warehouse", quantity: 3, unit: "pieces", reorder_point: null },
  { name: "Wheelbarrow", category: "tool", location: "warehouse", quantity: 1, unit: "pieces", reorder_point: null },
  { name: "Trowels", category: "tool", location: "warehouse", quantity: 2, unit: "pieces", reorder_point: null },
  { name: "Rags", category: "tool", location: "warehouse", quantity: 12, unit: "pieces", reorder_point: null },
  { name: "Thick plastic roll", category: "tool", location: "warehouse", quantity: 1, unit: "rolls", reorder_point: null },
  { name: "Shovels", category: "tool", location: "warehouse", quantity: 2, unit: "pieces", reorder_point: null },
  { name: "Hose", category: "tool", location: "warehouse", quantity: 1, unit: "pieces", reorder_point: null },
  { name: "Standing dustpans", category: "tool", location: "warehouse", quantity: 2, unit: "pieces", reorder_point: null },
  { name: "Dustpans", category: "tool", location: "warehouse", quantity: 2, unit: "pieces", reorder_point: null },
  { name: "Squeegees", category: "tool", location: "warehouse", quantity: 6, unit: "pieces", reorder_point: null },
  { name: "Steamer", category: "tool", location: "warehouse", quantity: 1, unit: "pieces", reorder_point: null },
  { name: "Bauer batteries", category: "tool", location: "warehouse", quantity: 4, unit: "pieces", reorder_point: null },
  { name: "Bauer chargers", category: "tool", location: "warehouse", quantity: 2, unit: "pieces", reorder_point: null },
  { name: "Multi tools", category: "tool", location: "warehouse", quantity: 2, unit: "pieces", reorder_point: null },

  // WAREHOUSE SUPPLIES
  { name: "Contractor bags", category: "supply", location: "warehouse", quantity: 400, unit: "bags", reorder_point: 500 },
  { name: "Medium duty ramboard", category: "supply", location: "warehouse", quantity: 7, unit: "rolls", reorder_point: 5 },
  { name: "Heavy duty ramboard", category: "supply", location: "warehouse", quantity: 7, unit: "rolls", reorder_point: 5 },
  { name: "Contractor paper", category: "supply", location: "warehouse", quantity: 5, unit: "rolls", reorder_point: 5 },
  { name: "Guardrail tape", category: "supply", location: "warehouse", quantity: 4, unit: "rolls", reorder_point: 5 },
  { name: "Spray bottles", category: "supply", location: "warehouse", quantity: 5, unit: "pieces", reorder_point: 5 },
  { name: "Windex", category: "supply", location: "warehouse", quantity: 9, unit: "bottles", reorder_point: 4 },
  { name: "White tape", category: "supply", location: "warehouse", quantity: 52, unit: "rolls", reorder_point: 5 },
  { name: "Duct tape", category: "supply", location: "warehouse", quantity: 4, unit: "rolls", reorder_point: 5 },
  { name: "Zippers", category: "supply", location: "warehouse", quantity: 16, unit: "pieces", reorder_point: 10 },
  { name: "Bleach powder", category: "supply", location: "warehouse", quantity: 8, unit: "containers", reorder_point: 6 },
  { name: "Floor cleaner", category: "supply", location: "warehouse", quantity: 10, unit: "bottles", reorder_point: 5 },
  { name: "Goo Gone", category: "supply", location: "warehouse", quantity: 3, unit: "bottles", reorder_point: 5 },
  { name: "Goof Off Hardwood", category: "supply", location: "warehouse", quantity: 4, unit: "bottles", reorder_point: 5 },
  { name: "Goof Off", category: "supply", location: "warehouse", quantity: 1, unit: "bottles", reorder_point: 5 },
  { name: "Dish soap", category: "supply", location: "warehouse", quantity: 1, unit: "bottles", reorder_point: 2 },
  { name: "Clorox", category: "supply", location: "warehouse", quantity: 1, unit: "bottles", reorder_point: 2 },
  { name: "Latex gloves", category: "supply", location: "warehouse", quantity: 2, unit: "boxes", reorder_point: 2 },
  { name: "Magic erasers", category: "supply", location: "warehouse", quantity: 6, unit: "pieces", reorder_point: 5 },
  { name: "Metal sponges", category: "supply", location: "warehouse", quantity: 11, unit: "pieces", reorder_point: 10 },
  { name: "Garbage bags", category: "supply", location: "warehouse", quantity: 2, unit: "boxes", reorder_point: 2 },
  { name: "Green scrub pads", category: "supply", location: "warehouse", quantity: 384, unit: "pieces", reorder_point: 50 },
  { name: "Mixed fuel", category: "supply", location: "warehouse", quantity: 1, unit: "bottles", reorder_point: 1 },
  { name: "Concrete bags", category: "supply", location: "warehouse", quantity: 2, unit: "bags", reorder_point: null },
  { name: "10 mil plastic", category: "supply", location: "warehouse", quantity: 0, unit: "rolls", reorder_point: 0.5 },
  { name: "Safety gloves", category: "supply", location: "warehouse", quantity: 10, unit: "pairs", reorder_point: 4 },
];

async function run() {
  const client = await pool.connect();
  try {
    // Ensure columns exist
    await client.query(`ALTER TABLE inventory ADD COLUMN IF NOT EXISTS category TEXT DEFAULT 'supply'`);
    await client.query(`ALTER TABLE inventory ADD COLUMN IF NOT EXISTS location TEXT DEFAULT 'van'`);

    // Clear existing inventory and stock
    await client.query(`DELETE FROM inventory_stock`);
    await client.query(`DELETE FROM inventory`);

    for (const item of items) {
      const id = uuidv4();
      const key = item.name.toLowerCase().replace(/[^a-z0-9]+/g, '_') + '_' + item.location;
      await client.query(
        `INSERT INTO inventory (id, item_key, name, category, location, item_type, quantity, unit, reorder_point, active)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,true)
         ON CONFLICT (item_key) DO UPDATE SET name=$3, category=$4, location=$5, item_type=$6, quantity=$7, unit=$8, reorder_point=$9, active=true`,
        [id, key, item.name, item.category, item.location, item.category === 'tool' ? 'tool' : 'consumable', item.quantity, item.unit, item.reorder_point]
      );
      await client.query(
        `INSERT INTO inventory_stock (item_id, location, quantity) VALUES ($1,$2,$3)
         ON CONFLICT (item_id, location) DO UPDATE SET quantity=$3`,
        [id, item.location, item.quantity]
      );
    }
    console.log(`✅ Synced ${items.length} inventory items successfully`);

    // Tell the live server to reload from Postgres
    const APP_URL = process.env.APP_URL || "https://swyft-ops-1.onrender.com";
    try {
      const http = require("https");
      await new Promise((resolve) => {
        const req = http.request(APP_URL + "/admin/reload-db", { method: "POST", headers: { "Content-Type": "application/json" } }, (res) => {
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
