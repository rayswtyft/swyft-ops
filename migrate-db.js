require("dotenv").config();
const { query, pool } = require("./database");

async function migrate() {
  await query(`CREATE TABLE IF NOT EXISTS contractors (
  id TEXT PRIMARY KEY,
  company_name TEXT,
  contact_name TEXT,
  email TEXT,
  phone TEXT,
  billing_address TEXT,
  payment_terms TEXT,
  created_at TEXT
)`);

  await query(`CREATE TABLE IF NOT EXISTS contractor_addresses (
  id SERIAL PRIMARY KEY,
  contractor_id TEXT REFERENCES contractors(id) ON DELETE CASCADE,
  address TEXT
)`);

  await query(`CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  contractor_id TEXT,
  service_address TEXT,
  service_date TEXT,
  notes TEXT,
  created_at TEXT,
  sort_order INTEGER DEFAULT 0,
  deleted_at TEXT,
  archived_at TEXT,
  finished_at TEXT,
  quickbooks_status TEXT DEFAULT 'not_sent',
  quickbooks_invoice_id TEXT,
  from_estimate_id TEXT,
  open_status TEXT DEFAULT 'single_day'
)`);

  await query(`CREATE TABLE IF NOT EXISTS job_services (
  id SERIAL PRIMARY KEY,
  job_id TEXT REFERENCES jobs(id) ON DELETE CASCADE,
  service_index INTEGER DEFAULT 0,
  category TEXT,
  subtype TEXT,
  crew_size NUMERIC,
  hours_manual NUMERIC,
  linear_feet NUMERIC,
  junk_load TEXT,
  junk_price NUMERIC,
  service_date TEXT,
  on_my_way_time TEXT,
  arrived_time TEXT,
  start_time TEXT,
  end_time TEXT,
  materials_used JSONB DEFAULT '{}'::jsonb,
  inventory_deducted_at TEXT
)`);

  await query(`CREATE TABLE IF NOT EXISTS job_photos (
  id TEXT PRIMARY KEY,
  job_id TEXT REFERENCES jobs(id) ON DELETE CASCADE,
  url TEXT,
  tag TEXT,
  caption TEXT,
  created_at TEXT
)`);

  await query(`CREATE TABLE IF NOT EXISTS estimates (
  id TEXT PRIMARY KEY,
  contractor_id TEXT,
  service_address TEXT,
  notes TEXT,
  status TEXT DEFAULT 'open',
  created_at TEXT,
  converted_job_id TEXT
)`);

  await query(`CREATE TABLE IF NOT EXISTS estimate_services (
  id SERIAL PRIMARY KEY,
  estimate_id TEXT REFERENCES estimates(id) ON DELETE CASCADE,
  service_index INTEGER DEFAULT 0,
  category TEXT,
  subtype TEXT,
  crew_size NUMERIC,
  hours_manual NUMERIC,
  linear_feet NUMERIC,
  junk_load TEXT,
  junk_price NUMERIC,
  service_date TEXT,
  on_my_way_time TEXT,
  arrived_time TEXT,
  start_time TEXT,
  end_time TEXT,
  materials_used JSONB DEFAULT '{}'::jsonb,
  inventory_deducted_at TEXT
)`);

  await query(`CREATE TABLE IF NOT EXISTS inventory (
  id TEXT PRIMARY KEY,
  item_key TEXT UNIQUE,
  name TEXT,
  quantity NUMERIC,
  unit TEXT,
  reorder_point NUMERIC,
  active BOOLEAN DEFAULT true,
  display JSONB
)`);

  await query(`CREATE TABLE IF NOT EXISTS employees (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  active BOOLEAN DEFAULT true,
  created_at TEXT
)`);

  await query(`CREATE TABLE IF NOT EXISTS time_clock_entries (
  id TEXT PRIMARY KEY,
  employee_id TEXT,
  employee_name TEXT,
  clock_in TEXT,
  clock_out TEXT,
  minutes NUMERIC,
  entry_date TEXT
)`);

  await query(`CREATE TABLE IF NOT EXISTS daily_setup (
  id INTEGER PRIMARY KEY,
  setup_date TEXT,
  crew_size NUMERIC DEFAULT 1,
  lunch_breaks JSONB DEFAULT '[]'::jsonb,
  active_lunch_start TEXT
)`);

  await query(`CREATE TABLE IF NOT EXISTS daily_checklist_state (
  id SERIAL PRIMARY KEY,
  check_date TEXT,
  item TEXT,
  checked BOOLEAN DEFAULT false,
  UNIQUE(check_date, item)
)`);

  await query(`CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value JSONB
)`);

  await query(`CREATE TABLE IF NOT EXISTS quickbooks_tokens (
  id INTEGER PRIMARY KEY,
  connected BOOLEAN DEFAULT false,
  realm_id TEXT,
  access_token TEXT,
  refresh_token TEXT,
  expires_at TEXT,
  refresh_expires_at TEXT,
  last_connected_at TEXT
)`);

  console.log("Postgres migration complete.");
}

migrate()
  .then(() => pool.end())
  .catch(err => {
    console.error("Migration failed:", err);
    pool.end();
    process.exit(1);
  });
