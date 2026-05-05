require("dotenv").config();

const { Pool } = require("pg");

if (!process.env.DATABASE_URL) {
  console.warn("DATABASE_URL is not set. Postgres-backed routes will not start correctly.");
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes("render.com")
    ? { rejectUnauthorized: false }
    : false,
  max: 8,                   // stay under Render free tier connection limit
  idleTimeoutMillis: 30000, // release idle connections after 30s
  connectionTimeoutMillis: 5000 // fail fast if can't get connection
});

pool.on("error", (err) => {
  console.error("Unexpected Postgres pool error:", err.message);
});

async function query(text, params) {
  return pool.query(text, params);
}

module.exports = {
  pool,
  query
};
