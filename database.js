require("dotenv").config();

const { Pool } = require("pg");

if (!process.env.DATABASE_URL) {
  console.warn("DATABASE_URL is not set. Postgres-backed routes will not start correctly.");
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes("render.com")
    ? { rejectUnauthorized: false }
    : false
});

async function query(text, params) {
  return pool.query(text, params);
}

module.exports = {
  pool,
  query
};
