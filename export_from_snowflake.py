#!/usr/bin/env python3
"""
Export a Snowflake query to CSV/CSV.GZ without row limits.

Env vars:
  SNOWFLAKE_ACCOUNT      (required) e.g. xy12345.ca-central-1
  SNOWFLAKE_USER         (required)
  SNOWFLAKE_PASSWORD     (required)  # swap to key-pair if preferred
  SNOWFLAKE_WAREHOUSE    (required)
  SNOWFLAKE_ROLE         (optional)
  SNOWFLAKE_DATABASE     (optional)
  SNOWFLAKE_SCHEMA       (optional)
  SNOWFLAKE_SQL_FILE     (default: sql/query.sql)
  OUTPUT_PATH            (default: data/export_YYYYMMDD.csv.gz)
  CHUNK_SIZE             (default: 20000)

Usage:
  python scripts/export_from_snowflake.py
"""
import csv, gzip, os, sys
from datetime import datetime

try:
    import snowflake.connector
except Exception as e:
    print("Missing dependency: snowflake-connector-python", file=sys.stderr)
    raise

def env(name, required=False, default=None):
    v = os.getenv(name, default)
    if required and (v is None or str(v).strip() == ""):
        print(f"Missing required env var: {name}", file=sys.stderr); sys.exit(1)
    return v

def open_writer(path):
    if path.endswith(".gz"):
        return gzip.open(path, "wt", newline="", encoding="utf-8")
    return open(path, "w", newline="", encoding="utf-8")

def main():
    sql_file   = env("SNOWFLAKE_SQL_FILE", default="sql/query.sql")
    output     = env("OUTPUT_PATH", default=f"data/export_{datetime.utcnow().strftime('%Y%m%d')}.csv.gz")
    chunk_size = int(env("CHUNK_SIZE", default="20000"))
    role       = env("SNOWFLAKE_ROLE", default=None)
    database   = env("SNOWFLAKE_DATABASE", default=None)
    schema     = env("SNOWFLAKE_SCHEMA", default=None)
    warehouse  = env("SNOWFLAKE_WAREHOUSE", required=True)

    if not os.path.exists(sql_file):
        print(f"SQL file not found: {sql_file}", file=sys.stderr); sys.exit(1)
    with open(sql_file, "r", encoding="utf-8") as f:
        sql = f.read().strip()
        if not sql:
            print("SQL file is empty.", file=sys.stderr); sys.exit(1)

    conn = snowflake.connector.connect(
        account=env("SNOWFLAKE_ACCOUNT", required=True),
        user=env("SNOWFLAKE_USER", required=True),
        password=env("SNOWFLAKE_PASSWORD", required=True),
        warehouse=warehouse,
        role=role,
        database=database,
        schema=schema,
        autocommit=True,
        client_session_keep_alive=True,
    )

    total = 0
    cur = None
    try:
        cur = conn.cursor()
        # Ensure context if provided
        if database: cur.execute(f'USE DATABASE "{database}"')
        if schema:   cur.execute(f'USE SCHEMA "{schema}"')
        if role:     cur.execute(f'USE ROLE "{role}"')
        cur.execute(f'USE WAREHOUSE "{warehouse}"')

        cur.execute(sql)
        cols = [c[0] for c in cur.description]

        os.makedirs(os.path.dirname(output), exist_ok=True)
        with open_writer(output) as f:
            w = csv.writer(f)
            w.writerow(cols)
            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows: break
                w.writerows(rows)
                total += len(rows)

        print(f"✅ Wrote {total:,} rows to {output}")
    finally:
        try:
            if cur is not None:
                cur.close()
        except Exception:
            pass
        conn.close()

if __name__ == "__main__":
    main()
