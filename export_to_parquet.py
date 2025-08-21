#!/usr/bin/env python3
"""
Export a Snowflake query to a single Parquet file using streaming batches.

Env vars:
  SNOWFLAKE_ACCOUNT      (required)
  SNOWFLAKE_USER         (required)
  SNOWFLAKE_PASSWORD     (required)
  SNOWFLAKE_WAREHOUSE    (required)
  SNOWFLAKE_ROLE         (optional)
  SNOWFLAKE_DATABASE     (optional)
  SNOWFLAKE_SCHEMA       (optional)
  SNOWFLAKE_SQL_FILE     (default: sql/query.sql)
  OUTPUT_PATH            (default: data/export.parquet)
  CHUNK_SIZE             (default: 20000)

Requires: pandas, pyarrow, snowflake-connector-python
"""
import os, sys
from datetime import datetime

try:
    import snowflake.connector
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception as e:
    print("Missing dependency. Install: pip install pandas pyarrow snowflake-connector-python", file=sys.stderr)
    raise

def env(name, required=False, default=None):
    v = os.getenv(name, default)
    if required and (v is None or str(v).strip() == ""):
        print(f"Missing required env var: {name}", file=sys.stderr); sys.exit(1)
    return v

def main():
    sql_file   = env("SNOWFLAKE_SQL_FILE", default="sql/query.sql")
    output     = env("OUTPUT_PATH", default="data/export.parquet")
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
    writer = None
    try:
        cur = conn.cursor()
        if database: cur.execute(f'USE DATABASE "{database}"')
        if schema:   cur.execute(f'USE SCHEMA "{schema}"')
        if role:     cur.execute(f'USE ROLE "{role}"')
        cur.execute(f'USE WAREHOUSE "{warehouse}"')

        cur.execute(sql)
        cols = [c[0] for c in cur.description]

        os.makedirs(os.path.dirname(output), exist_ok=True)

        # Stream rows -> DataFrame -> Arrow Table -> Parquet
        while True:
            rows = cur.fetchmany(chunk_size)
            if not rows:
                break
            df = pd.DataFrame.from_records(rows, columns=cols)
            table = pa.Table.from_pandas(df, preserve_index=False)

            if writer is None:
                writer = pq.ParquetWriter(output, table.schema, compression="snappy")
            writer.write_table(table)
            total += len(df)

        if writer is not None:
            writer.close()

        if total == 0:
            # Create an empty parquet with the correct columns (as nulls)
            arrs = [pa.array([], type=pa.null()) for _ in cols]
            schema = pa.schema([(c, pa.null()) for c in cols])
            empty = pa.Table.from_arrays(arrs, names=cols, schema=schema)
            pq.write_table(empty, output)
            print(f"⚠️ Query returned 0 rows. Wrote empty parquet with columns to {output}")
        else:
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
