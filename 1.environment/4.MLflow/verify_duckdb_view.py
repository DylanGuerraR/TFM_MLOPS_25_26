#!/usr/bin/env python3
import os
import sys

import duckdb


DB_PATH = os.environ.get("TFM_DUCKDB_PATH", "/opt/datalake/warehouse/tfm.duckdb")
SCHEMA = os.environ.get("TFM_DUCKDB_SCHEMA", "main")
TABLE = os.environ.get("TFM_DUCKDB_TABLE", "v_dashboard_model_metrics")


def main() -> int:
    if not os.path.exists(DB_PATH):
        print(f"[verify_duckdb_view] ERROR: missing duckdb file: {DB_PATH}", file=sys.stderr)
        return 1

    con = duckdb.connect(DB_PATH, read_only=True)

    # DuckDB information_schema.tables works
    sql_exists = (
        "select count(*) "
        "from information_schema.tables "
        f"where table_schema='{SCHEMA}' and table_name='{TABLE}'"
    )
    exists = con.execute(sql_exists).fetchone()[0]
    if exists == 0:
        print(
            f"[verify_duckdb_view] ERROR: missing {SCHEMA}.{TABLE} in {DB_PATH}",
            file=sys.stderr,
        )
        return 2

    rows = con.execute(f"select count(*) from {SCHEMA}.{TABLE}").fetchone()[0]
    print(f"[verify_duckdb_view] OK: {SCHEMA}.{TABLE} rows={rows}")

    sample = con.execute(f"select * from {SCHEMA}.{TABLE} limit 5").fetchall()
    print("[verify_duckdb_view] sample (limit 5):")
    for r in sample:
        print("  ", r)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
