import os
import sys

import duckdb


def main() -> int:
    db_path = os.environ.get("TFM_DUCKDB_PATH", "/opt/datalake/warehouse/tfm.duckdb")
    schema = os.environ.get("TFM_DUCKDB_SCHEMA", "main")
    table = os.environ.get("TFM_DUCKDB_TABLE", "v_dashboard_model_metrics")
    full = f"{schema}.{table}"

    print(f"[verify_duckdb_view] db_path={db_path}")
    print(f"[verify_duckdb_view] object={full}")

    if not os.path.exists(db_path):
        print(f"[verify_duckdb_view] ERROR: missing {db_path}")
        return 2

    con = duckdb.connect(db_path, read_only=True)

    exists = con.execute(
        "select count(*) from information_schema.tables where table_schema=? and table_name=?",
        [schema, table],
    ).fetchone()[0]

    if exists == 0:
        print(f"[verify_duckdb_view] ERROR: {full} does not exist")
        return 3

    rows = con.execute(f"select count(*) from {full}").fetchone()[0]
    sample = con.execute(f"select * from {full} limit 5").fetchall()

    print(f"[verify_duckdb_view] OK: rows({full})={rows}")
    print(f"[verify_duckdb_view] sample(5)={sample}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
