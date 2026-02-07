import glob
import json
import os
import subprocess

import yaml

from superset.app import create_app
from superset.extensions import db

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATASOURCE_YML = os.path.join(BASE_DIR, "datasource.yml")
DATASETS_YML = os.path.join(BASE_DIR, "datasets.yml")
DASHBOARDS_DIR = os.environ.get("SUPERSET_DASHBOARDS_DIR", "/app/dashboards")


def load_yaml(path: str) -> dict:
    if not os.path.exists(path):
        print(f"[apply_assets] missing yaml: {path}")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def upsert_database(payload: dict) -> None:
    # Lazy import: evita "Working outside of application context" en Superset 4.x
    from superset.models.core import Database

    name = payload["database_name"]
    uri = payload["sqlalchemy_uri"]

    expose_in_sqllab = bool(payload.get("expose_in_sqllab", True))
    allow_run_async = bool(payload.get("allow_run_async", False))
    allow_ctas = bool(payload.get("allow_ctas", False))
    allow_cvas = bool(payload.get("allow_cvas", False))
    allow_dml = bool(payload.get("allow_dml", False))

    extra_raw = payload.get("extra", "")
    extra = extra_raw.strip()
    if extra:
        try:
            json.loads(extra)
        except Exception as e:
            raise RuntimeError(f"Invalid JSON in datasource.yml extra for {name}: {e}")

    obj = db.session.query(Database).filter(Database.database_name == name).one_or_none()
    if obj is None:
        obj = Database(database_name=name)
        db.session.add(obj)

    obj.sqlalchemy_uri = uri
    obj.expose_in_sqllab = expose_in_sqllab
    obj.allow_run_async = allow_run_async
    obj.allow_ctas = allow_ctas
    obj.allow_cvas = allow_cvas
    obj.allow_dml = allow_dml
    obj.extra = extra

    db.session.commit()
    print(f"[apply_assets] database upserted: {name}")


def upsert_dataset(db_name: str, table: dict) -> None:
    # Lazy imports por el mismo motivo
    from superset.models.core import Database
    from superset.connectors.sqla.models import SqlaTable

    db_obj = db.session.query(Database).filter(Database.database_name == db_name).one_or_none()
    if not db_obj:
        raise RuntimeError(f"Database not found in Superset: {db_name}")

    table_name = table["table_name"]
    schema = table.get("schema") or None

    ds = (
        db.session.query(SqlaTable)
        .filter(SqlaTable.database_id == db_obj.id)
        .filter(SqlaTable.table_name == table_name)
        .filter(SqlaTable.schema == schema)
        .one_or_none()
    )

    if ds is None:
        ds = SqlaTable(database=db_obj, table_name=table_name, schema=schema)
        db.session.add(ds)

    db.session.commit()
    printable_schema = f"{schema}." if schema else ""
    print(f"[apply_assets] dataset upserted: {db_name}.{printable_schema}{table_name}")


def _run_cmd(cmd: list[str]) -> None:
    # check=True: lanza excepción si el comando falla
    subprocess.run(cmd, check=True)


def import_dashboards() -> None:
    if not os.path.isdir(DASHBOARDS_DIR):
        print(f"[apply_assets] dashboards dir not found: {DASHBOARDS_DIR}")
        return

    zips = sorted(glob.glob(os.path.join(DASHBOARDS_DIR, "*.zip")))
    if not zips:
        print("[apply_assets] no dashboards zip to import")
        return

    # Añadimos -u admin para que el import tenga user context
    candidates = [
        lambda p: ["superset", "import-dashboards", "-p", p, "-u", "admin"],
        lambda p: ["superset", "import-dashboards", "--path", p, "-u", "admin"],
        lambda p: ["superset", "dashboards", "import", "-p", p, "-u", "admin"],
    ]

    failures: list[tuple[str, str]] = []

    for p in zips:
        ok = False
        last_err = None

        for cmd_builder in candidates:
            cmd = cmd_builder(p)
            try:
                print(f"[apply_assets] importing dashboard: {os.path.basename(p)} with: {' '.join(cmd)}")
                _run_cmd(cmd)
                ok = True
                break
            except Exception as e:
                last_err = str(e)

        if not ok:
            failures.append((p, last_err or "unknown error"))
            print(f"[apply_assets] dashboard import failed for {p}: {last_err}")

    # Si falla algo, devolvemos error -> Airflow detecta el fallo
    if failures:
        msg = "\n".join([f"- {path}: {err}" for path, err in failures])
        raise RuntimeError(f"[apply_assets] one or more dashboard imports failed:\n{msg}")


def main() -> int:
    app = create_app()

    with app.app_context():
        ds_cfg = load_yaml(DATASOURCE_YML)
        for d in ds_cfg.get("databases", []):
            upsert_database(d)

        tbl_cfg = load_yaml(DATASETS_YML)
        for d in tbl_cfg.get("databases", []):
            db_name = d["database_name"]
            for t in d.get("tables", []):
                upsert_dataset(db_name, t)

        import_dashboards()

    print("[apply_assets] done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
