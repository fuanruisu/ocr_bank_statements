from pathlib import Path
import argparse
import hashlib
import re

import duckdb
import pandas as pd


MONTHS_ES = {
    "enero": 1, "ene": 1,
    "febrero": 2, "feb": 2,
    "marzo": 3, "mar": 3,
    "abril": 4, "abr": 4,
    "mayo": 5, "may": 5,
    "junio": 6, "jun": 6,
    "julio": 7, "jul": 7,
    "agosto": 8, "ago": 8,
    "septiembre": 9, "setiembre": 9, "sep": 9, "set": 9,
    "octubre": 10, "oct": 10,
    "noviembre": 11, "nov": 11,
    "diciembre": 12, "dic": 12,
}


def parse_spanish_date(value: str, year: int) -> str:
    value = value.strip().lower()
    # "2 de enero", "lunes 02 de marzo, 2026"
    m = re.match(r"(?:[a-záéíóú]+\s+)?(\d{1,2})\s+de\s+([a-záéíóú]+)(?:,?\s+(\d{4}))?", value)
    if not m:
        # "09 mar 2026" (abbreviated month, no "de")
        m = re.match(r"(\d{1,2})\s+([a-záéíóú]+)\s+(\d{4})", value)
    if not m:
        raise ValueError(f"Fecha no reconocida: {value}")

    day = int(m.group(1))
    month_name = m.group(2)
    month = MONTHS_ES.get(month_name)
    parsed_year = int(m.group(3)) if m.group(3) else year

    if not month:
        raise ValueError(f"Mes no reconocido: {month_name}")

    return f"{parsed_year:04d}-{month:02d}-{day:02d}"


def make_hash_id(row: pd.Series) -> str:
    raw = "|".join(
        [
            str(row.get("fecha", "")),
            str(row.get("descripcion", "")).strip().lower(),
            str(row.get("detalle", "")).strip().lower(),
            str(row.get("monto", "")),
            str(row.get("tarjeta_cuenta", "")).strip().lower(),
            str(row.get("tipo", "")).strip().lower(),
            str(row.get("moneda", "")).strip().upper(),
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def normalize_csv(input_csv: Path, bank: str, default_currency: str, year: int) -> pd.DataFrame:
    df = pd.read_csv(input_csv)

    required_cols = {
        "fecha",
        "categoria",
        "detalle",
        "monto",
        "descripcion",
        "tarjeta_cuenta",
        "tipo",
    }

    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas requeridas en staging: {missing}")

    out = df.copy()
    out = out.dropna(subset=["fecha"])

    out["fecha"] = out["fecha"].astype(str).map(lambda x: parse_spanish_date(x, year))
    out["categoria"] = out["categoria"].fillna("").astype(str).str.strip().str.lower()
    out["detalle"] = out["detalle"].fillna("").astype(str).str.strip()
    out["descripcion"] = out["descripcion"].fillna("").astype(str).str.strip()
    out["tarjeta_cuenta"] = out["tarjeta_cuenta"].fillna("").astype(str).str.strip()
    out["tipo"] = out["tipo"].fillna("").astype(str).str.strip().str.lower()

    out["monto"] = pd.to_numeric(out["monto"], errors="coerce")
    out = out.dropna(subset=["monto"])

    if "moneda" in out.columns:
        out["moneda"] = out["moneda"].fillna("").astype(str).str.strip().str.upper()
        out["moneda"] = out["moneda"].replace("", default_currency)
    else:
        out["moneda"] = default_currency

    out["banco"] = bank
    out["source_file"] = input_csv.name
    out["hash_id"] = out.apply(make_hash_id, axis=1)

    out = out[
        [
            "fecha",
            "categoria",
            "detalle",
            "monto",
            "descripcion",
            "tarjeta_cuenta",
            "tipo",
            "banco",
            "moneda",
            "source_file",
            "hash_id",
        ]
    ]

    return out


def ensure_table(con):
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS movimientos (
            fecha DATE,
            categoria TEXT,
            detalle TEXT,
            monto DOUBLE,
            descripcion TEXT,
            tarjeta_cuenta TEXT,
            tipo TEXT,
            banco TEXT,
            moneda TEXT,
            source_file TEXT,
            hash_id TEXT PRIMARY KEY
        )
        """
    )

    con.execute("CREATE INDEX IF NOT EXISTS idx_fecha ON movimientos(fecha)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_categoria ON movimientos(categoria)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_detalle ON movimientos(detalle)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_tipo ON movimientos(tipo)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_banco ON movimientos(banco)")


def upsert_movimientos(con, df: pd.DataFrame):
    con.register("df_movs", df)

    con.execute(
        """
        INSERT INTO movimientos
        SELECT *
        FROM df_movs d
        WHERE NOT EXISTS (
            SELECT 1
            FROM movimientos m
            WHERE m.hash_id = d.hash_id
        )
        """
    )


def open_db_connection(db: str) -> duckdb.DuckDBPyConnection:
    """Connect to DuckDB locally or via MotherDuck (md: prefix).

    For MotherDuck, set the motherduck_token env var before calling:
        export motherduck_token=<your_token>
    Then pass --db md:finanzas (or any database name on your account).
    """
    if db.startswith("md:"):
        db_name = db[3:]
        if db_name:
            tmp = duckdb.connect("md:")
            tmp.execute(f'CREATE DATABASE IF NOT EXISTS "{db_name}"')
            tmp.close()
        return duckdb.connect(db)
    path = Path(db)
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))


def main():
    parser = argparse.ArgumentParser(description="Normaliza CSV staging y lo carga a DuckDB")
    parser.add_argument("--input", required=True, help="CSV staging de entrada")
    parser.add_argument("--db", default="data/duckdb/finanzas.duckdb", help="Ruta del archivo DuckDB")
    parser.add_argument("--bank", required=True, help="Nombre del banco, ej. plata")
    parser.add_argument("--default-currency", default="MXN", help="Moneda default")
    parser.add_argument("--year", type=int, required=True, help="Año para las fechas")

    args = parser.parse_args()

    input_csv = Path(args.input)

    if not input_csv.exists():
        raise SystemExit(f"No existe input CSV: {input_csv}")

    df = normalize_csv(
        input_csv=input_csv,
        bank=args.bank,
        default_currency=args.default_currency,
        year=args.year,
    )

    con = open_db_connection(args.db)
    ensure_table(con)
    upsert_movimientos(con, df)

    total = con.execute("SELECT COUNT(*) FROM movimientos").fetchone()[0]

    print("Normalización completada")
    print("DB:", args.db)
    print("Input:", input_csv)
    print("Filas procesadas:", len(df))
    print("Total filas en movimientos:", total)

    con.close()


if __name__ == "__main__":
    main()
