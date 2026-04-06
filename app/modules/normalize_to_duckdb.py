from pathlib import Path
import argparse
import hashlib
import re

import duckdb
import pandas as pd


MONTHS_ES = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}


def parse_spanish_date(value: str, year: int) -> str:
    value = value.strip().lower()
    m = re.match(r"(\d{1,2})\s+de\s+([a-záéíóú]+)", value)
    if not m:
        raise ValueError(f"Fecha no reconocida: {value}")

    day = int(m.group(1))
    month_name = m.group(2)
    month = MONTHS_ES.get(month_name)

    if not month:
        raise ValueError(f"Mes no reconocido: {month_name}")

    return f"{year:04d}-{month:02d}-{day:02d}"


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


# =============================
# NUEVO: CLASIFICACION
# =============================
def asignar_bucket(row):
    detalle = str(row.get("detalle", "")).lower()

    if "uber" in detalle or "walmart" in detalle:
        return "necesidades"
    elif "amazon" in detalle:
        return "gustos"
    elif "pago" in detalle:
        return "deuda"
    else:
        return "necesidades"


def es_deuda(row):
    return row.get("bucket_presupuesto") == "deuda"


def es_pago(row):
    texto = (str(row.get("descripcion", "")) + str(row.get("detalle", ""))).lower()
    return "pago" in texto


def crear_deuda(con, row, meses=3):
    monto = float(row["monto"])
    mensualidad = monto / meses

    deuda_id = con.execute(
        "SELECT COALESCE(MAX(id), 0) + 1 FROM deudas"
    ).fetchone()[0]

    con.execute(
        """
        INSERT INTO deudas VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            deuda_id,
            row.get("detalle", ""),
            monto,
            monto,
            mensualidad,
            meses,
            meses,
            row["fecha"],
            "activa",
            row["hash_id"],
        ],
    )

    for i in range(meses):
        con.execute(
            """
            INSERT INTO pagos_deuda VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                None,
                deuda_id,
                i + 1,
                row["fecha"],
                mensualidad,
                "pendiente",
                None,
                None,
            ],
        )


def aplicar_pago(con, row):
    deuda = con.execute(
        """
        SELECT id FROM deudas
        WHERE status = 'activa'
        ORDER BY fecha_inicio
        LIMIT 1
        """
    ).fetchone()

    if not deuda:
        return

    deuda_id = deuda[0]

    pago = con.execute(
        """
        SELECT id, monto FROM pagos_deuda
        WHERE deuda_id = ?
        AND status = 'pendiente'
        ORDER BY numero_pago
        LIMIT 1
        """,
        [deuda_id],
    ).fetchone()

    if not pago:
        return

    pago_id, monto = pago

    con.execute(
        """
        UPDATE pagos_deuda
        SET status = 'pagado',
            fecha_pago_real = ?,
            movimiento_hash = ?
        WHERE id = ?
        """,
        [row["fecha"], row["hash_id"], pago_id],
    )

    con.execute(
        """
        UPDATE deudas
        SET saldo_actual = saldo_actual - ?,
            meses_restantes = meses_restantes - 1
        WHERE id = ?
        """,
        [monto, deuda_id],
    )


def normalize_csv(input_csv: Path, bank: str, default_currency: str, year: int):
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

    # 👇 usamos bucket pero NO lo persistimos
    out["bucket_presupuesto"] = out.apply(asignar_bucket, axis=1)

    # 👇 guardamos copia completa para lógica
    df_full = out.copy()

    # 👇 dejamos schema limpio para DB
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

    return out, df_full


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

    # NUEVAS TABLAS
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS deudas (
            id INTEGER,
            descripcion TEXT,
            monto_total DOUBLE,
            saldo_actual DOUBLE,
            mensualidad DOUBLE,
            meses_totales INTEGER,
            meses_restantes INTEGER,
            fecha_inicio DATE,
            status TEXT,
            movimiento_origen_hash TEXT
        )
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pagos_deuda (
            id INTEGER,
            deuda_id INTEGER,
            numero_pago INTEGER,
            fecha_programada DATE,
            monto DOUBLE,
            status TEXT,
            fecha_pago_real DATE,
            movimiento_hash TEXT
        )
        """
    )

    con.execute("CREATE INDEX IF NOT EXISTS idx_fecha ON movimientos(fecha)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_categoria ON movimientos(categoria)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_detalle ON movimientos(detalle)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_tipo ON movimientos(tipo)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_banco ON movimientos(banco)")


def upsert_movimientos(con, df: pd.DataFrame, df_full: pd.DataFrame):
    con.register("df_movs", df)

    con.execute(
        """
        INSERT INTO movimientos
        SELECT fecha, categoria, detalle, monto, descripcion,
               tarjeta_cuenta, tipo, banco, moneda, source_file, hash_id
        FROM df_movs d
        WHERE NOT EXISTS (
            SELECT 1
            FROM movimientos m
            WHERE m.hash_id = d.hash_id
        )
        """
    )

    # LOGICA DE DEUDAS
    for _, row in df_full.iterrows():
        if es_deuda(row):
            crear_deuda(con, row, meses=3)

        if es_pago(row):
            aplicar_pago(con, row)


def main():
    parser = argparse.ArgumentParser(description="Normaliza CSV staging y lo carga a DuckDB")
    parser.add_argument("--input", required=True, help="CSV staging de entrada")
    parser.add_argument("--db", default="data/duckdb/finanzas.duckdb", help="Ruta del archivo DuckDB")
    parser.add_argument("--bank", required=True, help="Nombre del banco, ej. plata")
    parser.add_argument("--default-currency", default="MXN", help="Moneda default")
    parser.add_argument("--year", type=int, required=True, help="Año para las fechas")

    args = parser.parse_args()

    input_csv = Path(args.input)
    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if not input_csv.exists():
        raise SystemExit(f"No existe input CSV: {input_csv}")

    df, df_full = normalize_csv(
        input_csv=input_csv,
        bank=args.bank,
        default_currency=args.default_currency,
        year=args.year,
    )

    con = duckdb.connect(str(db_path))
    ensure_table(con)
    upsert_movimientos(con, df, df_full)

    total = con.execute("SELECT COUNT(*) FROM movimientos").fetchone()[0]

    print("Normalización completada")
    print("DB:", db_path)
    print("Input:", input_csv)
    print("Filas procesadas:", len(df))
    print("Total filas en movimientos:", total)

    con.close()


if __name__ == "__main__":
    main()
