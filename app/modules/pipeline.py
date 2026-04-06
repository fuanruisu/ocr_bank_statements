from pathlib import Path

import duckdb

from app.modules.ocr_extract import extract_text
from app.modules.ocr_to_csv import convert_to_csv
from app.modules.normalize_to_duckdb import normalize_csv, ensure_table, upsert_movimientos


def run_pipeline(input_path: Path, config_path: Path, bank: str, year: int, default_currency="MXN"):
    input_path = Path(input_path)
    config_path = Path(config_path)

    base_dir = Path("data")

    raw_text_dir = base_dir / "raw_text"
    staging_csv_dir = base_dir / "staging_csv"
    duckdb_dir = base_dir / "duckdb"

    raw_text_dir.mkdir(parents=True, exist_ok=True)
    staging_csv_dir.mkdir(parents=True, exist_ok=True)
    duckdb_dir.mkdir(parents=True, exist_ok=True)

    # =============================
    # 1. OCR
    # =============================
    txt_path = extract_text(input_path, raw_text_dir)

    # =============================
    # 2. PARSE (TXT → CSV)
    # =============================
    csv_path = convert_to_csv(txt_path, staging_csv_dir, config_path)

    # =============================
    # 3. NORMALIZE (CSV → DF)
    # 🔥 CAMBIO: ahora regresa df y df_full
    # =============================
    df, df_full = normalize_csv(
        input_csv=csv_path,
        bank=bank,
        default_currency=default_currency,
        year=year,
    )

    # =============================
    # 4. LOAD (DF → DuckDB)
    # =============================
    db_path = duckdb_dir / "finanzas.duckdb"
    con = duckdb.connect(str(db_path))

    ensure_table(con)

    # 🔥 CAMBIO: ahora pasamos df_full también
    upsert_movimientos(con, df, df_full)

    total = con.execute("SELECT COUNT(*) FROM movimientos").fetchone()[0]

    con.close()

    return {
        "input": str(input_path),
        "txt": str(txt_path),
        "csv": str(csv_path),
        "db": str(db_path),
        "rows_processed": len(df),
        "total_rows_db": total,
    }
