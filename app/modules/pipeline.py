from pathlib import Path
import duckdb

from app.modules.ocr_extract import extract_text
from app.modules.ocr_to_csv import convert_to_csv
from app.modules.normalize_to_duckdb import normalize_csv, ensure_table, upsert_movimientos


def run_pipeline(
    input_path: str,
    config_path: str,
    bank: str,
    year: int,
    output_base: str = "data",
    db_path: str = "data/duckdb/finanzas.duckdb",
    default_currency: str = "MXN",
):
    input_path = Path(input_path)
    output_base = Path(output_base)
    db_path = Path(db_path)

    if not input_path.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {input_path}")

    raw_text_dir = output_base / "raw_text"
    staging_dir = output_base / "staging_csv"
    raw_text_dir.mkdir(parents=True, exist_ok=True)
    staging_dir.mkdir(parents=True, exist_ok=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    txt_path = extract_text(input_path, raw_text_dir)
    csv_path = convert_to_csv(txt_path, staging_dir, config_path)

    df = normalize_csv(
        input_csv=csv_path,
        bank=bank,
        default_currency=default_currency,
        year=year,
    )

    con = duckdb.connect(str(db_path))
    ensure_table(con)
    upsert_movimientos(con, df)
    total = con.execute("SELECT COUNT(*) FROM movimientos").fetchone()[0]
    con.close()

    return {
        "input": str(input_path),
        "raw_text": str(txt_path),
        "staging_csv": str(csv_path),
        "db": str(db_path),
        "rows_processed": len(df),
        "total_rows": total,
    }
