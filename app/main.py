#!/usr/bin/env python3
import argparse
from pathlib import Path

from app.modules.ocr_extract import extract_text
from app.modules.ocr_to_csv import convert_to_csv
from app.modules.normalize_to_duckdb import normalize_csv, ensure_table, upsert_movimientos, open_db_connection
from app.modules.pipeline import run_pipeline


def cmd_ocr(args):
    output_base = Path(args.output)
    raw_text_dir = output_base / "raw_text"
    raw_text_dir.mkdir(parents=True, exist_ok=True)

    txt_path = extract_text(Path(args.input), raw_text_dir)
    print(f"Texto OCR guardado en: {txt_path}")

    if args.show:
        print("\n=== OCR OUTPUT BEGIN ===\n")
        print(Path(txt_path).read_text(encoding="utf-8"))
        print("\n=== OCR OUTPUT END ===")


def cmd_parse(args):
    output_base = Path(args.output)
    staging_dir = output_base / "staging_csv"
    staging_dir.mkdir(parents=True, exist_ok=True)

    csv_path = convert_to_csv(
        txt_path=Path(args.input),
        out_dir=staging_dir,
        config=args.config,
    )
    print(f"CSV staging guardado en: {csv_path}")


def cmd_load(args):
    df = normalize_csv(
        input_csv=Path(args.input),
        bank=args.bank,
        default_currency=args.default_currency,
        year=args.year,
    )

    con = open_db_connection(args.db)
    ensure_table(con)
    upsert_movimientos(con, df)
    total = con.execute("SELECT COUNT(*) FROM movimientos").fetchone()[0]
    con.close()

    print("Carga completada")
    print("DB:", args.db)
    print("Filas procesadas:", len(df))
    print("Total filas en movimientos:", total)


def cmd_pipeline(args):
    result = run_pipeline(
        input_path=args.input,
        config_path=args.config,
        bank=args.bank,
        year=args.year,
        output_base=args.output,
        db_path=args.db,
        default_currency=args.default_currency,
    )

    print("Pipeline completado")
    for k, v in result.items():
        print(f"{k}: {v}")


def build_parser():
    parser = argparse.ArgumentParser(
        prog="banketl",
        description="CLI para OCR, parsing y carga de estados de cuenta a DuckDB.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    p_ocr = subparsers.add_parser("ocr", help="Extrae texto OCR desde imagen o PDF")
    p_ocr.add_argument("--input", required=True, help="Archivo de entrada (imagen o PDF)")
    p_ocr.add_argument("--output", default="data", help="Directorio base de salida")
    p_ocr.add_argument("--show", action="store_true", help="Muestra el texto OCR en pantalla")
    p_ocr.set_defaults(func=cmd_ocr)

    p_parse = subparsers.add_parser("parse", help="Convierte raw text a CSV staging")
    p_parse.add_argument("--input", required=True, help="Archivo .txt de OCR")
    p_parse.add_argument("--config", required=True, help="Archivo YAML de parsing")
    p_parse.add_argument("--output", default="data", help="Directorio base de salida")
    p_parse.set_defaults(func=cmd_parse)

    p_load = subparsers.add_parser("load", help="Normaliza CSV staging y carga a DuckDB")
    p_load.add_argument("--input", required=True, help="CSV staging de entrada")
    p_load.add_argument("--bank", required=True, help="Nombre del banco")
    p_load.add_argument("--year", required=True, type=int, help="Año para fechas sin año")
    p_load.add_argument("--db", default="data/duckdb/finanzas.duckdb", help="Ruta DuckDB")
    p_load.add_argument("--default-currency", default="MXN", help="Moneda default")
    p_load.set_defaults(func=cmd_load)

    p_pipe = subparsers.add_parser("pipeline", help="Ejecuta OCR -> parse -> load")
    p_pipe.add_argument("--input", required=True, help="Archivo de entrada (imagen o PDF)")
    p_pipe.add_argument("--config", required=True, help="Archivo YAML de parsing")
    p_pipe.add_argument("--bank", required=True, help="Nombre del banco")
    p_pipe.add_argument("--year", required=True, type=int, help="Año para fechas sin año")
    p_pipe.add_argument("--output", default="data", help="Directorio base de salida")
    p_pipe.add_argument("--db", default="data/duckdb/finanzas.duckdb", help="Ruta DuckDB")
    p_pipe.add_argument("--default-currency", default="MXN", help="Moneda default")
    p_pipe.set_defaults(func=cmd_pipeline)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
