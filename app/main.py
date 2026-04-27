#!/usr/bin/env python3
import argparse
import uuid
from pathlib import Path

from app.modules.ocr_extract import extract_text, ocr_image, ocr_pdf
from app.modules.ocr_to_csv import convert_to_csv
from app.modules.normalize_to_duckdb import (
    normalize_csv, ensure_table, upsert_movimientos, open_db_connection,
    print_unclassified_summary, update_movimiento,
)
from app.modules.pipeline import run_pipeline
from app.modules.corte_extractor import extract_corte_values, insert_corte


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
    print_unclassified_summary(df)


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


def cmd_corte(args):
    input_path = Path(args.input)
    if not input_path.exists():
        raise SystemExit(f"Archivo no encontrado: {input_path}")

    print(f"Extrayendo texto de {input_path.name}...")
    if input_path.suffix.lower() == ".pdf":
        text = ocr_pdf(str(input_path))
    else:
        text = ocr_image(str(input_path))

    if args.show:
        print("\n=== OCR OUTPUT ===")
        print(text)
        print("=== FIN OCR ===\n")

    detected = extract_corte_values(text)
    saldo_det = detected["saldo_total"]
    pago_det  = detected["pago_sin_intereses"]

    print(f"\nValores detectados en el OCR:")
    print(f"  saldo_total         : {f'${saldo_det:,.2f}' if saldo_det is not None else 'NO ENCONTRADO'}")
    print(f"  pago_sin_intereses  : {f'${pago_det:,.2f}' if pago_det is not None else 'NO ENCONTRADO'}")
    print()

    # confirm or override each value
    def prompt_amount(label: str, detected_val) -> float:
        hint = f"{detected_val:,.2f}" if detected_val is not None else "no detectado"
        raw = input(f"{label} [{hint}]: ").strip()
        if raw == "" and detected_val is not None:
            return detected_val
        try:
            return float(raw.replace(",", ""))
        except ValueError:
            raise SystemExit(f"Monto inválido: {raw!r}")

    saldo = prompt_amount("saldo_total", saldo_det)
    pago  = prompt_amount("pago_sin_intereses", pago_det)

    # fecha_corte
    if args.fecha_corte:
        fecha = args.fecha_corte
    else:
        fecha = input("fecha_corte (YYYY-MM-DD): ").strip()
        if not fecha:
            raise SystemExit("fecha_corte es requerida.")

    # confirm
    print(f"\nResumen a insertar:")
    print(f"  cuenta_id          : {args.cuenta}")
    print(f"  fecha_corte        : {fecha}")
    print(f"  saldo_total        : ${saldo:,.2f}")
    print(f"  pago_sin_intereses : ${pago:,.2f}")
    print(f"  source_file        : {input_path.name}")
    ok = input("\n¿Confirmar? [s/N]: ").strip().lower()
    if ok != "s":
        print("Cancelado.")
        return

    con = open_db_connection(args.db)
    insert_corte(
        con,
        cuenta_id=args.cuenta,
        fecha=fecha,
        saldo=saldo,
        pago_sin_intereses=pago,
        source_file=input_path.name,
        notas=args.notas,
    )
    con.close()
    print("Corte guardado.")


_VALID_SUBTIPOS = {"normal", "transferencia_interna", "msi", "inversion_ahorro", "saldo_inicial"}
_VALID_CATEGORIAS = {"Necesidades", "Gustos", "Inversion", "Deuda"}


def cmd_annotate(args):
    con = open_db_connection(args.db)

    if args.unclassified:
        rows = con.execute(
            """
            SELECT hash_id, fecha, descripcion, detalle, monto, moneda, tarjeta_cuenta
            FROM movimientos
            WHERE categoria = '' OR categoria IS NULL
            ORDER BY fecha DESC
            LIMIT ?
            """,
            [args.last],
        ).fetchdf()
        mode = "categoria"
    else:
        rows = con.execute(
            """
            SELECT hash_id, fecha, descripcion, detalle, monto, moneda, tarjeta_cuenta, subtipo
            FROM movimientos
            WHERE subtipo = 'normal'
            ORDER BY fecha DESC
            LIMIT ?
            """,
            [args.last],
        ).fetchdf()
        mode = "subtipo"

    if rows.empty:
        print("No hay filas para anotar.")
        con.close()
        return

    print(rows.to_string(index=False))
    print()

    raw = input("hash_id (o prefijo) de la fila a actualizar (Enter para salir): ").strip()
    if not raw:
        con.close()
        return

    match = con.execute(
        "SELECT hash_id FROM movimientos WHERE hash_id LIKE ?", [f"{raw}%"]
    ).fetchone()
    if not match:
        print(f"No se encontró ninguna fila con hash_id que empiece por: {raw}")
        con.close()
        return
    hash_id = match[0]

    updates = {}

    if mode == "categoria":
        cat = input(f"categoria {sorted(_VALID_CATEGORIAS)}: ").strip()
        if cat not in _VALID_CATEGORIAS:
            print(f"Valor no válido. Opciones: {sorted(_VALID_CATEGORIAS)}")
            con.close()
            return
        updates["categoria"] = cat
    else:
        subtipo = input(f"subtipo {sorted(_VALID_SUBTIPOS)} (Enter para omitir): ").strip()
        if subtipo and subtipo not in _VALID_SUBTIPOS:
            print(f"Valor no válido. Opciones: {sorted(_VALID_SUBTIPOS)}")
            con.close()
            return
        if subtipo:
            updates["subtipo"] = subtipo

        if updates.get("subtipo") == "transferencia_interna":
            cuenta_destino = input("cuenta_destino (ej. ciudad_maderas, openbank): ").strip()
            if cuenta_destino:
                updates["cuenta_destino"] = cuenta_destino
            id_ref = input("id_referencia (Enter para generar nuevo UUID): ").strip()
            updates["id_referencia"] = id_ref if id_ref else str(uuid.uuid4())
            if not id_ref:
                print(f"  id_referencia generado: {updates['id_referencia']}")

    if not updates:
        print("Sin cambios.")
        con.close()
        return

    update_movimiento(con, hash_id, **updates)
    con.close()
    print(f"Actualizado {hash_id[:12]}... -> {updates}")


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

    p_corte = subparsers.add_parser("corte", help="OCR de estado de cuenta: extrae saldo y pago sin intereses")
    p_corte.add_argument("--input",       required=True,  help="Screenshot o PDF del estado de cuenta")
    p_corte.add_argument("--cuenta",      required=True,  help="ID de la cuenta (ej. santander_free)")
    p_corte.add_argument("--fecha-corte", default=None,   help="Fecha de corte YYYY-MM-DD (se pregunta si no se pasa)")
    p_corte.add_argument("--db",          default="data/duckdb/finanzas.duckdb", help="Ruta DuckDB")
    p_corte.add_argument("--notas",       default=None,   help="Notas opcionales")
    p_corte.add_argument("--show",        action="store_true", help="Muestra el texto OCR completo")
    p_corte.set_defaults(func=cmd_corte)

    p_annotate = subparsers.add_parser("annotate", help="Anota y corrige movimientos manualmente")
    p_annotate.add_argument("--db", default="data/duckdb/finanzas.duckdb", help="Ruta DuckDB")
    p_annotate.add_argument("--last", type=int, default=20, help="Número de filas a mostrar")
    p_annotate.add_argument("--unclassified", action="store_true", help="Mostrar filas sin categoria")
    p_annotate.set_defaults(func=cmd_annotate)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
