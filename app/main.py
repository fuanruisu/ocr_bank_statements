#!/usr/bin/env python3

import argparse
from pathlib import Path

from ocr_extract import extract_text
from ocr_to_csv import convert_to_csv


def main():

    parser = argparse.ArgumentParser(
        prog="ocr-bank-statements",
        description="""
Pipeline para convertir estados de cuenta (imagen o PDF)
a CSV estructurado usando OCR.

Flujo:

input -> OCR -> raw_text -> parser -> staging_csv
""",
        epilog="""
Ejemplos:

Solo ver OCR:
python app/main.py --input data/inputs/plataSS.jpg --ocr_only

Pipeline completo:
python app/main.py --input data/inputs/plataSS.jpg

Especificar output:
python app/main.py --input estado.pdf --output data

Usar config YAML:
python app/main.py --input estado.jpg --config config/plata_mobile.yaml
""",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Archivo de entrada (imagen o PDF)"
    )

    parser.add_argument(
        "--output",
        default="data",
        help="Directorio base de salida (default: data)"
    )

    parser.add_argument(
        "--config",
        default=None,
        help="Archivo YAML con reglas de parsing"
    )

    parser.add_argument(
        "--ocr_only",
        action="store_true",
        help="Solo ejecuta OCR y muestra el texto extraído"
    )

    args = parser.parse_args()

    input_path = Path(args.input)
    output_base = Path(args.output)

    if not input_path.exists():
        raise SystemExit(f"ERROR: archivo no encontrado: {input_path}")

    raw_text_dir = output_base / "raw_text"
    staging_dir = output_base / "staging_csv"

    raw_text_dir.mkdir(parents=True, exist_ok=True)
    staging_dir.mkdir(parents=True, exist_ok=True)

    print("STEP 1 — OCR extraction")

    txt_path = extract_text(input_path, raw_text_dir)

    print("Texto OCR guardado en:", txt_path)

    if args.ocr_only:
        print("\n=== OCR OUTPUT BEGIN ===\n")
        print(txt_path.read_text(encoding="utf-8"))
        print("\n=== OCR OUTPUT END ===")
        return

    print("STEP 2 — OCR to CSV")

    csv_path = convert_to_csv(txt_path, staging_dir, args.config)

    print("\nPipeline finished")
    print("Raw text:", txt_path)
    print("CSV:", csv_path)


if __name__ == "__main__":
    main()
