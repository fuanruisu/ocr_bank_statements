from pathlib import Path
import pandas as pd
import re


def convert_to_csv(txt_path, out_dir, config=None):

    out_dir = Path(out_dir)

    out_dir.mkdir(parents=True, exist_ok=True)

    text = Path(txt_path).read_text(encoding="utf-8")

    lines = [line.strip() for line in text.splitlines() if line.strip()]

    rows = []

    current_date = None

    date_regex = re.compile(r"\d{1,2}\s+de\s+\w+", re.IGNORECASE)
    amount_regex = re.compile(r"[-+]?\$[\d,]+\.\d{2}")

    for line in lines:

        if line.startswith("=== PAGE"):
            continue

        if date_regex.search(line):
            current_date = line
            continue

        amount_match = amount_regex.search(line)

        if amount_match and current_date:

            amount = amount_match.group()

            description = line.replace(amount, "").strip()

            rows.append(
                {
                    "fecha_raw": current_date,
                    "descripcion_raw": description,
                    "monto_raw": amount,
                    "linea_raw": line,
                }
            )

    df = pd.DataFrame(rows)

    out_file = out_dir / f"{Path(txt_path).stem}.csv"

    df.to_csv(out_file, index=False)

    return out_file
