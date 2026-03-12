from pathlib import Path
import re
import pandas as pd
import yaml


def load_yaml_config(config_path):
    if not config_path:
        return None

    config_file = Path(config_path)

    if not config_file.exists():
        raise FileNotFoundError(f"No existe el config YAML: {config_file}")

    with config_file.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def clean_description(text, cleanup_cfg):
    value = text

    if not cleanup_cfg:
        return value.strip()

    for pattern in cleanup_cfg.get("remove_prefixes", []):
        value = re.sub(pattern, "", value)

    if cleanup_cfg.get("normalize_whitespace", False):
        value = re.sub(r"\s+", " ", value)

    return value.strip()


def parse_amount_to_absolute_type_and_currency(amount_text, cfg=None):
    raw = amount_text.strip()

    sign = "ingreso"

    if "-" in raw or "–" in raw:
        sign = "gasto"
    elif "+" in raw:
        sign = "ingreso"

    currency = (cfg or {}).get("defaults", {}).get("currency", "MXN")

    if "USD" in raw.upper():
        currency = "USD"

    has_dollar = "$" in raw
    has_usd = "USD" in raw.upper()

    numeric = re.search(r"\d+\.\d+", raw)
    if not numeric:
        raise ValueError(f"No se pudo extraer monto de: {raw}")

    numeric = numeric.group()

    ocr_fixes = (cfg or {}).get("ocr_fixes", {})

    if (
        ocr_fixes.get("suspicious_leading_8_without_dollar", False)
        and sign == "gasto"
        and not has_dollar
        and not has_usd
        and numeric.startswith("8")
        and "." in numeric
        and len(numeric) >= 5
    ):
        numeric = numeric[1:]

    amount = float(numeric)

    return amount, sign, currency


def line_matches_any_regex(line, regex_list):
    for pattern in regex_list:
        if re.search(pattern, line):
            return True
    return False


def line_contains_any(line, contains_list):
    for token in contains_list:
        if token in line:
            return True
    return False


def normalize_detail(raw_detail, detail_rules):
    if not raw_detail:
        return ""

    value = raw_detail.strip()

    for rule in detail_rules:
        pattern = rule.get("match")
        mapped = rule.get("value", value)
        if pattern and re.search(pattern, value):
            return mapped

    return value


def finalize_dataframe(rows, cfg):
    output_columns = cfg.get(
        "output_columns",
        [
            "fecha",
            "categoria",
            "detalle",
            "monto",
            "descripcion",
            "tarjeta_cuenta",
            "tipo",
            "moneda",
        ],
    )

    df = pd.DataFrame(rows)

    if df.empty:
        return pd.DataFrame(columns=output_columns)

    for col in output_columns:
        if col not in df.columns:
            df[col] = ""

    df = df[output_columns]
    return df


def convert_multiline_after_date_with_secondary_category(lines, cfg):
    patterns = cfg.get("patterns", {})
    defaults = cfg.get("defaults", {})
    cleanup_cfg = cfg.get("description_cleanup", {})
    ignore_lines = cfg.get("ignore_lines", [])
    ignore_contains = cfg.get("ignore_contains", [])
    detail_rules = cfg.get("detail_rules", [])

    if "date" not in patterns:
        raise ValueError("Falta 'patterns.date' en el YAML")
    if "primary_amount" not in patterns:
        raise ValueError("Falta 'patterns.primary_amount' en el YAML")
    if "secondary_amount" not in patterns:
        raise ValueError("Falta 'patterns.secondary_amount' en el YAML")

    date_re = re.compile(patterns["date"])
    primary_amount_re = re.compile(patterns["primary_amount"])
    secondary_amount_re = re.compile(patterns["secondary_amount"])

    current_date = None
    last_primary_description = None
    rows = []

    for original_line in lines:
        line = original_line.strip()

        if not line:
            continue

        if line_contains_any(line, ignore_contains):
            continue

        if line_matches_any_regex(line, ignore_lines):
            continue

        if date_re.search(line):
            current_date = line
            continue

        if current_date is None:
            continue

        secondary_match = secondary_amount_re.search(line)
        if secondary_match:
            amount_text = secondary_match.group()
            amount, _, currency = parse_amount_to_absolute_type_and_currency(amount_text, cfg)

            detail_text = line.replace(amount_text, "").strip()
            detail_text = clean_description(detail_text, cleanup_cfg)
            detail_text = normalize_detail(detail_text, detail_rules)

            rows.append(
                {
                    "fecha": current_date,
                    "categoria": "",
                    "detalle": detail_text,
                    "monto": amount,
                    "descripcion": last_primary_description or "",
                    "tarjeta_cuenta": defaults.get("cuenta", ""),
                    "tipo": "ingreso",
                    "moneda": currency,
                }
            )
            continue

        primary_match = primary_amount_re.search(line)
        if primary_match:
            amount_text = primary_match.group()
            amount, tx_type, currency = parse_amount_to_absolute_type_and_currency(amount_text, cfg)

            description = line.replace(amount_text, "").strip()
            description = clean_description(description, cleanup_cfg)

            last_primary_description = description

            rows.append(
                {
                    "fecha": current_date,
                    "categoria": "",
                    "detalle": "",
                    "monto": amount,
                    "descripcion": description,
                    "tarjeta_cuenta": defaults.get("cuenta", ""),
                    "tipo": tx_type,
                    "moneda": currency,
                }
            )
            continue

    return finalize_dataframe(rows, cfg)


def convert_multiline_after_date_with_detail_line(lines, cfg):
    patterns = cfg.get("patterns", {})
    defaults = cfg.get("defaults", {})
    cleanup_cfg = cfg.get("description_cleanup", {})
    ignore_lines = cfg.get("ignore_lines", [])
    ignore_contains = cfg.get("ignore_contains", [])
    detail_rules = cfg.get("detail_rules", [])

    if "date" not in patterns:
        raise ValueError("Falta 'patterns.date' en el YAML")
    if "primary_amount" not in patterns:
        raise ValueError("Falta 'patterns.primary_amount' en el YAML")

    date_re = re.compile(patterns["date"])
    primary_amount_re = re.compile(patterns["primary_amount"])

    current_date = None
    pending_row = None
    rows = []

    for original_line in lines:
        line = original_line.strip()

        if not line:
            continue

        if line_contains_any(line, ignore_contains):
            continue

        if line_matches_any_regex(line, ignore_lines):
            continue

        if date_re.search(line):
            if pending_row is not None:
                rows.append(pending_row)
                pending_row = None

            current_date = line
            continue

        if current_date is None:
            continue

        primary_match = primary_amount_re.search(line)
        if primary_match:

            if pending_row is not None:
                rows.append(pending_row)
                pending_row = None

            amount_text = primary_match.group()
            amount, tx_type, currency = parse_amount_to_absolute_type_and_currency(amount_text, cfg)

            description = line.replace(amount_text, "").strip()
            description = clean_description(description, cleanup_cfg)

            pending_row = {
                "fecha": current_date,
                "categoria": "",
                "detalle": "",
                "monto": amount,
                "descripcion": description,
                "tarjeta_cuenta": defaults.get("cuenta", ""),
                "tipo": tx_type,
                "moneda": currency,
            }

            continue

        if pending_row is not None:
            detail_text = clean_description(line, cleanup_cfg)
            detail_text = normalize_detail(detail_text, detail_rules)
            pending_row["detalle"] = detail_text
            rows.append(pending_row)
            pending_row = None
            continue

    if pending_row is not None:
        rows.append(pending_row)

    return finalize_dataframe(rows, cfg)


# ---------------- NUEVO PARSER PARA SANTANDER ----------------

def convert_positional_triplets(lines, cfg):

    patterns = cfg.get("patterns", {})
    defaults = cfg.get("defaults", {})

    date_re = re.compile(patterns["date"])
    amount_re = re.compile(patterns["amount"])

    ignore_contains = cfg.get("ignore_contains", [])
    ignore_regex = cfg.get("ignore_regex", [])

    fechas = []
    descs = []
    montos = []

    for line in lines:

        if line_contains_any(line, ignore_contains):
            continue

        if line_matches_any_regex(line, ignore_regex):
            continue

        if date_re.search(line):
            fechas.append(line)
            continue

        if amount_re.search(line):
            montos.append(line)
            continue

        descs.append(line)

    n = min(len(fechas), len(descs), len(montos))

    rows = []

    sign_conv = cfg.get("sign_convention", "normal")

    for i in range(n):

        amount_text = montos[i]

        amount, tx_type, currency = parse_amount_to_absolute_type_and_currency(
            amount_text, cfg
        )

        if sign_conv == "inverted":
            tx_type = "ingreso" if tx_type == "gasto" else "gasto"

        rows.append(
            {
                "fecha": fechas[i],
                "categoria": "",
                "detalle": "",
                "monto": amount,
                "descripcion": descs[i],
                "tarjeta_cuenta": defaults.get("cuenta", ""),
                "tipo": tx_type,
                "moneda": currency,
            }
        )

    return finalize_dataframe(rows, cfg)


# -------------------------------------------------------------


def convert_to_csv(txt_path, out_dir, config=None):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    text = Path(txt_path).read_text(encoding="utf-8")
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    cfg = load_yaml_config(config)

    if not cfg:
        raise ValueError("Se requiere --config para esta versión del parser.")

    mode = cfg.get("mode")

    if mode == "multiline_after_date_with_secondary_category":
        df = convert_multiline_after_date_with_secondary_category(lines, cfg)

    elif mode == "multiline_after_date_with_detail_line":
        df = convert_multiline_after_date_with_detail_line(lines, cfg)

    elif mode == "positional_triplets":
        df = convert_positional_triplets(lines, cfg)

    else:
        raise ValueError(f"Modo no soportado todavía: {mode}")

    out_file = out_dir / f"{Path(txt_path).stem}.csv"
    df.to_csv(out_file, index=False)

    return out_file
