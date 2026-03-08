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


def parse_amount_to_absolute_and_type(amount_text, cfg=None):
    raw = amount_text.strip()

    sign = "ingreso"
    if raw.startswith("-") or raw.startswith("–"):
        sign = "gasto"
    elif raw.startswith("+"):
        sign = "ingreso"

    has_dollar = "$" in raw
    has_usd = "USD" in raw.upper()

    numeric = raw
    numeric = numeric.replace("USD", "")
    numeric = numeric.replace("$", "")
    numeric = numeric.replace(",", "")
    numeric = numeric.replace("–", "-")
    numeric = numeric.replace("+", "")
    numeric = numeric.replace("-", "")
    numeric = numeric.strip()

    ocr_fixes = (cfg or {}).get("ocr_fixes", {})

    # Fix OCR específico para algunos casos tipo:
    # -8261.11  -> realmente era -$261.11
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

    return amount, sign


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


def normalize_category(raw_category, category_rules):
    if not raw_category:
        return ""

    value = raw_category.strip()

    for rule in category_rules:
        pattern = rule.get("match")
        mapped = rule.get("value", value)
        if pattern and re.search(pattern, value):
            return mapped

    return value


def convert_multiline_after_date_with_secondary_category(lines, cfg):
    patterns = cfg.get("patterns", {})
    defaults = cfg.get("defaults", {})
    cleanup_cfg = cfg.get("description_cleanup", {})
    ignore_lines = cfg.get("ignore_lines", [])
    ignore_contains = cfg.get("ignore_contains", [])
    category_rules = cfg.get("category_rules", [])

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

        # Línea secundaria: categoría + ingreso asociado
        secondary_match = secondary_amount_re.search(line)
        if secondary_match:
            amount_text = secondary_match.group()
            amount, _ = parse_amount_to_absolute_and_type(amount_text, cfg)

            category_text = line.replace(amount_text, "").strip()
            category_text = clean_description(category_text, cleanup_cfg)
            category_text = normalize_category(category_text, category_rules)

            rows.append(
                {
                    "fecha": current_date,
                    "categoria": category_text,
                    "monto": amount,
                    "descripcion": last_primary_description or "",
                    "tarjeta_cuenta": defaults.get("cuenta", ""),
                    "tipo": "ingreso",
                }
            )
            continue

        # Línea principal: comercio + monto
        primary_match = primary_amount_re.search(line)
        if primary_match:
            amount_text = primary_match.group()
            amount, tx_type = parse_amount_to_absolute_and_type(amount_text, cfg)

            description = line.replace(amount_text, "").strip()
            description = clean_description(description, cleanup_cfg)

            last_primary_description = description

            rows.append(
                {
                    "fecha": current_date,
                    "categoria": "",
                    "monto": amount,
                    "descripcion": description,
                    "tarjeta_cuenta": defaults.get("cuenta", ""),
                    "tipo": tx_type,
                }
            )
            continue

    df = pd.DataFrame(rows)

    output_columns = cfg.get(
        "output_columns",
        ["fecha", "categoria", "monto", "descripcion", "tarjeta_cuenta", "tipo"],
    )

    if df.empty:
        df = pd.DataFrame(columns=output_columns)
        return df

    for col in output_columns:
        if col not in df.columns:
            df[col] = ""

    df = df[output_columns]

    return df


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
    else:
        raise ValueError(f"Modo no soportado todavía: {mode}")

    out_file = out_dir / f"{Path(txt_path).stem}.csv"
    df.to_csv(out_file, index=False)

    return out_file
