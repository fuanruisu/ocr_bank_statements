"""
Extract saldo_total and pago_sin_intereses from OCR text of a credit card statement.
Covers BBVA, Santander, Plata, MercadoPago, and generic Spanish patterns.
"""
import re


# Ordered from most specific to most generic so the first match wins
_PAGO_PATTERNS = [
    r"pago\s+para\s+no\s+generar\s+inter[eé]ses[^\d]*\$?\s*([\d,]+\.?\d*)",
    r"pago\s+sin\s+inter[eé]ses[^\d]*\$?\s*([\d,]+\.?\d*)",
    r"para\s+no\s+generar\s+inter[eé]ses[^\d]*\$?\s*([\d,]+\.?\d*)",
    r"evitar\s+inter[eé]ses[^\d]*\$?\s*([\d,]+\.?\d*)",
    r"no\s+generar\s+inter[eé]ses[^\d]*\$?\s*([\d,]+\.?\d*)",
    r"pago\s+total\s+sin\s+inter[eé]s[^\d]*\$?\s*([\d,]+\.?\d*)",
]

_SALDO_PATTERNS = [
    r"saldo\s+total[^\d]*\$?\s*([\d,]+\.?\d*)",
    r"total\s+a\s+pagar[^\d]*\$?\s*([\d,]+\.?\d*)",
    r"saldo\s+al\s+corte[^\d]*\$?\s*([\d,]+\.?\d*)",
    r"saldo\s+actual[^\d]*\$?\s*([\d,]+\.?\d*)",
    r"monto\s+total[^\d]*\$?\s*([\d,]+\.?\d*)",
]


def _parse_amount(raw: str) -> float:
    """'1,234.56' or '1234.56' → 1234.56"""
    return float(raw.replace(",", ""))


def _search(patterns: list[str], text: str) -> float | None:
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                return _parse_amount(m.group(1))
            except ValueError:
                continue
    return None


def extract_corte_values(text: str) -> dict:
    """
    Returns {'saldo_total': float|None, 'pago_sin_intereses': float|None}.
    """
    return {
        "saldo_total": _search(_SALDO_PATTERNS, text),
        "pago_sin_intereses": _search(_PAGO_PATTERNS, text),
    }


def insert_corte(con, cuenta_id: str, fecha: str, saldo: float,
                 pago_sin_intereses: float | None, source_file: str,
                 notas: str | None = None) -> None:
    """
    Insert or replace a corte record in saldo_deuda_historico.
    Uses INSERT OR REPLACE so re-running on the same (cuenta_id, fecha) is safe.
    """
    con.execute(
        """
        INSERT INTO saldo_deuda_historico
          (fecha, cuenta_id, saldo, pago_sin_intereses, source_file, notas)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (cuenta_id, fecha) DO UPDATE SET
          saldo              = EXCLUDED.saldo,
          pago_sin_intereses = EXCLUDED.pago_sin_intereses,
          source_file        = EXCLUDED.source_file,
          notas              = EXCLUDED.notas
        """,
        (fecha, cuenta_id, saldo, pago_sin_intereses, source_file, notas),
    )
