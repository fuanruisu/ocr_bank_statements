"""
Seed comprehensive test data into test_finanzas.duckdb.
Covers Jan-Apr 2026 across all accounts.
Run from repo root: python3 scripts/seed_test_data.py
"""
import argparse
import os
import re
import sys
import duckdb
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from app.modules.normalize_to_duckdb import ensure_table, open_db_connection

parser = argparse.ArgumentParser()
parser.add_argument("--db", default="data/duckdb/test_finanzas.duckdb",
                    help="DuckDB path or md:<name> for MotherDuck")
cli = parser.parse_args()
DB_PATH      = cli.db
IS_MOTHERDUCK = DB_PATH.startswith("md:")

CORTE_DIR = Path("data/inputs/test_cortes")

# ── reset (local only) ───────────────────────────────────────────────────────
if not IS_MOTHERDUCK:
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"Deleted old {DB_PATH}")

CORTE_DIR.mkdir(parents=True, exist_ok=True)

con = open_db_connection(DB_PATH)

# create movimientos first, then migrate.sql adds the rest
ensure_table(con)

sql = open("migrate.sql").read()
clean = re.sub(r"--[^\n]*", "", sql)
for stmt in clean.split(";"):
    stmt = stmt.strip()
    if stmt:
        try:
            con.execute(stmt)
        except Exception as e:
            print(f"  SKIP: {str(e)[:90]}")

print("Schema ready.")

# ── helpers ───────────────────────────────────────────────────────────────────
_seq = 0

def ins_mov(
    fecha, descripcion, monto, tipo, tarjeta_cuenta, banco,
    categoria="", detalle="", moneda="MXN",
    subtipo="normal", cuenta_destino=None, id_referencia=None,
    monto_total=None, meses_totales=None, meses_restantes=None,
    tiene_intereses=False, tasa_interes=None,
):
    global _seq
    _seq += 1
    con.execute(
        "INSERT OR IGNORE INTO movimientos VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            fecha, categoria, detalle, monto, descripcion,
            tarjeta_cuenta, tipo, banco, moneda,
            "seed", f"seed-{_seq:04d}",
            subtipo, cuenta_destino, id_referencia,
            monto_total, meses_totales, meses_restantes,
            tiene_intereses, tasa_interes,
        ),
    )


def ins_transfer(fecha, monto, src_cuenta, src_banco, dst_cuenta, ref_id,
                 categoria="Inversion"):
    ins_mov(fecha, f"Transferencia a {dst_cuenta}", monto, "gasto",
            src_cuenta, src_banco, categoria=categoria,
            detalle="Transferencia interbancaria enviada",
            subtipo="transferencia_interna",
            cuenta_destino=dst_cuenta, id_referencia=ref_id)
    ins_mov(fecha, f"Deposito desde {src_cuenta}", monto, "ingreso",
            dst_cuenta, dst_cuenta, categoria=categoria,
            detalle="Transferencia interbancaria recibida",
            subtipo="transferencia_interna",
            cuenta_destino=dst_cuenta, id_referencia=ref_id)


def ins_sdh(fecha, cuenta_id, saldo, pago_sin_intereses=None, source_file=None, notas=None):
    con.execute(
        """INSERT OR IGNORE INTO saldo_deuda_historico
             (fecha, cuenta_id, saldo, pago_sin_intereses, source_file, notas)
           VALUES (?,?,?,?,?,?)""",
        (fecha, cuenta_id, saldo, pago_sin_intereses, source_file, notas),
    )


def _load_font(size):
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            continue
    return ImageFont.load_default()


def create_corte_image(filename, bank_name, fecha_corte, saldo_total, pago_sin_intereses):
    """Create a fake credit card statement PNG, readable by Tesseract."""
    W, H = 920, 560
    img = Image.new("RGB", (W, H), "white")
    d = ImageDraw.Draw(img)

    f_title  = _load_font(34)
    f_label  = _load_font(27)
    f_sub    = _load_font(22)

    # header
    d.rectangle([(0, 0), (W, 80)], fill="#1a1a2e")
    d.text((30, 20), bank_name, fill="white", font=f_title)

    y = 110
    d.text((30, y), "Estado de Cuenta  —  Tarjeta de Credito", fill="#444", font=f_sub)
    y += 40
    d.text((30, y), f"Fecha de corte: {fecha_corte}", fill="#222", font=f_sub)
    y += 50
    d.line([(30, y), (W - 30, y)], fill="#ccc", width=2)
    y += 30

    rows = [
        ("Saldo total",                    f"${saldo_total:,.2f} MXN"),
        ("Pago minimo",                    f"${saldo_total * 0.025:,.2f} MXN"),
        ("Pago para no generar intereses", f"${pago_sin_intereses:,.2f} MXN"),
    ]
    for label, value in rows:
        d.text((30,  y), label, fill="#111", font=f_label)
        d.text((580, y), value, fill="#111", font=f_label)
        y += 56

    path = CORTE_DIR / filename
    img.save(str(path))
    return path


# ═══════════════════════════════════════════════════════════════════════════════
# Extra account — invisalign (not in migrate.sql seed, specific to this user)
# ═══════════════════════════════════════════════════════════════════════════════
con.execute("""
    INSERT OR IGNORE INTO cuentas VALUES
    ('invisalign', 'Deuda Invisalign', 'deuda', NULL,
     NULL, NULL, NULL, NULL,
     false, 'Tratamiento ortodoncia', true,
     'Mensualidad fija $2,500 MXN')
""")

# ═══════════════════════════════════════════════════════════════════════════════
# movimientos  ── Jan – Apr 2026
# ═══════════════════════════════════════════════════════════════════════════════

# ── SALDOS INICIALES (2026-01-01) ─────────────────────────────────────────────
ins_mov("2026-01-01", "Saldo inicial openbank", 15000.0, "ingreso", "openbank", "openbank",
        categoria="Inversion", detalle="Apertura cuenta ahorro",
        subtipo="saldo_inicial", cuenta_destino="openbank")

ins_mov("2026-01-01", "Saldo inicial ciudad maderas", 8000.0, "ingreso", "ciudad_maderas", "openbank",
        categoria="Inversion", detalle="Apertura meta ahorro viaje",
        subtipo="saldo_inicial", cuenta_destino="ciudad_maderas")

# ── JANUARY 2026 ──────────────────────────────────────────────────────────────
# BBVA
ins_mov("2026-01-05", "Nomina empresa enero",       18500.0, "ingreso", "bbva", "bbva")
ins_mov("2026-01-05", "Pago renta departamento",     7200.0, "gasto",   "bbva", "bbva", categoria="Necesidades")
ins_mov("2026-01-06", "Walmart supermercado",        1823.5, "gasto",   "bbva", "bbva", categoria="Necesidades")
ins_mov("2026-01-07", "CFE luz bimestral",            820.0, "gasto",   "bbva", "bbva", categoria="Necesidades")
ins_mov("2026-01-08", "Telmex internet mensual",     1100.0, "gasto",   "bbva", "bbva", categoria="Necesidades")
ins_mov("2026-01-09", "Gas del hogar",                340.0, "gasto",   "bbva", "bbva", categoria="Necesidades")
ins_mov("2026-01-10", "Netflix suscripcion",          349.0, "gasto",   "bbva", "bbva", categoria="Gustos")
ins_mov("2026-01-10", "Spotify suscripcion",          219.0, "gasto",   "bbva", "bbva", categoria="Gustos")
ins_mov("2026-01-12", "Rappi pedido comida",          385.0, "gasto",   "bbva", "bbva", categoria="Gustos")
ins_mov("2026-01-15", "GBM inversion fondo",         1500.0, "gasto",   "bbva", "bbva", categoria="Inversion")
ins_mov("2026-01-15", "Pago TDC Santander",          3500.0, "gasto",   "bbva", "bbva", categoria="Deuda")
ins_mov("2026-01-18", "Retiro cajero BBVA",          2000.0, "gasto",   "bbva", "bbva")
ins_mov("2026-01-31", "Pago Invisalign mensualidad", 2500.0, "gasto",   "bbva", "bbva", categoria="Deuda", detalle="Mensualidad ortodoncia 1/12")
ins_transfer("2026-01-20", 2000.0, "bbva", "bbva", "ciudad_maderas", "ref-jan-maderas-001")

# Santander
ins_mov("2026-01-08", "Restaurant El Bajio",          780.0, "gasto",   "santander", "santander", categoria="Gustos")
ins_mov("2026-01-10", "Farmacia Guadalajara",          290.0, "gasto",   "santander", "santander", categoria="Necesidades")
ins_mov("2026-01-14", "Gasolina Pemex",               1200.0, "gasto",   "santander", "santander", categoria="Necesidades")
ins_mov("2026-01-15", "Costco supermercado compra",   3100.0, "gasto",   "santander", "santander", categoria="Necesidades")
ins_mov("2026-01-18", "PAGO POR TRANSFERENCIA",       3500.0, "ingreso", "santander", "santander")
ins_mov("2026-01-22", "Amazon Mexico compra",         2340.0, "gasto",   "santander", "santander", categoria="Gustos")

# Plata
ins_mov("2026-01-12", "Uber Eats restaurante",         420.0, "gasto",  "plata", "plata", categoria="Gustos")
ins_mov("2026-01-14", "Starbucks cafeteria",            185.0, "gasto",  "plata", "plata", categoria="Gustos")
ins_mov("2026-01-20", "Airbnb hospedaje",             1200.0, "gasto",  "plata", "plata")

# Mercadopago
ins_mov("2026-01-10", "Compra Mercado Libre electronico", 1650.0, "gasto", "mercadopago", "mercadopago", categoria="Gustos")
ins_mov("2026-01-15", "Compra Mercado Libre ropa",         890.0, "gasto", "mercadopago", "mercadopago")

# ── FEBRUARY 2026 ─────────────────────────────────────────────────────────────
# BBVA
ins_mov("2026-02-05", "Nomina empresa febrero",      18500.0, "ingreso", "bbva", "bbva")
ins_mov("2026-02-05", "Pago renta departamento",      7200.0, "gasto",   "bbva", "bbva", categoria="Necesidades")
ins_mov("2026-02-06", "Walmart supermercado",         1980.0, "gasto",   "bbva", "bbva", categoria="Necesidades")
ins_mov("2026-02-08", "Telmex internet mensual",      1100.0, "gasto",   "bbva", "bbva", categoria="Necesidades")
ins_mov("2026-02-09", "Gas del hogar",                 310.0, "gasto",   "bbva", "bbva", categoria="Necesidades")
ins_mov("2026-02-10", "Netflix suscripcion",           349.0, "gasto",   "bbva", "bbva", categoria="Gustos")
ins_mov("2026-02-10", "Spotify suscripcion",           219.0, "gasto",   "bbva", "bbva", categoria="Gustos")
ins_mov("2026-02-13", "Rappi pedido comida",           620.0, "gasto",   "bbva", "bbva", categoria="Gustos")
ins_mov("2026-02-15", "GBM inversion fondo",          1500.0, "gasto",   "bbva", "bbva", categoria="Inversion")
ins_mov("2026-02-15", "Pago TDC Santander",           4200.0, "gasto",   "bbva", "bbva", categoria="Deuda")
ins_mov("2026-02-18", "Retiro cajero BBVA",           2000.0, "gasto",   "bbva", "bbva")
ins_mov("2026-02-28", "Pago Invisalign mensualidad",  2500.0, "gasto",   "bbva", "bbva", categoria="Deuda", detalle="Mensualidad ortodoncia 2/12")
ins_transfer("2026-02-20", 2000.0, "bbva", "bbva", "ciudad_maderas", "ref-feb-maderas-001")

# Santander — MSI laptop
ins_mov("2026-02-01", "PAGO POR TRANSFERENCIA",       4200.0, "ingreso", "santander", "santander")
ins_mov("2026-02-05", "Liverpool compra electronica", 1833.0, "gasto",   "santander", "santander",
        categoria="Gustos", detalle="MSI cuota 1/18",
        subtipo="msi", monto_total=33000.0, meses_totales=18, meses_restantes=17,
        tiene_intereses=False)
ins_mov("2026-02-08", "Gasolina Pemex",               1050.0, "gasto",   "santander", "santander", categoria="Necesidades")
ins_mov("2026-02-12", "Farmacia del Ahorro",           180.0, "gasto",   "santander", "santander", categoria="Necesidades")
ins_mov("2026-02-15", "Cinepolis cine",                540.0, "gasto",   "santander", "santander", categoria="Gustos")
ins_mov("2026-02-20", "Costco supermercado compra",  2850.0,  "gasto",   "santander", "santander", categoria="Necesidades")

# Plata
ins_mov("2026-02-07", "Uber Eats restaurante",         510.0, "gasto",  "plata", "plata", categoria="Gustos")
ins_mov("2026-02-14", "Cena San Valentin restaurante", 980.0, "gasto",  "plata", "plata", categoria="Gustos")
ins_mov("2026-02-18", "Costo anual tarjeta credito",   850.0, "gasto",  "plata", "plata", categoria="Deuda")
ins_mov("2026-02-22", "Amazon Mexico compra libros",   460.0, "gasto",  "plata", "plata", categoria="Gustos")

# Mercadopago
ins_mov("2026-02-10", "Compra Mercado Libre herramienta", 750.0, "gasto", "mercadopago", "mercadopago", categoria="Necesidades")
ins_mov("2026-02-14", "Compra Mercado Libre regalo",     1200.0, "gasto", "mercadopago", "mercadopago")
ins_mov("2026-02-25", "Recarga saldo mercadopago",       500.0, "ingreso","mercadopago", "mercadopago")

# Merpago — MSI ropa
ins_mov("2026-02-10", "El Palacio de Hierro ropa",    2400.0, "gasto",  "merpago", "merpago",
        categoria="Gustos", detalle="MSI cuota 1/6",
        subtipo="msi", monto_total=14400.0, meses_totales=6, meses_restantes=5,
        tiene_intereses=True, tasa_interes=0.63)

# ── MARCH 2026 ────────────────────────────────────────────────────────────────
# BBVA
ins_mov("2026-03-05", "Nomina empresa marzo",        19000.0, "ingreso", "bbva", "bbva")
ins_mov("2026-03-05", "Pago renta departamento",      7200.0, "gasto",   "bbva", "bbva", categoria="Necesidades")
ins_mov("2026-03-06", "Walmart supermercado",         2100.0, "gasto",   "bbva", "bbva", categoria="Necesidades")
ins_mov("2026-03-07", "CFE luz bimestral",             910.0, "gasto",   "bbva", "bbva", categoria="Necesidades")
ins_mov("2026-03-08", "Telmex internet mensual",      1100.0, "gasto",   "bbva", "bbva", categoria="Necesidades")
ins_mov("2026-03-09", "Gas del hogar",                 290.0, "gasto",   "bbva", "bbva", categoria="Necesidades")
ins_mov("2026-03-10", "Netflix suscripcion",           349.0, "gasto",   "bbva", "bbva", categoria="Gustos")
ins_mov("2026-03-10", "Spotify suscripcion",           219.0, "gasto",   "bbva", "bbva", categoria="Gustos")
ins_mov("2026-03-12", "Rappi pedido comida",           450.0, "gasto",   "bbva", "bbva", categoria="Gustos")
ins_mov("2026-03-15", "GBM inversion fondo",          2000.0, "gasto",   "bbva", "bbva", categoria="Inversion")
ins_mov("2026-03-15", "Pago TDC Santander",           3800.0, "gasto",   "bbva", "bbva", categoria="Deuda")
ins_mov("2026-03-18", "Retiro cajero BBVA",           2000.0, "gasto",   "bbva", "bbva")
ins_mov("2026-03-20", "Bono trimestral empresa",      5000.0, "ingreso", "bbva", "bbva")
ins_mov("2026-03-31", "Pago Invisalign mensualidad",  2500.0, "gasto",   "bbva", "bbva", categoria="Deuda", detalle="Mensualidad ortodoncia 3/12")
ins_transfer("2026-03-21", 3000.0, "bbva", "bbva", "openbank", "ref-mar-openbank-001")

# Santander — MSI cuota 2
ins_mov("2026-03-01", "PAGO POR TRANSFERENCIA",       3800.0, "ingreso", "santander", "santander")
ins_mov("2026-03-05", "Liverpool compra electronica", 1833.0, "gasto",   "santander", "santander",
        categoria="Gustos", detalle="MSI cuota 2/18",
        subtipo="msi", monto_total=33000.0, meses_totales=18, meses_restantes=16,
        tiene_intereses=False)
ins_mov("2026-03-08", "Gasolina Pemex",               1150.0, "gasto",   "santander", "santander", categoria="Necesidades")
ins_mov("2026-03-12", "Restaurant Toks",               650.0, "gasto",   "santander", "santander", categoria="Gustos")
ins_mov("2026-03-18", "Farmacia del Ahorro",           340.0, "gasto",   "santander", "santander", categoria="Necesidades")
ins_mov("2026-03-22", "Amazon Mexico electronico",    1890.0, "gasto",   "santander", "santander", categoria="Gustos")
ins_mov("2026-03-25", "Costco supermercado compra",  3200.0,  "gasto",   "santander", "santander", categoria="Necesidades")

# Plata
ins_mov("2026-03-05", "Uber Eats restaurante",         390.0, "gasto",  "plata", "plata", categoria="Gustos")
ins_mov("2026-03-10", "Starbucks cafeteria",           165.0, "gasto",  "plata", "plata", categoria="Gustos")
ins_mov("2026-03-15", "Airbnb hospedaje semana santa", 4500.0, "gasto", "plata", "plata")
ins_mov("2026-03-20", "Compra sin descripcion 8821",   230.0, "gasto",  "plata", "plata")

# Mercadopago
ins_mov("2026-03-08", "Compra Mercado Libre zapatos",  1100.0, "gasto", "mercadopago", "mercadopago")
ins_mov("2026-03-15", "Compra Mercado Libre cocina",    780.0, "gasto", "mercadopago", "mercadopago", categoria="Necesidades")
ins_mov("2026-03-20", "Compra Mercado Libre juguete",   450.0, "gasto", "mercadopago", "mercadopago")

# Merpago — MSI cuota 2
ins_mov("2026-03-10", "El Palacio de Hierro ropa",    2400.0, "gasto",  "merpago", "merpago",
        categoria="Gustos", detalle="MSI cuota 2/6",
        subtipo="msi", monto_total=14400.0, meses_totales=6, meses_restantes=4,
        tiene_intereses=True, tasa_interes=0.63)

# ── APRIL 2026 ────────────────────────────────────────────────────────────────
# BBVA
ins_mov("2026-04-05", "Nomina empresa abril",        19000.0, "ingreso", "bbva", "bbva")
ins_mov("2026-04-05", "Pago renta departamento",      7200.0, "gasto",   "bbva", "bbva", categoria="Necesidades")
ins_mov("2026-04-06", "Walmart supermercado",         1843.5, "gasto",   "bbva", "bbva", categoria="Necesidades")
ins_mov("2026-04-08", "Telmex internet mensual",      1100.0, "gasto",   "bbva", "bbva", categoria="Necesidades")
ins_mov("2026-04-09", "Gas del hogar",                 320.0, "gasto",   "bbva", "bbva", categoria="Necesidades")
ins_mov("2026-04-10", "Netflix suscripcion",           349.0, "gasto",   "bbva", "bbva", categoria="Gustos")
ins_mov("2026-04-10", "Spotify suscripcion",           219.0, "gasto",   "bbva", "bbva", categoria="Gustos")
ins_mov("2026-04-12", "Rappi pedido comida",           680.0, "gasto",   "bbva", "bbva", categoria="Gustos")
ins_mov("2026-04-14", "CFE luz bimestral",             875.0, "gasto",   "bbva", "bbva", categoria="Necesidades")
ins_mov("2026-04-15", "GBM inversion fondo",          2000.0, "gasto",   "bbva", "bbva", categoria="Inversion")
ins_mov("2026-04-15", "Pago TDC Santander",           5000.0, "gasto",   "bbva", "bbva", categoria="Deuda")
ins_mov("2026-04-18", "Retiro cajero BBVA",           2000.0, "gasto",   "bbva", "bbva")
ins_mov("2026-04-20", "Devolucion Amazon",             450.0, "ingreso", "bbva", "bbva")
ins_transfer("2026-04-22", 2500.0, "bbva", "bbva", "ciudad_maderas", "ref-apr-maderas-001")

# Santander — MSI cuota 3
ins_mov("2026-04-01", "PAGO POR TRANSFERENCIA",       5000.0, "ingreso", "santander", "santander")
ins_mov("2026-04-05", "Liverpool compra electronica", 1833.0, "gasto",   "santander", "santander",
        categoria="Gustos", detalle="MSI cuota 3/18",
        subtipo="msi", monto_total=33000.0, meses_totales=18, meses_restantes=15,
        tiene_intereses=False)
ins_mov("2026-04-07", "Costco supermercado compra",  2780.0,  "gasto",   "santander", "santander", categoria="Necesidades")
ins_mov("2026-04-09", "Starbucks cafeteria",           340.0, "gasto",   "santander", "santander", categoria="Gustos")
ins_mov("2026-04-12", "Gasolina Pemex",               1300.0, "gasto",   "santander", "santander", categoria="Necesidades")
ins_mov("2026-04-16", "Farmacia Guadalajara",          210.0, "gasto",   "santander", "santander", categoria="Necesidades")
ins_mov("2026-04-20", "Cargo comision anual tarjeta",  480.0, "gasto",   "santander", "santander", categoria="Deuda")

# Plata
ins_mov("2026-04-03", "Uber Eats restaurante",         460.0, "gasto",  "plata", "plata", categoria="Gustos")
ins_mov("2026-04-08", "Amazon Mexico compra",         4200.0, "gasto",  "plata", "plata", categoria="Gustos")
ins_mov("2026-04-13", "Costo anual tarjeta credito",   850.0, "gasto",  "plata", "plata", categoria="Deuda")
ins_mov("2026-04-18", "Compra sin descripcion 9120",   175.0, "gasto",  "plata", "plata")

# Mercadopago
ins_mov("2026-04-05", "Compra Mercado Libre electronico", 2100.0, "gasto", "mercadopago", "mercadopago", categoria="Gustos")
ins_mov("2026-04-12", "Compra Mercado Libre deporte",      680.0, "gasto", "mercadopago", "mercadopago")
ins_mov("2026-04-15", "Recarga saldo mercadopago",         500.0, "ingreso","mercadopago", "mercadopago")

# Merpago — MSI cuota 3
ins_mov("2026-04-10", "El Palacio de Hierro ropa",    2400.0, "gasto",  "merpago", "merpago",
        categoria="Gustos", detalle="MSI cuota 3/6",
        subtipo="msi", monto_total=14400.0, meses_totales=6, meses_restantes=3,
        tiene_intereses=True, tasa_interes=0.63)

total_movs = con.execute("SELECT COUNT(*) FROM movimientos").fetchone()[0]
print(f"movimientos: {total_movs} rows inserted")

# ═══════════════════════════════════════════════════════════════════════════════
# presupuesto_metas  ── 4 months × 4 categories = 16 rows
# ═══════════════════════════════════════════════════════════════════════════════

metas = [
    (2026, 1, "Necesidades", 0.50, 18500.0),
    (2026, 1, "Gustos",      0.20, 18500.0),
    (2026, 1, "Inversion",   0.20, 18500.0),
    (2026, 1, "Deuda",       0.10, 18500.0),
    (2026, 2, "Necesidades", 0.50, 18500.0),
    (2026, 2, "Gustos",      0.20, 18500.0),
    (2026, 2, "Inversion",   0.20, 18500.0),
    (2026, 2, "Deuda",       0.10, 18500.0),
    (2026, 3, "Necesidades", 0.50, 19000.0),
    (2026, 3, "Gustos",      0.20, 19000.0),
    (2026, 3, "Inversion",   0.20, 19000.0),
    (2026, 3, "Deuda",       0.10, 19000.0),
    (2026, 4, "Necesidades", 0.50, 19000.0),
    (2026, 4, "Gustos",      0.20, 19000.0),
    (2026, 4, "Inversion",   0.20, 19000.0),
    (2026, 4, "Deuda",       0.10, 19000.0),
]
for anio, mes, cat, pct, base in metas:
    con.execute(
        "INSERT OR IGNORE INTO presupuesto_metas (anio, mes, categoria, porcentaje, monto_meta, ingreso_base) VALUES (?,?,?,?,?,?)",
        (anio, mes, cat, pct, round(base * pct, 2), base),
    )

total_pm = con.execute("SELECT COUNT(*) FROM presupuesto_metas").fetchone()[0]
print(f"presupuesto_metas: {total_pm} rows inserted")

# ═══════════════════════════════════════════════════════════════════════════════
# saldo_deuda_historico  ── credit cards (4 accounts × 4 months) + invisalign
# ═══════════════════════════════════════════════════════════════════════════════

# Credit card cortes
credit_hist = [
    # January
    ("2026-01-31", "santander_free", 9800.0,   9800.0,   "corte_santander_ene26.jpg",  "Corte enero"),
    ("2026-01-31", "santander_amex", 4200.0,   4200.0,   "corte_amex_ene26.jpg",       "Corte enero"),
    ("2026-01-31", "plata",          3100.0,   3100.0,   "corte_plata_ene26.jpg",      "Corte enero"),
    ("2026-01-31", "merpago",        2400.0,   2400.0,   "corte_merpago_ene26.jpg",    "Apertura MSI ropa"),
    # February
    ("2026-02-28", "santander_free", 38833.0,  38833.0,  "corte_santander_feb26.jpg",  "Incluye MSI laptop 18 meses"),
    ("2026-02-28", "santander_amex", 3800.0,   3800.0,   "corte_amex_feb26.jpg",       "Corte febrero"),
    ("2026-02-28", "plata",          2650.0,   2650.0,   "corte_plata_feb26.jpg",      "Corte febrero"),
    ("2026-02-28", "merpago",        12000.0,  2400.0,   "corte_merpago_feb26.jpg",    "MSI ropa pago mensual"),
    # March
    ("2026-03-31", "santander_free", 36000.0,  1833.0,   "corte_santander_mar26.jpg",  "MSI laptop cuota 2/18"),
    ("2026-03-31", "santander_amex", 4100.0,   4100.0,   "corte_amex_mar26.jpg",       "Corte marzo"),
    ("2026-03-31", "plata",          5200.0,   5200.0,   "corte_plata_mar26.jpg",      "Incluye Airbnb"),
    ("2026-03-31", "merpago",        9600.0,   2400.0,   "corte_merpago_mar26.jpg",    "MSI ropa cuota 3/6"),
    # April
    ("2026-04-30", "santander_free", 33167.0,  1833.0,   "corte_santander_abr26.jpg",  "MSI laptop cuota 3/18"),
    ("2026-04-30", "santander_amex", 5100.0,   5100.0,   "corte_amex_abr26.jpg",       "Corte abril"),
    ("2026-04-30", "plata",          5700.0,   5700.0,   "corte_plata_abr26.jpg",      "Corte abril"),
    ("2026-04-30", "merpago",        7200.0,   2400.0,   "corte_merpago_abr26.jpg",    "MSI ropa cuota 4/6"),
]
for row in credit_hist:
    ins_sdh(*row)

# Invisalign personal debt — negative saldo = what you still owe
invisalign_hist = [
    # (fecha, saldo_restante, pago_mensual, source_file, notas)
    ("2025-12-31", -30000.0, 2500.0, None,                          "Inicio tratamiento"),
    ("2026-01-31", -27500.0, 2500.0, "recibo_invisalign_ene26.jpg", "Pago mensualidad 1/12"),
    ("2026-02-28", -25000.0, 2500.0, "recibo_invisalign_feb26.jpg", "Pago mensualidad 2/12"),
    ("2026-03-31", -22500.0, 2500.0, "recibo_invisalign_mar26.jpg", "Pago mensualidad 3/12"),
]
for fecha, saldo, pago_si, src, notas in invisalign_hist:
    ins_sdh(fecha, "invisalign", saldo, pago_si, src, notas)

total_sdh = con.execute("SELECT COUNT(*) FROM saldo_deuda_historico").fetchone()[0]
print(f"saldo_deuda_historico: {total_sdh} rows inserted")

# ═══════════════════════════════════════════════════════════════════════════════
# Fake corte images for `banketl corte` testing
# ═══════════════════════════════════════════════════════════════════════════════

cortes = [
    ("test_corte_santander_may26.png", "BANCO SANTANDER",    "31/05/2026", 31334.0,  1833.0 ),
    ("test_corte_plata_may26.png",     "PLATA CARD",         "31/05/2026",  5850.0,  5850.0 ),
    ("test_corte_merpago_may26.png",   "MERCADO PAGO TDC",   "31/05/2026",  4800.0,  2400.0 ),
    ("test_corte_amex_may26.png",      "SANTANDER AMEX",     "31/05/2026",  6200.0,  6200.0 ),
]

for filename, bank, fecha, saldo, pago in cortes:
    path = create_corte_image(filename, bank, fecha, saldo, pago)
    print(f"  Created {path}")

print(f"Corte images saved to {CORTE_DIR}/")

# ═══════════════════════════════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════════════════════════════
print()
print("=== Final row counts ===")
for tbl in ("cuentas", "movimientos", "presupuesto_metas", "saldo_deuda_historico"):
    n = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    print(f"  {tbl:<28} {n} rows")

print()
print("=== cuentas ===")
df = con.execute("SELECT id, nombre, tipo FROM cuentas ORDER BY tipo, id").df()
print(df.to_string(index=False))

print()
print("=== invisalign debt progress ===")
df = con.execute("""
    SELECT fecha, saldo, pago_sin_intereses, notas
    FROM saldo_deuda_historico
    WHERE cuenta_id = 'invisalign'
    ORDER BY fecha
""").df()
print(df.to_string(index=False))

con.close()
print()
print("Done. DB:", DB_PATH)
