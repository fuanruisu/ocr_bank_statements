-- migrate.sql — Run once against MotherDuck (safe to re-run, all statements are idempotent)
-- Usage: banketl load connects via open_db_connection; run this file manually once before loading.

-- ============================================================
-- 1. Extend movimientos with new columns
-- ============================================================

ALTER TABLE movimientos ADD COLUMN IF NOT EXISTS subtipo VARCHAR DEFAULT 'normal';
-- Values: 'normal' | 'transferencia_interna' | 'msi' | 'inversion_ahorro' | 'saldo_inicial'

ALTER TABLE movimientos ADD COLUMN IF NOT EXISTS cuenta_destino VARCHAR;
-- For internal transfers: destination sub-account id (e.g. 'ciudad_maderas', 'openbank')

ALTER TABLE movimientos ADD COLUMN IF NOT EXISTS id_referencia VARCHAR;
-- Links the A<->B pair of an internal transfer (both rows share the same id_referencia)

ALTER TABLE movimientos ADD COLUMN IF NOT EXISTS monto_total DOUBLE;
-- MSI only: original full purchase amount (e.g. 12000). NULL for all other subtypes.

ALTER TABLE movimientos ADD COLUMN IF NOT EXISTS meses_totales INTEGER;
-- MSI only: total number of installments (e.g. 12)

ALTER TABLE movimientos ADD COLUMN IF NOT EXISTS meses_restantes INTEGER;
-- MSI only: installments still pending (updated manually)

ALTER TABLE movimientos ADD COLUMN IF NOT EXISTS tiene_intereses BOOLEAN DEFAULT false;
-- MSI only: true if the installment plan accrues interest

ALTER TABLE movimientos ADD COLUMN IF NOT EXISTS tasa_interes DOUBLE;
-- Annual interest rate (e.g. 0.56 = 56%) — only when tiene_intereses = true

-- ============================================================
-- 2. cuentas
-- ============================================================

CREATE TABLE IF NOT EXISTS cuentas (
  id               VARCHAR PRIMARY KEY,
  nombre           VARCHAR NOT NULL,
  tipo             VARCHAR NOT NULL,      -- 'credito' | 'debito' | 'ahorro' | 'inversion'
  banco            VARCHAR,
  limite_credito   DOUBLE,
  tasa_interes     DOUBLE,                -- annual rate e.g. 0.56
  fecha_corte      INTEGER,               -- billing cycle day e.g. 15
  fecha_pago       INTEGER,               -- payment due day
  es_subcuenta     BOOLEAN DEFAULT false, -- true for savings sub-accounts
  objetivo_ahorro  VARCHAR,
  activa           BOOLEAN DEFAULT true,
  notas            VARCHAR
);

INSERT OR IGNORE INTO cuentas VALUES
  ('bbva',           'BBVA Debito',           'debito',   'BBVA',        NULL, 0.56,  NULL, NULL, false, NULL,              true, NULL),
  ('santander_free', 'TDC Santander Free',    'credito',  'Santander',   NULL, 0.699, NULL, NULL, false, NULL,              true, NULL),
  ('santander_amex', 'TDC Santander Amex',    'credito',  'Santander',   NULL, 0.649, NULL, NULL, false, NULL,              true, NULL),
  ('merpago',        'MERPAGO TDC',           'credito',  'MERPAGO',     NULL, 0.63,  NULL, NULL, false, NULL,              true, NULL),
  ('plata',          'Plata TDC',             'credito',  'Plata',       NULL, NULL,  NULL, NULL, false, NULL,              true, NULL),
  ('mercadopago',    'MercadoPago',           'debito',   'MercadoPago', NULL, NULL,  NULL, NULL, false, NULL,              true, NULL),
  ('ciudad_maderas', 'Ahorro Ciudad Maderas', 'ahorro',   NULL,          NULL, NULL,  NULL, NULL, true,  'Ciudad Maderas',  true, NULL),
  ('openbank',       'Openbank',              'ahorro',   'Openbank',    NULL, NULL,  NULL, NULL, true,  'Ahorro general',  true, NULL);

-- ============================================================
-- 3. presupuesto_metas
-- ============================================================

CREATE TABLE IF NOT EXISTS presupuesto_metas (
  id           VARCHAR PRIMARY KEY DEFAULT gen_random_uuid(),
  anio         INTEGER NOT NULL,
  mes          INTEGER NOT NULL,
  categoria    VARCHAR NOT NULL,   -- 'Necesidades' | 'Gustos' | 'Inversion' | 'Deuda'
  porcentaje   DOUBLE,             -- e.g. 0.30
  monto_meta   DOUBLE,             -- ingreso_base * porcentaje
  ingreso_base DOUBLE,
  UNIQUE (anio, mes, categoria)
);

-- ============================================================
-- 4. saldo_deuda_historico
-- ============================================================

CREATE TABLE IF NOT EXISTS saldo_deuda_historico (
  id                 VARCHAR PRIMARY KEY DEFAULT gen_random_uuid(),
  fecha              DATE    NOT NULL,
  cuenta_id          VARCHAR NOT NULL REFERENCES cuentas(id),
  saldo              DOUBLE  NOT NULL,
  pago_sin_intereses DOUBLE,
  source_file        VARCHAR,
  notas              VARCHAR,
  UNIQUE (cuenta_id, fecha)
);

ALTER TABLE saldo_deuda_historico ADD COLUMN IF NOT EXISTS pago_sin_intereses DOUBLE;
ALTER TABLE saldo_deuda_historico ADD COLUMN IF NOT EXISTS source_file VARCHAR;

-- ============================================================
-- 5. Views
-- ============================================================

CREATE OR REPLACE VIEW v_gastos_mes AS
SELECT
  strftime('%Y-%m', fecha) AS mes,
  categoria,
  COUNT(*)                  AS num_movimientos,
  ROUND(SUM(monto), 2)      AS total,
  moneda
FROM movimientos
WHERE tipo    = 'gasto'
  AND subtipo NOT IN ('transferencia_interna', 'saldo_inicial')
GROUP BY mes, categoria, moneda;

-- ----------------------------------------------------------------

CREATE OR REPLACE VIEW v_balance_mensual AS
SELECT
  strftime('%Y-%m', fecha) AS mes,
  ROUND(SUM(CASE WHEN tipo = 'ingreso' AND subtipo NOT IN ('transferencia_interna','saldo_inicial') THEN  monto ELSE 0 END), 2) AS total_ingresos,
  ROUND(SUM(CASE WHEN tipo = 'gasto'   AND subtipo NOT IN ('transferencia_interna','saldo_inicial') THEN  monto ELSE 0 END), 2) AS total_gastos,
  ROUND(SUM(CASE WHEN tipo = 'ingreso' AND subtipo NOT IN ('transferencia_interna','saldo_inicial') THEN  monto
                 WHEN tipo = 'gasto'   AND subtipo NOT IN ('transferencia_interna','saldo_inicial') THEN -monto
                 ELSE 0 END), 2) AS balance_neto,
  moneda
FROM movimientos
GROUP BY mes, moneda;

-- ----------------------------------------------------------------

CREATE OR REPLACE VIEW v_presupuesto_vs_real AS
SELECT
  pm.anio,
  pm.mes,
  pm.categoria,
  pm.monto_meta,
  pm.porcentaje,
  COALESCE(SUM(m.monto), 0)                                    AS total_real,
  pm.monto_meta - COALESCE(SUM(m.monto), 0)                   AS diferencia,
  ROUND(COALESCE(SUM(m.monto), 0) / pm.monto_meta * 100, 1)  AS pct_usado
FROM presupuesto_metas pm
LEFT JOIN movimientos m
  ON strftime('%Y', m.fecha) = CAST(pm.anio AS VARCHAR)
 AND strftime('%m', m.fecha) = printf('%02d', pm.mes)
 AND m.categoria              = pm.categoria
 AND m.tipo                   = 'gasto'
 AND m.subtipo NOT IN ('transferencia_interna', 'saldo_inicial')
GROUP BY pm.anio, pm.mes, pm.categoria, pm.monto_meta, pm.porcentaje;

-- ----------------------------------------------------------------

CREATE OR REPLACE VIEW v_msi_activos AS
SELECT
  hash_id,
  fecha                             AS fecha_compra,
  descripcion,
  detalle,
  tarjeta_cuenta,
  monto_total,
  monto                             AS pago_mensual,
  meses_totales,
  meses_restantes,
  tiene_intereses,
  tasa_interes,
  ROUND(monto * meses_restantes, 2) AS deuda_restante
FROM movimientos
WHERE subtipo = 'msi'
  AND meses_restantes > 0;

-- ----------------------------------------------------------------

CREATE OR REPLACE VIEW v_ahorro_subcuentas AS
SELECT
  cuenta_destino                                                         AS subcuenta,
  ROUND(SUM(CASE WHEN tipo = 'ingreso' THEN monto ELSE -monto END), 2) AS saldo_actual,
  COUNT(*)                                                               AS num_movimientos,
  MAX(fecha)                                                             AS ultimo_movimiento
FROM movimientos
WHERE subtipo IN ('transferencia_interna', 'saldo_inicial', 'inversion_ahorro')
  AND cuenta_destino IS NOT NULL
GROUP BY cuenta_destino;

-- ----------------------------------------------------------------

CREATE OR REPLACE VIEW v_top_comercios AS
SELECT
  COALESCE(NULLIF(descripcion, ''), NULLIF(detalle, ''), 'Sin descripcion') AS comercio,
  strftime('%Y-%m', fecha)                                                    AS mes,
  COUNT(*)                                                                    AS veces,
  ROUND(SUM(monto), 2)                                                        AS total,
  moneda
FROM movimientos
WHERE tipo    = 'gasto'
  AND subtipo = 'normal'
GROUP BY comercio, mes, moneda
ORDER BY total DESC;
