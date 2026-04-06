🧾 OCR Bank ETL

End-to-end pipeline to extract financial transactions from bank screenshots using OCR, transform them into structured data, and store them in DuckDB for analytics.

🚀 Features
📸 OCR extraction from images (Tesseract)
⚙️ YAML-driven parsing per bank
🧠 Robust handling of OCR errors (decimals, signs, layouts)
🗂️ Normalization into a unified schema
🧮 Deduplication using hash_id
💱 Multi-currency support (MXN / USD)
📊 Ready for BI tools (Metabase)
🏗️ Architecture
OCR (Tesseract)
    ↓
YAML Parser (bank-specific logic)
    ↓
CSV Staging
    ↓
Normalization + hash_id
    ↓
DuckDB
    ↓
Metabase Dashboards
📁 Project Structure
ocr_bank_statements/
│
├── app/
│   ├── main.py
│   └── modules/
│       ├── ocr_extract.py
│       ├── ocr_to_csv.py
│       ├── normalize_to_duckdb.py
│       └── pipeline.py
│
├── config/
│   ├── plata.yaml
│   ├── bbva.yaml
│   ├── santander_mobile.yaml
│   ├── mercadopago_mobile.yaml
│   └── openbank_mobile.yaml
│
├── data/
│   ├── inputs/
│   ├── raw_text/
│   ├── staging_csv/
│   └── duckdb/
│
├── requirements.txt
├── setup.py
└── README.md
🧠 Supported Banks
Bank	Strategy
Plata	Multiline + cashback
BBVA	Detail lines
Santander	Positional triplets
MercadoPago	Column OCR collapse
Openbank	Block + detail merge
⚙️ Setup (from scratch)
git clone <repo_url>
cd ocr_bank_statements

python3 -m venv .venv
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt

# optional (recommended)
pip install -e .

Verify:

banketl --help
🧪 Usage
1. OCR
banketl ocr --input data/inputs/file.jpg --show
2. Parse
banketl parse \
  --input data/raw_text/file.txt \
  --config config/<bank>.yaml
3. Load to DB
banketl load \
  --input data/staging_csv/file.csv \
  --bank bbva \
  --year 2026
4. Full Pipeline
banketl pipeline \
  --input data/inputs/file.jpg \
  --config config/<bank>.yaml \
  --bank bbva \
  --year 2026
🧾 Output Schema
fecha
categoria
detalle
monto
descripcion
tarjeta_cuenta
tipo
moneda
banco
source_file
hash_id
🧩 OCR Challenges Solved
Problem	Solution
3,299:35	Normalize to 3299.35
Missing signs	Force type
Column layouts	Positional parsing
Multiline descriptions	Block merging
Sign inversion (Santander)	Configurable logic
📊 Example Output
fecha,categoria,detalle,monto,descripcion,tarjeta_cuenta,tipo,moneda
17-11-2025,,Mercado Libre,240.00,Compra,mercadopago,gasto,MXN
🔧 Adding a New Bank
Create YAML config in /config
Define:
regex patterns
ignore rules
parsing mode
(Optional) Implement parser
Register in parser registry
Run pipeline
🛣️ Roadmap
🧠 Category classifier (inversion, necesidades, gustos, deuda)
🌐 Streamlit UI for uploads
🤖 Auto bank detection from OCR
📱 Mobile app
📊 Advanced dashboards
💡 Design Principles
Modular ETL architecture
YAML-driven flexibility
Separation of concerns
Extensible to any bank
📌 Status

🚧 Actively evolving
✅ Multi-bank OCR parsing working
✅ End-to-end pipeline functional
