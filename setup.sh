#!/usr/bin/env bash
set -euo pipefail

VENV_DIR=".venv"
REQ_FILE="requirements.txt"

echo "==> Iniciando setup del proyecto"

if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 no está instalado."
  exit 1
fi

echo "==> Usando Python: $(command -v python3)"
python3 --version

# Crear venv si no existe
if [ ! -d "$VENV_DIR" ]; then
  echo "==> Creando entorno virtual en $VENV_DIR"
  python3 -m venv "$VENV_DIR"
else
  echo "==> El entorno virtual ya existe en $VENV_DIR"
fi

# Activar venv
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

echo "==> Python activo: $(command -v python)"
echo "==> Pip activo: $(command -v pip || true)"

# Asegurar pip dentro del venv
python -m ensurepip --upgrade >/dev/null 2>&1 || true
python -m pip install --upgrade pip setuptools wheel

# Crear requirements.txt por defecto si no existe
if [ ! -f "$REQ_FILE" ]; then
  echo "==> No existe $REQ_FILE, creando uno por defecto"
  cat > "$REQ_FILE" <<'EOF'
pillow
pytesseract
pdf2image
pandas
EOF
fi

echo "==> Instalando dependencias Python desde $REQ_FILE"
python -m pip install -r "$REQ_FILE"

echo "==> Verificando dependencias del sistema"
MISSING=0

if ! command -v tesseract >/dev/null 2>&1; then
  echo "AVISO: falta tesseract"
  MISSING=1
fi

if ! command -v pdftoppm >/dev/null 2>&1; then
  echo "AVISO: falta poppler-utils (pdftoppm)"
  MISSING=1
fi

if [ "$MISSING" -eq 1 ]; then
  echo
  echo "Instala estas dependencias del sistema:"
  echo "  sudo apt update"
  echo "  sudo apt install -y tesseract-ocr tesseract-ocr-spa poppler-utils"
  echo
else
  echo "==> Dependencias del sistema OK"
fi

echo "==> Creando directorios base"
mkdir -p data/inputs data/raw_text data/staging_csv config

echo
echo "==> Setup completado"
echo "Para activar el entorno en futuras sesiones:"
echo "  source $VENV_DIR/bin/activate"
echo
echo "Para probar tu app:"
echo "  python app/main.py --help"
