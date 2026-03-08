# OCR Bank Statements

Herramienta CLI para convertir estados de cuenta (PDF o screenshots)
a CSV usando OCR.

## Pipeline

input → OCR → raw_text → parser → CSV staging

## Uso

### Ver solo OCR

python app/main.py --input data/inputs/plataSS.jpg --ocr_only

### Pipeline completo

python app/main.py --input data/inputs/plataSS.jpg

### Especificar directorio output

python app/main.py --input estado.pdf --output data

## Estructura

app/  
- main.py  
- ocr_extract.py  
- ocr_to_csv.py  

config/  
- archivos YAML (futuro)

data/  
- inputs/  
- raw_text/  
- staging_csv/
