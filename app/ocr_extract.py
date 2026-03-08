from pathlib import Path
from PIL import Image
import pytesseract
from pdf2image import convert_from_path

OCR_LANG = "spa+eng"


def ocr_image(image_path):

    img = Image.open(image_path).convert("RGB")

    text = pytesseract.image_to_string(img, lang=OCR_LANG)

    return text


def ocr_pdf(pdf_path):

    pages = convert_from_path(pdf_path, dpi=250)

    texts = []

    for idx, page in enumerate(pages, start=1):

        txt = pytesseract.image_to_string(page, lang=OCR_LANG)

        texts.append(f"=== PAGE {idx} ===\n{txt}")

    return "\n".join(texts)


def extract_text(input_path, out_dir):

    input_path = Path(input_path)

    out_dir.mkdir(parents=True, exist_ok=True)

    if input_path.suffix.lower() == ".pdf":
        text = ocr_pdf(str(input_path))
    else:
        text = ocr_image(str(input_path))

    out_file = out_dir / f"{input_path.stem}.txt"

    out_file.write_text(text, encoding="utf-8")

    return out_file
