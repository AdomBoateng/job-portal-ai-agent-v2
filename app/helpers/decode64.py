import base64
import io
import zipfile

from docx import Document
from pypdf import PdfReader

from app.helpers.logging_config import get_logger

logger = get_logger("helpers.decode64")


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    parts = []
    for page in reader.pages:
        page_text = page.extract_text() or ""
        if page_text:
            parts.append(page_text)
    return "\n".join(parts).strip()


def _is_docx_file(doc_bytes: bytes) -> bool:
    if not doc_bytes.startswith(b"PK\x03\x04"):
        return False
    try:
        with zipfile.ZipFile(io.BytesIO(doc_bytes)) as zf:
            return "word/document.xml" in zf.namelist()
    except zipfile.BadZipFile:
        return False


def _extract_docx_text(doc_bytes: bytes) -> str:
    doc = Document(io.BytesIO(doc_bytes))
    parts = [para.text for para in doc.paragraphs if para.text]
    return "\n".join(parts).strip()


def decode_base64_text(encoded_text: str) -> str:
    """Decode base64 encoded content to text, extracting PDFs when detected."""
    try:
        logger.debug(f"Decoding base64 text of length: {len(encoded_text)}")
        decoded_bytes = base64.b64decode(encoded_text)

        if decoded_bytes.startswith(b"%PDF-"):
            logger.debug("Detected PDF payload, extracting text")
            extracted_text = _extract_pdf_text(decoded_bytes)
            if extracted_text:
                logger.debug(f"Extracted {len(extracted_text)} characters from PDF")
                return extracted_text
            logger.warning("PDF text extraction returned empty content")
            return ""

        if _is_docx_file(decoded_bytes):
            logger.debug("Detected DOCX payload, extracting text")
            extracted_text = _extract_docx_text(decoded_bytes)
            if extracted_text:
                logger.debug(f"Extracted {len(extracted_text)} characters from DOCX")
                return extracted_text
            logger.warning("DOCX text extraction returned empty content")
            return ""

        decoded_text = decoded_bytes.decode("utf-8", errors="replace")
        logger.debug(f"Successfully decoded to {len(decoded_text)} characters")
        return decoded_text
    except Exception as e:
        logger.error(f"Failed to decode base64 text: {e}")
        raise