"""
Google Document AI Integration for Nexodify Forensic Engine
High-quality OCR extraction for Italian legal documents (Perizia/CTU)
"""

import os
import io
import logging
from typing import List, Dict, Any, Optional
from google.cloud import documentai_v1 as documentai
from google.api_core.client_options import ClientOptions
from PyPDF2 import PdfReader, PdfWriter

logger = logging.getLogger(__name__)

# Configuration from environment
GOOGLE_CLOUD_PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT', 'emergent-perizia')
GOOGLE_CLOUD_LOCATION = os.environ.get('GOOGLE_CLOUD_LOCATION', 'eu')
DOCUMENT_AI_PROCESSOR_ID = os.environ.get('DOCUMENT_AI_PROCESSOR_ID', '675530c0dde80224')
DOC_AI_TIMEOUT_SECONDS = int(os.environ.get('DOC_AI_TIMEOUT_SECONDS', '30'))

# Page limit for Document AI (15 pages per request for non-imageless mode)
MAX_PAGES_PER_REQUEST = 15

class GoogleDocumentAIExtractor:
    """
    Google Document AI OCR extractor for high-quality text extraction
    from Italian legal documents with table and entity extraction.
    """
    
    def __init__(self):
        self.project_id = GOOGLE_CLOUD_PROJECT
        self.location = GOOGLE_CLOUD_LOCATION
        self.processor_id = DOCUMENT_AI_PROCESSOR_ID
        
        # Initialize client with location-specific endpoint
        self.client_options = ClientOptions(
            api_endpoint=f"{self.location}-documentai.googleapis.com"
        )
        self.client = documentai.DocumentProcessorServiceClient(
            client_options=self.client_options
        )
        
        # Build processor name
        self.processor_name = self.client.processor_path(
            self.project_id,
            self.location,
            self.processor_id
        )
        
        logger.info(f"Initialized Google Document AI with processor: {self.processor_name}")
    
    def _split_pdf(self, file_content: bytes, max_pages: int = MAX_PAGES_PER_REQUEST) -> List[tuple]:
        """
        Split a PDF into chunks if it exceeds the page limit.
        
        Returns:
            List of tuples: (chunk_bytes, start_page, end_page)
        """
        try:
            pdf_reader = PdfReader(io.BytesIO(file_content))
            total_pages = len(pdf_reader.pages)
            
            if total_pages <= max_pages:
                return [(file_content, 1, total_pages)]
            
            chunks = []
            for start_page in range(0, total_pages, max_pages):
                end_page = min(start_page + max_pages, total_pages)
                
                pdf_writer = PdfWriter()
                for page_num in range(start_page, end_page):
                    pdf_writer.add_page(pdf_reader.pages[page_num])
                
                output = io.BytesIO()
                pdf_writer.write(output)
                chunk_bytes = output.getvalue()
                
                # Page numbers are 1-indexed for user display
                chunks.append((chunk_bytes, start_page + 1, end_page))
                logger.info(f"Created PDF chunk: pages {start_page + 1}-{end_page}")
            
            return chunks
            
        except Exception as e:
            logger.error(f"Error splitting PDF: {e}")
            return [(file_content, 1, 0)]  # Return original as single chunk
    
    def process_document(self, file_content: bytes, mime_type: str = "application/pdf") -> Dict[str, Any]:
        """
        Process a document using Google Document AI OCR.
        Automatically splits large PDFs into chunks.
        
        Args:
            file_content: Raw bytes of the PDF/image file
            mime_type: MIME type of the document (application/pdf, image/png, etc.)
            
        Returns:
            Dictionary with pages data, full text, and metadata
        """
        try:
            logger.info(f"Processing document with Google Document AI ({len(file_content)} bytes)")
            
            all_pages = []
            full_text_parts = []
            
            # Split PDF if needed
            if mime_type == "application/pdf":
                chunks = self._split_pdf(file_content)
            else:
                chunks = [(file_content, 1, 1)]
            
            logger.info(f"Processing {len(chunks)} chunk(s)")
            
            for chunk_idx, (chunk_bytes, start_page, end_page) in enumerate(chunks):
                logger.info(f"Processing chunk {chunk_idx + 1}/{len(chunks)} (pages {start_page}-{end_page})")
                
                # Create raw document
                raw_document = documentai.RawDocument(
                    content=chunk_bytes,
                    mime_type=mime_type
                )
                
                # Configure OCR options
                process_options = documentai.ProcessOptions(
                    ocr_config=documentai.OcrConfig(
                        enable_native_pdf_parsing=True,
                        enable_image_quality_scores=True,
                    )
                )
                
                # Build processing request
                request = documentai.ProcessRequest(
                    name=self.processor_name,
                    raw_document=raw_document,
                    process_options=process_options
                )
                
                # Process document
                response = self.client.process_document(request=request, timeout=DOC_AI_TIMEOUT_SECONDS)
                document = response.document
                
                logger.info(f"Chunk {chunk_idx + 1}: {len(document.pages)} pages, {len(document.text)} chars")
                
                # Extract structured data with correct page numbering
                chunk_pages = self._extract_pages_data(document, page_offset=start_page - 1)
                all_pages.extend(chunk_pages)
                full_text_parts.append(document.text)
            
            full_text = "\n\n".join(full_text_parts)
            
            logger.info(f"Document AI extracted {len(all_pages)} total pages, {len(full_text)} chars total")
            
            return {
                "success": True,
                "pages": all_pages,
                "full_text": full_text,
                "total_pages": len(all_pages),
                "total_chars": len(full_text)
            }
            
        except Exception as e:
            logger.error(f"Google Document AI error: {e}")
            return {
                "success": False,
                "error": str(e),
                "pages": [],
                "full_text": ""
            }
    
    def _extract_pages_data(self, document: documentai.Document, page_offset: int = 0) -> List[Dict[str, Any]]:
        """Extract structured data from each page."""
        pages_data = []
        
        for page_idx, page in enumerate(document.pages):
            # Calculate actual page number with offset
            actual_page_number = page_offset + page_idx + 1
            
            # Extract page text
            page_text = self._get_page_text(document, page)
            
            # Extract tables
            tables = self._extract_tables(document, page)
            
            # Extract form fields/key-value pairs
            form_fields = self._extract_form_fields(document, page)
            
            # Get confidence score
            confidence = float(page.layout.confidence) if page.layout and page.layout.confidence else 0.0
            
            # Get page dimensions
            width = float(page.dimension.width) if page.dimension else 0
            height = float(page.dimension.height) if page.dimension else 0
            
            page_data = {
                "page_number": actual_page_number,
                "text": page_text,
                "confidence": confidence,
                "tables": tables,
                "form_fields": form_fields,
                "width": width,
                "height": height,
                "char_count": len(page_text)
            }
            
            pages_data.append(page_data)
            logger.info(f"Page {actual_page_number}: {len(page_text)} chars, {len(tables)} tables, confidence: {confidence:.2%}")
        
        return pages_data
        
        return pages_data
    
    def _get_page_text(self, document: documentai.Document, page: documentai.Document.Page) -> str:
        """Extract complete text from a page preserving structure."""
        text_parts = []
        
        # Try to extract from paragraphs first (preserves structure)
        if page.paragraphs:
            for paragraph in page.paragraphs:
                if paragraph.layout and paragraph.layout.text_anchor:
                    para_text = self._get_text_from_anchor(document.text, paragraph.layout.text_anchor)
                    if para_text:
                        text_parts.append(para_text)
        
        # If no paragraphs, try lines
        if not text_parts and page.lines:
            for line in page.lines:
                if line.layout and line.layout.text_anchor:
                    line_text = self._get_text_from_anchor(document.text, line.layout.text_anchor)
                    if line_text:
                        text_parts.append(line_text)
        
        # Fallback to page layout text anchor
        if not text_parts and page.layout and page.layout.text_anchor:
            page_text = self._get_text_from_anchor(document.text, page.layout.text_anchor)
            if page_text:
                text_parts.append(page_text)
        
        return "\n".join(text_parts)
    
    def _extract_tables(self, document: documentai.Document, page: documentai.Document.Page) -> List[Dict[str, Any]]:
        """Extract tables from a page with structured row/column data."""
        tables = []
        
        for table in page.tables:
            table_data = {
                "header_rows": [],
                "body_rows": []
            }
            
            # Extract header rows
            for header_row in table.header_rows:
                row_cells = []
                for cell in header_row.cells:
                    cell_text = self._get_text_from_anchor(document.text, cell.layout.text_anchor) if cell.layout else ""
                    row_cells.append(cell_text.strip())
                if row_cells:
                    table_data["header_rows"].append(row_cells)
            
            # Extract body rows
            for body_row in table.body_rows:
                row_cells = []
                for cell in body_row.cells:
                    cell_text = self._get_text_from_anchor(document.text, cell.layout.text_anchor) if cell.layout else ""
                    row_cells.append(cell_text.strip())
                if row_cells:
                    table_data["body_rows"].append(row_cells)
            
            if table_data["header_rows"] or table_data["body_rows"]:
                tables.append(table_data)
        
        return tables
    
    def _extract_form_fields(self, document: documentai.Document, page: documentai.Document.Page) -> List[Dict[str, str]]:
        """Extract key-value pairs from form fields."""
        form_fields = []
        
        if hasattr(page, 'form_fields') and page.form_fields:
            for field in page.form_fields:
                field_name = ""
                field_value = ""
                
                if field.field_name and field.field_name.text_anchor:
                    field_name = self._get_text_from_anchor(document.text, field.field_name.text_anchor)
                
                if field.field_value and field.field_value.text_anchor:
                    field_value = self._get_text_from_anchor(document.text, field.field_value.text_anchor)
                
                if field_name or field_value:
                    form_fields.append({
                        "name": field_name.strip(),
                        "value": field_value.strip()
                    })
        
        return form_fields
    
    def _get_text_from_anchor(self, full_text: str, text_anchor: documentai.Document.TextAnchor) -> str:
        """Extract text using text anchor indices."""
        if not text_anchor or not text_anchor.text_segments:
            return ""
        
        text_parts = []
        for segment in text_anchor.text_segments:
            start_index = int(segment.start_index) if segment.start_index else 0
            end_index = int(segment.end_index) if segment.end_index else 0
            
            if end_index <= len(full_text):
                text_parts.append(full_text[start_index:end_index])
        
        return "".join(text_parts)


# Singleton instance
_extractor_instance: Optional[GoogleDocumentAIExtractor] = None

def get_document_ai_extractor() -> GoogleDocumentAIExtractor:
    """Get or create the Document AI extractor singleton."""
    global _extractor_instance
    if _extractor_instance is None:
        _extractor_instance = GoogleDocumentAIExtractor()
    return _extractor_instance


def extract_pdf_with_google_docai(file_content: bytes, mime_type: str = "application/pdf") -> Dict[str, Any]:
    """
    Convenience function to extract text from PDF using Google Document AI.
    
    Returns:
        {
            "success": bool,
            "pages": [{"page_number": int, "text": str, "tables": list, ...}],
            "full_text": str,
            "total_pages": int
        }
    """
    extractor = get_document_ai_extractor()
    return extractor.process_document(file_content, mime_type)
