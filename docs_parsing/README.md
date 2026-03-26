# PDF Document Parsing Pipeline

## Overview

Healthcare data ops teams regularly work with **PDF documents**: explanation of benefits (EOB), lab results, clinical reports, hospital discharge summaries, claims attachments. LiteParse provides fast, local PDF parsing with bounding boxes and OCR — no cloud dependency, no API costs.

**This pipeline demonstrates:**
- Extracting structured text from healthcare PDFs using LiteParse
- Bounding box extraction for field-level parsing (e.g., member ID, date of service, amount)
- Batch processing a directory of PDFs
- Feeding parsed text into the LLM QA assistant for natural language querying
- Screenshot generation for visual verification

## Architecture

```
PDF Documents (EOB, Lab Results, Claims)
    ↓
LiteParse (local, no cloud)
    ↓
Structured Text + Bounding Boxes
    ↓
Parsed Output (JSON / Text)
    ↓
LLM QA Assistant (MiniMax)
    ↓
Natural Language Q&A
```

## Quick Start

```bash
# Install LiteParse
npm install -g @llamaindex/liteparse

# Parse a single PDF
lit parse sample_pdfs/eob_sample.pdf --format json -o output/eob_sample.json

# Screenshot a page (for visual verification)
lit screenshot sample_pdfs/eob_sample.pdf --target-pages "1" -o output/screenshots/

# Batch parse a directory
lit batch-parse ./sample_pdfs ./output/

# Use the Python wrapper
python3 parse_pdfs.py --input ./sample_pdfs --output ./output
```

## Python Wrapper

`parse_pdfs.py` provides a Python wrapper around LiteParse with:
- Batch directory processing
- Field extraction using bounding box coordinates
- JSON output with page-level structure
- Integration with the LLM QA assistant

```python
from parse_pdfs import HealthcarePDFParser

parser = HealthcarePDFParser(ocr_enabled=True)
result = parser.parse("sample_pdfs/eob_sample.pdf")

# Access extracted text
for page in result.pages:
    print(f"Page {page.number}: {len(page.text)} chars")
    print(f"Bounding boxes: {len(page.blocks)} blocks")

# Extract specific fields by bounding box region
member_id = result.extract_field(page=1, bbox={"x": 50, "y": 100, "width": 200, "height": 30})
```

## Use Cases in Healthcare Data Ops

| Document Type | What to Extract | Downstream |
|---|---|---|
| Explanation of Benefits | Member ID, Date of Service, Amount, Provider | Claims pipeline |
| Lab Results | Patient ID, Test Name, Result Value, Reference Range | Clinical analytics |
| Hospital Discharge Summary | Patient ID, Admission Date, Diagnosis Codes | Care quality metrics |
| Claims Attachment | Claim ID, Service Date, CPT Codes, Charges | Claims adjudication |
| Prior Auth Request | Member ID, Diagnosis, Requested Service | Utilization management |

## LiteParse vs LlamaParse

| Feature | LiteParse (this) | LlamaParse (cloud) |
|---|---|---|
| Cost | Free (local) | Free tier + paid |
| Speed | Fast (local CPU) | Fast (cloud) |
| Complex documents | Good | Best (multimodal LLM) |
| OCR | Built-in Tesseract.js | Built-in |
| No internet required | ✅ | ❌ |
| HIPAA compliance | ✅ (data never leaves machine) | ⚠️ (data to cloud) |

LiteParse is the right choice for PHI/HIPAA-sensitive healthcare documents.
