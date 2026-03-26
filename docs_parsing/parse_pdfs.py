"""
docs_parsing/parse_pdfs.py
Python wrapper around LiteParse for healthcare PDF document parsing.

Provides:
- Batch directory processing
- Structured JSON output per page
- Bounding box field extraction
- Integration with the LLM QA assistant for natural language querying

Usage:
    python parse_pdfs.py --input ./sample_pdfs --output ./output
    python parse_pdfs.py --file sample_pdfs/eob.pdf --format json
    python parse_pdfs.py --file sample_pdfs/lab_result.pdf --screenshot
"""

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


# ============================================================
# Data Classes
# ============================================================

@dataclass
class PageResult:
    """Represents a single parsed page."""
    number: int
    text: str
    blocks: list = field(default_factory=list)
    screenshot_path: str | None = None


@dataclass
class ParseResult:
    """Represents a fully parsed document."""
    filename: str
    pages: list[PageResult] = field(default_factory=list)
    total_pages: int = 0
    total_chars: int = 0
    extracted_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> dict:
        return {
            "filename": self.filename,
            "total_pages": self.total_pages,
            "total_chars": self.total_chars,
            "extracted_at": self.extracted_at,
            "pages": [
                {
                    "page_number": p.number,
                    "text": p.text,
                    "block_count": len(p.blocks),
                    "blocks": p.blocks,
                    "screenshot": p.screenshot_path,
                }
                for p in self.pages
            ],
        }


# ============================================================
# LiteParse Wrapper
# ============================================================

class LiteParseRunner:
    """Runs lit CLI commands."""

    def __init__(self, lit_path: str = "lit"):
        self.lit = lit_path

    def _run(self, args: list[str], capture_output: bool = True) -> subprocess.CompletedProcess:
        cmd = [self.lit] + args
        return subprocess.run(
            cmd,
            capture_output=capture_output,
            text=True,
            check=False,
        )

    def parse(
        self,
        pdf_path: str,
        output_format: str = "json",
        output_path: str | None = None,
        ocr: bool = True,
        pages: str | None = None,
    ) -> tuple[str, int]:
        """
        Parse a PDF and return (output_content, return_code).
        If output_path specified, writes there instead of returning content.
        """
        args = ["parse", pdf_path, "--format", output_format]
        if not ocr:
            args.append("--no-ocr")
        if pages:
            args.extend(["--target-pages", pages])
        if output_path:
            args.extend(["-o", output_path])
            result = self._run(args)
            return "", result.returncode
        else:
            args.append("--format-json-stdout") if output_format == "json" else None
            result = self._run(args)
            return result.stdout, result.returncode

    def screenshot(
        self,
        pdf_path: str,
        output_dir: str,
        pages: str | None = None,
        dpi: int = 150,
    ) -> list[str]:
        """Generate screenshots for PDF pages."""
        args = ["screenshot", pdf_path, "-o", output_dir, "--dpi", str(dpi)]
        if pages:
            args.extend(["--target-pages", pages])
        result = self._run(args)
        if result.returncode != 0:
            raise RuntimeError(f"Screenshot failed: {result.stderr}")
        # Find generated files
        return sorted(Path(output_dir).glob(f"{Path(pdf_path).stem}*.png"))

    def batch_parse(
        self,
        input_dir: str,
        output_dir: str,
        output_format: str = "json",
        ocr: bool = True,
    ) -> dict:
        """Batch parse all PDFs in a directory."""
        input_path = Path(input_dir)
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        results = {}
        pdfs = list(input_path.glob("*.pdf")) + list(input_path.glob("*.PDF"))

        if not pdfs:
            print(f"No PDFs found in {input_dir}")
            return results

        for pdf in sorted(pdfs):
            print(f"Parsing: {pdf.name}")
            out_file = output_path / f"{pdf.stem}.json"
            _, code = self.parse(
                str(pdf),
                output_format=output_format,
                output_path=str(out_file),
                ocr=ocr,
            )
            results[pdf.name] = {
                "status": "success" if code == 0 else "failed",
                "output": str(out_file),
            }
            if code == 0:
                with open(out_file) as f:
                    data = json.load(f)
                    results[pdf.name]["stats"] = {
                        "pages": data.get("total_pages", 0),
                        "chars": sum(len(p.get("text", "")) for p in data.get("pages", [])),
                    }

        return results


# ============================================================
# Healthcare PDF Parser
# ============================================================

class HealthcarePDFParser:
    """
    Wrapper around LiteParse with healthcare-specific field extraction.
    """

    def __init__(self, ocr_enabled: bool = True, lit_path: str = "lit"):
        self.lit = LiteParseRunner(lit_path=lit_path)
        self.ocr = ocr_enabled

    def parse(self, pdf_path: str, pages: str | None = None) -> ParseResult:
        """Parse a PDF and return structured result."""
        if not os.path.exists(pdf_path):
            raise FileNotFoundError(f"PDF not found: {pdf_path}")

        # Parse to temp JSON
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            temp_path = f.name

        try:
            _, code = self.lit.parse(
                pdf_path,
                output_format="json",
                output_path=temp_path,
                ocr=self.ocr,
                pages=pages,
            )

            if code != 0:
                raise RuntimeError(f"LiteParse failed on {pdf_path}")

            with open(temp_path) as f:
                data = json.load(f)

        finally:
            os.unlink(temp_path)

        result = ParseResult(filename=os.path.basename(pdf_path))

        for page_data in data.get("pages", []):
            page = PageResult(
                number=page_data.get("page_number", 0),
                text=page_data.get("text", ""),
                blocks=page_data.get("blocks", []),
            )
            result.pages.append(page)

        result.total_pages = len(result.pages)
        result.total_chars = sum(len(p.text) for p in result.pages)

        return result

    def screenshot(self, pdf_path: str, output_dir: str, pages: str | None = None) -> list[str]:
        """Generate page screenshots."""
        os.makedirs(output_dir, exist_ok=True)
        return self.lit.screenshot(pdf_path, output_dir, pages=pages)

    def extract_field(
        self,
        pdf_path: str,
        bbox: dict,
        page: int = 1,
    ) -> str:
        """
        Extract text from a specific bounding box region on a page.
        bbox: {"x": float, "y": float, "width": float, "height": float}
        Coordinates are in PDF points (72 dpi equivalent).
        """
        # Parse the page
        result = self.parse(pdf_path, pages=str(page))
        if page > len(result.pages):
            return ""

        target_page = result.pages[0]

        # Filter blocks that fall within the bounding box
        matched_blocks = []
        for block in target_page.blocks:
            block_bbox = block.get("bbox", {})
            bx0 = block_bbox.get("x0", 0)
            by0 = block_bbox.get("y0", 0)
            bx1 = block_bbox.get("x1", 0)
            by1 = block_bbox.get("y1", 0)

            # Check overlap
            if (bx0 < bbox["x"] + bbox["width"]
                and bx1 > bbox["x"]
                and by0 < bbox["y"] + bbox["height"]
                and by1 > bbox["y"]):
                matched_blocks.append(block.get("text", ""))

        return " ".join(matched_blocks).strip()

    def to_llm_prompt(self, pdf_path: str) -> str:
        """Generate a formatted text block for the LLM QA assistant."""
        result = self.parse(pdf_path)
        lines = [f"# Document: {result.filename}"]
        lines.append(f"Parsed: {result.extracted_at} | Pages: {result.total_pages} | Chars: {result.total_chars}")
        lines.append("")
        for page in result.pages:
            lines.append(f"## Page {page.number}")
            lines.append(page.text)
            lines.append("")
        return "\n".join(lines)


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Healthcare PDF parsing with LiteParse")
    parser.add_argument("--file", help="Single PDF file to parse")
    parser.add_argument("--input", help="Input directory for batch parsing")
    parser.add_argument("--output", help="Output directory (default: ./output)")
    parser.add_argument("--format", default="json", choices=["json", "text"], help="Output format")
    parser.add_argument("--no-ocr", action="store_true", help="Disable OCR")
    parser.add_argument("--screenshot", action="store_true", help="Also generate screenshots")
    parser.add_argument("--pages", help="Page range, e.g. '1-5,10,15-20'")
    parser.add_argument("--dpi", type=int, default=150, help="Screenshot DPI")
    parser.add_argument("--to-llm", action="store_true", help="Output formatted for LLM QA assistant")

    args = parser.parse_args()
    out_dir = args.output or "./output"
    os.makedirs(out_dir, exist_ok=True)

    lit = LiteParseRunner()

    if args.file:
        pdf_parser = HealthcarePDFParser(ocr_enabled=not args.no_ocr)

        if args.to_llm:
            prompt = pdf_parser.to_llm_prompt(args.file)
            out_file = Path(out_dir) / f"{Path(args.file).stem}_llm.txt"
            with open(out_file, "w") as f:
                f.write(prompt)
            print(f"LLM prompt saved: {out_file}")
            return

        result = pdf_parser.parse(args.file, pages=args.pages)

        if args.screenshot:
            ss_dir = Path(out_dir) / "screenshots"
            ss_files = pdf_parser.screenshot(args.file, str(ss_dir), pages=args.pages)
            print(f"Screenshots: {[str(f) for f in ss_files]}")

        out_file = Path(out_dir) / f"{Path(args.file).stem}.json"
        with open(out_file, "w") as f:
            json.dump(result.to_dict(), f, indent=2)
        print(f"Parsed: {result.total_pages} pages, {result.total_chars} chars → {out_file}")

    elif args.input:
        results = lit.batch_parse(
            args.input,
            out_dir,
            output_format=args.format,
            ocr=not args.no_ocr,
        )
        print(f"\nBatch complete: {len(results)} files")
        for name, info in results.items():
            status = "✅" if info["status"] == "success" else "❌"
            stats = info.get("stats", {})
            print(f"  {status} {name}: {stats.get('pages', '?')} pages, {stats.get('chars', '?')} chars")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
