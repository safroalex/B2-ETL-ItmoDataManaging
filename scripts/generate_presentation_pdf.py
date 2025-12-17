"""Generate a simple PDF presentation from presentation/presentation.md.

The rubric requires a presentation artifact (PDF/PPT/etc.) committed to the repo.
This script produces a minimal PDF with one markdown line per paragraph.

Usage:
  python scripts/generate_presentation_pdf.py

Output:
  presentation/presentation.pdf
"""

from __future__ import annotations

from pathlib import Path

from fpdf import FPDF

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "presentation" / "presentation.md"
OUT = ROOT / "presentation" / "presentation.pdf"


def _iter_paragraphs(markdown_text: str) -> list[str]:
    lines = [line.rstrip() for line in markdown_text.splitlines()]
    paragraphs: list[str] = []
    buf: list[str] = []

    for line in lines:
        if not line.strip():
            if buf:
                paragraphs.append(" ".join(buf).strip())
                buf = []
            continue
        buf.append(line)

    if buf:
        paragraphs.append(" ".join(buf).strip())

    return paragraphs


def main() -> None:
    if not SRC.exists():
        raise SystemExit(f"Missing source file: {SRC}")

    text = SRC.read_text(encoding="utf-8")
    paragraphs = _iter_paragraphs(text)

    pdf = FPDF(format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # Use core PDF font to avoid bundling font files.
    pdf.set_font("Helvetica", size=16)
    pdf.multi_cell(0, 10, "Weather ELT project — presentation")
    pdf.ln(2)

    pdf.set_font("Helvetica", size=11)
    for p in paragraphs:
        if p.startswith("#"):
            # crude markdown heading handling
            title = p.lstrip("#").strip()
            pdf.set_font("Helvetica", style="B", size=13)
            pdf.multi_cell(0, 8, title)
            pdf.set_font("Helvetica", size=11)
            continue

        pdf.multi_cell(0, 6, p)
        pdf.ln(1)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    pdf.output(str(OUT))
    print(f"Wrote: {OUT}")


if __name__ == "__main__":
    main()
