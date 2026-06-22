from __future__ import annotations

from pathlib import Path
import re
import textwrap


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "docs" / "SOG_BIBLE.md"
OUTPUT = ROOT / "The_SOG_Bible_v2.pdf"

PAGE_WIDTH = 612.0
PAGE_HEIGHT = 792.0
MARGIN_X = 54.0
MARGIN_TOP = 58.0
MARGIN_BOTTOM = 54.0
CONTENT_WIDTH = PAGE_WIDTH - (MARGIN_X * 2)


def _pdf_escape(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace("(", "\\(")
        .replace(")", "\\)")
        .replace("\r", "")
    )


def _ascii(value: str) -> str:
    replacements = {
        "\u2013": "-",
        "\u2014": "-",
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u2026": "...",
        "\u2264": "<=",
        "\u2265": ">=",
        "\u2192": "->",
    }
    for old, new in replacements.items():
        value = value.replace(old, new)
    return value.encode("latin-1", errors="replace").decode("latin-1")


def _clean_inline(value: str) -> str:
    value = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", value)
    for marker in ("**", "__", "`"):
        value = value.replace(marker, "")
    return _ascii(value)


def _wrap(value: str, size: float, indent_chars: int = 0, code: bool = False) -> list[str]:
    approx_char_width = size * (0.6 if code else 0.52)
    max_chars = max(18, int(CONTENT_WIDTH / approx_char_width) - indent_chars)
    if not value:
        return [""]
    if code:
        chunks: list[str] = []
        remaining = value
        while len(remaining) > max_chars:
            chunks.append(remaining[:max_chars])
            remaining = "  " + remaining[max_chars:]
        chunks.append(remaining)
        return chunks
    return textwrap.wrap(
        value,
        width=max_chars,
        break_long_words=False,
        break_on_hyphens=False,
    ) or [""]


class PdfPage:
    def __init__(self) -> None:
        self.y = PAGE_HEIGHT - MARGIN_TOP
        self.ops: list[str] = []

    def add_text(self, x: float, y: float, text: str, font: str, size: float) -> None:
        escaped = _pdf_escape(text)
        self.ops.append(f"BT /{font} {size:.2f} Tf 1 0 0 1 {x:.2f} {y:.2f} Tm ({escaped}) Tj ET")


class MarkdownPdf:
    def __init__(self) -> None:
        self.pages: list[PdfPage] = [PdfPage()]

    @property
    def page(self) -> PdfPage:
        return self.pages[-1]

    def new_page(self) -> None:
        self.pages.append(PdfPage())

    def ensure(self, height: float) -> None:
        if self.page.y - height < MARGIN_BOTTOM:
            self.new_page()

    def add_gap(self, amount: float) -> None:
        self.ensure(amount)
        self.page.y -= amount

    def add_paragraph(
        self,
        text: str,
        *,
        font: str = "F1",
        size: float = 10.0,
        indent: float = 0.0,
        leading: float | None = None,
        code: bool = False,
    ) -> None:
        text = _ascii(text) if code else _clean_inline(text)
        lines = _wrap(text, size=size, indent_chars=int(indent / 5), code=code)
        line_height = leading or (size * 1.35)
        for line in lines:
            self.ensure(line_height)
            self.page.add_text(MARGIN_X + indent, self.page.y, line, font, size)
            self.page.y -= line_height

    def render_markdown(self, markdown: str) -> None:
        in_code = False
        for raw_line in markdown.splitlines():
            line = raw_line.rstrip()
            stripped = line.strip()

            if stripped.startswith("```"):
                in_code = not in_code
                self.add_gap(5)
                continue

            if in_code:
                self.add_paragraph(line, font="F3", size=8.2, leading=10.4, code=True)
                continue

            if not stripped:
                self.add_gap(7)
                continue

            if stripped == "---":
                self.add_gap(10)
                continue

            if stripped.startswith("# "):
                if len(self.pages) > 1 or self.page.y < PAGE_HEIGHT - MARGIN_TOP - 10:
                    self.new_page()
                self.add_paragraph(stripped[2:], font="F2", size=22, leading=28)
                self.add_gap(8)
                continue

            if stripped.startswith("## "):
                self.ensure(42)
                self.add_gap(8)
                self.add_paragraph(stripped[3:], font="F2", size=15.5, leading=20)
                self.add_gap(2)
                continue

            if stripped.startswith("### "):
                self.ensure(32)
                self.add_gap(6)
                self.add_paragraph(stripped[4:], font="F2", size=12.2, leading=16)
                continue

            if stripped.startswith("|"):
                self.add_paragraph(stripped, font="F3", size=7.2, leading=9.3, code=True)
                continue

            bullet = re.match(r"^(\s*)-\s+(.*)$", line)
            if bullet:
                body = "- " + bullet.group(2)
                self.add_paragraph(body, size=9.6, indent=10, leading=12.5)
                continue

            numbered = re.match(r"^(\d+)\.\s+(.*)$", stripped)
            if numbered:
                self.add_paragraph(f"{numbered.group(1)}. {numbered.group(2)}", size=9.6, indent=10, leading=12.5)
                continue

            self.add_paragraph(stripped, size=10, leading=13.5)

    def finish(self) -> bytes:
        for index, page in enumerate(self.pages, start=1):
            footer = f"The SOG Bible - current edition - page {index} of {len(self.pages)}"
            page.add_text(MARGIN_X, 30.0, footer, "F1", 8.0)
        return _write_pdf(self.pages)


def _write_pdf(pages: list[PdfPage]) -> bytes:
    objects: list[bytes] = []

    def add_object(data: str | bytes) -> int:
        if isinstance(data, str):
            data = data.encode("latin-1")
        objects.append(data)
        return len(objects)

    catalog_id = add_object("<< /Type /Catalog /Pages 2 0 R >>")
    pages_id = add_object(b"")
    font_regular = add_object("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    font_bold = add_object("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>")
    font_mono = add_object("<< /Type /Font /Subtype /Type1 /BaseFont /Courier >>")

    page_ids: list[int] = []
    for page in pages:
        stream = "\n".join(page.ops).encode("latin-1", errors="replace")
        content_id = add_object(
            b"<< /Length " + str(len(stream)).encode("ascii") + b" >>\nstream\n" + stream + b"\nendstream"
        )
        page_id = add_object(
            "<< /Type /Page "
            f"/Parent {pages_id} 0 R "
            f"/MediaBox [0 0 {PAGE_WIDTH:.0f} {PAGE_HEIGHT:.0f}] "
            f"/Resources << /Font << /F1 {font_regular} 0 R /F2 {font_bold} 0 R /F3 {font_mono} 0 R >> >> "
            f"/Contents {content_id} 0 R >>"
        )
        page_ids.append(page_id)

    kids = " ".join(f"{page_id} 0 R" for page_id in page_ids)
    objects[pages_id - 1] = f"<< /Type /Pages /Kids [{kids}] /Count {len(page_ids)} >>".encode("latin-1")

    header = b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n"
    body = bytearray(header)
    offsets = [0]
    for index, data in enumerate(objects, start=1):
        offsets.append(len(body))
        body.extend(f"{index} 0 obj\n".encode("ascii"))
        body.extend(data)
        body.extend(b"\nendobj\n")

    xref_offset = len(body)
    body.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    body.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        body.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    body.extend(
        (
            "trailer\n"
            f"<< /Size {len(objects) + 1} /Root {catalog_id} 0 R >>\n"
            "startxref\n"
            f"{xref_offset}\n"
            "%%EOF\n"
        ).encode("ascii")
    )
    return bytes(body)


def main() -> None:
    markdown = SOURCE.read_text(encoding="utf-8")
    pdf = MarkdownPdf()
    pdf.render_markdown(markdown)
    OUTPUT.write_bytes(pdf.finish())
    print(f"Wrote {OUTPUT} ({len(pdf.pages)} pages)")


if __name__ == "__main__":
    main()
