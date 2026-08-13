#!/usr/bin/env python3
"""Generate Wapahki's one-page automation-fit screen."""

from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "output" / "pdf" / "wapahki-automation-fit-screen.pdf"

NAVY = colors.HexColor("#183247")
GREEN = colors.HexColor("#18A07A")
PALE = colors.HexColor("#EEF5F3")
INK = colors.HexColor("#1F2933")
MUTED = colors.HexColor("#61717E")
LINE = colors.HexColor("#CBD8DC")
AMBER = colors.HexColor("#F6C85F")


def text(c, x, y, value, size=8.5, color=INK, font="Helvetica"):
    c.setFillColor(color)
    c.setFont(font, size)
    c.drawString(x, y, value)


def wrapped(c, x, y, value, width, size=8.2, leading=10.2, color=INK, font="Helvetica"):
    words = value.split()
    lines = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        if stringWidth(candidate, font, size) <= width:
            current = candidate
        else:
            if current:
                lines.append(current)
            current = word
    if current:
        lines.append(current)
    for line in lines:
        text(c, x, y, line, size, color, font)
        y -= leading
    return y


def line_field(c, x, y, label, width):
    text(c, x, y, label.upper(), 6.7, MUTED, "Helvetica-Bold")
    c.setStrokeColor(LINE)
    c.setLineWidth(0.7)
    c.line(x, y - 8, x + width, y - 8)


def section(c, x, y, w, h, number, title, prompt, checks):
    c.setFillColor(colors.white)
    c.setStrokeColor(LINE)
    c.roundRect(x, y - h, w, h, 7, fill=1, stroke=1)
    c.setFillColor(GREEN)
    c.circle(x + 17, y - 18, 9, fill=1, stroke=0)
    text(c, x + 14.5, y - 21.5, str(number), 8, colors.white, "Helvetica-Bold")
    text(c, x + 32, y - 17, title, 9.5, NAVY, "Helvetica-Bold")
    wrapped(c, x + 12, y - 35, prompt, w - 24, 7.3, 8.8, MUTED)
    cursor = y - 57
    for item in checks:
        c.setStrokeColor(GREEN)
        c.rect(x + 13, cursor - 2, 7, 7, fill=0, stroke=1)
        text(c, x + 25, cursor - 1, item, 7.5, INK)
        cursor -= 14


def build():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(OUTPUT), pagesize=letter)
    width, height = letter

    c.setFillColor(NAVY)
    c.rect(0, height - 104, width, 104, fill=1, stroke=0)
    text(c, 38, height - 39, "WAPAHKI", 10, GREEN, "Helvetica-Bold")
    text(c, 38, height - 63, "Automation-fit screen", 22, colors.white, "Helvetica-Bold")
    text(c, 38, height - 82, "A first-pass way to rule out weak robotic-cell candidates before a site visit.", 9, colors.white)
    c.setFillColor(AMBER)
    c.roundRect(width - 174, height - 77, 136, 28, 6, fill=1, stroke=0)
    text(c, width - 162, height - 66, "RESEARCH AID - NOT", 7.5, NAVY, "Helvetica-Bold")
    text(c, width - 162, height - 75, "AN ENGINEERING ASSESSMENT", 7.5, NAVY, "Helvetica-Bold")

    top = height - 124
    line_field(c, 38, top, "Company / facility", 252)
    line_field(c, 312, top, "Candidate physical task", 262)
    line_field(c, 38, top - 31, "Facility ID", 116)
    line_field(c, 171, top - 31, "Task claim ID", 119)
    line_field(c, 312, top - 31, "Economic claim ID", 122)
    line_field(c, 451, top - 31, "Contact-facility evidence ID", 123)

    grid_top = top - 61
    gap = 10
    card_w = (width - 76 - gap) / 2
    card_h = 105
    section(c, 38, grid_top, card_w, card_h, 1, "Motion and repeatability",
            "Describe one object, start point, end point, and normal cycle. Keep variants separate.",
            ["Motion repeats within a run", "Object presentation is observable", "Cycle timing can be measured"])
    section(c, 38 + card_w + gap, grid_top, card_w, card_h, 2, "Changeover and variation",
            "List what changes between runs and whether the cell can adapt without a mechanical rebuild.",
            ["Variant set is limited", "Changeover boundary is known", "No invented product or station detail"])

    grid_top -= card_h + gap
    section(c, 38, grid_top, card_w, card_h, 3, "Intervention and exceptions",
            "Name misfeeds, reorientation, quality checks, or recovery work and how often each occurs.",
            ["Normal cases dominate", "Intervention frequency is bounded", "People retain true exceptions"])
    section(c, 38 + card_w + gap, grid_top, card_w, card_h, 4, "Safety and integration",
            "Record guarding, lifting, sanitation, damage risk, controls, space, and upstream/downstream limits.",
            ["Safety constraint is explicit", "Integration owner is identified", "A stop condition is stated"])

    grid_top -= card_h + gap
    section(c, 38, grid_top, card_w, card_h, 5, "Rate and operating consequence",
            "Use sourced or operator-confirmed rate, staffing, stoppage, cycle-time, safety, or changeover impact.",
            ["Consequence has a claim ID", "Unknown values stay unknown", "Manual work alone is not economics"])
    section(c, 38 + card_w + gap, grid_top, card_w, card_h, 6, "Payback and first-pass result",
            "Compare the smallest useful task coverage with integration cost and remaining operator work.",
            ["Rule out", "Investigate after one missing fact", "Candidate for bounded evaluation"])

    result_y = grid_top - card_h - 18
    c.setFillColor(PALE)
    c.roundRect(38, result_y - 92, width - 76, 92, 7, fill=1, stroke=0)
    text(c, 50, result_y - 15, "FIRST-PASS OUTPUT", 7, GREEN, "Helvetica-Bold")
    text(c, 50, result_y - 31, "Decision:", 8, NAVY, "Helvetica-Bold")
    text(c, 102, result_y - 31, "[  ] rule out    [  ] one fact missing    [  ] bounded evaluation candidate", 8, INK)
    text(c, 50, result_y - 45, "Reason / next evidence:", 8, NAVY, "Helvetica-Bold")
    c.setStrokeColor(LINE)
    c.line(159, result_y - 47, width - 50, result_y - 47)
    text(c, 50, result_y - 62, "Public evidence used:", 8, NAVY, "Helvetica-Bold")
    c.line(155, result_y - 64, width - 50, result_y - 64)
    text(c, 50, result_y - 79, "Completed by / date:", 8, NAVY, "Helvetica-Bold")
    c.line(147, result_y - 81, width - 50, result_y - 81)

    footer_y = 25
    text(c, 38, footer_y + 12, "Evidence rule", 7.5, NAVY, "Helvetica-Bold")
    wrapped(c, 99, footer_y + 12,
            "Every factual statement must map to an active source claim. Unknowns remain blank or become the single question.",
            360, 7.3, 8.5, MUTED)
    text(c, width - 107, footer_y + 7, "wapahki.ca", 7.5, GREEN, "Helvetica-Bold")

    c.setTitle("Wapahki Automation-fit Screen")
    c.setAuthor("Wapahki Industries")
    c.showPage()
    c.save()
    print(OUTPUT)


if __name__ == "__main__":
    build()
