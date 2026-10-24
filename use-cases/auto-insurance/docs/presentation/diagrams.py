"""
Diagrams for the auto-insurance deck, drawn in the same visual style as the
original deck (navy boxes, coloured borders, white bold text).

  python docs/presentation/diagrams.py      # writes PNGs to docs/presentation/images/
"""

import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyBboxPatch  # noqa: E402

NAVY, TEAL, ORANGE, BLUE, RED, GREY, PURPLE = (
    "#2A2A4A", "#00D4AA", "#FF8C00", "#4488FF", "#FF4444", "#BBBBCC", "#9B59B6")

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "images")


def _canvas(w, h):
    fig = plt.figure(figsize=(w / 100, h / 100), dpi=100)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, w)
    ax.set_ylim(h, 0)
    ax.axis("off")
    return fig, ax


def _box(ax, x, y, w, h, text, edge, size=15):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0,rounding_size=14",
                                facecolor=NAVY, edgecolor=edge, linewidth=4, zorder=3))
    ax.text(x + w / 2, y + h / 2, text, color="white", fontsize=size, fontweight="bold",
            ha="center", va="center", zorder=4, linespacing=1.15)


def _arrow(ax, x1, y1, x2, y2, color, lw=3.5, style="-|>"):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1), zorder=2,
                arrowprops=dict(arrowstyle=style, color=color, lw=lw, mutation_scale=22,
                                shrinkA=0, shrinkB=0))


def _label(ax, x, y, text, color, size=13, **kw):
    ax.text(x, y, text, color=color, fontsize=size, ha=kw.pop("ha", "center"), va=kw.pop("va", "center"),
            zorder=5, **kw)


def _save(fig, name):
    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, name)
    fig.savefig(path, dpi=100, facecolor="white")
    plt.close(fig)
    return path


def architecture():
    fig, ax = _canvas(1300, 730)
    _label(ax, 145, 40, "Insurance Event Sources", GREY, 15, style="italic")
    _label(ax, 470, 40, "Message Bus", ORANGE, 15, style="italic")
    _label(ax, 1172, 40, "AI Ops Agent", TEAL, 15, style="italic")

    for i, t in enumerate(["Telematics\n(connected cars)", "FNOL\n(app · IVR · auto)", "Claims · Fraud\n· Payments"]):
        _box(ax, 30, 68 + i * 82, 230, 66, t, GREY, 13)
        _arrow(ax, 262, 101 + i * 82, 330, 140, ORANGE, 2.5)
    _box(ax, 330, 90, 290, 100, "Apache Kafka\nTopic: insurance-events", ORANGE, 15)

    ax.add_patch(FancyBboxPatch((20, 330), 1010, 210, boxstyle="round,pad=0,rounding_size=14",
                                facecolor="none", edgecolor=TEAL, linewidth=2.5, linestyle=(0, (6, 4)), zorder=1))
    _label(ax, 520, 362, "Streaming MCP Server", TEAL, 19, fontweight="bold")
    _box(ax, 40, 400, 220, 100, "Kafka\nConsumer", ORANGE)
    _box(ax, 290, 400, 220, 100, "Anomaly\nDetection", BLUE)
    _box(ax, 540, 400, 220, 100, "WebSocket\nServer :8765", TEAL)
    _box(ax, 790, 400, 220, 100, "Cedar Policy\nEngine", RED)
    _arrow(ax, 470, 192, 150, 398, ORANGE)
    _label(ax, 395, 300, "consume", ORANGE, 13)
    for x in (262, 512, 762):
        _arrow(ax, x, 450, x + 26, 450, GREY, 3)

    _box(ax, 1060, 70, 225, 80, "Subscribe to\nlive stream", TEAL, 14)
    _box(ax, 1060, 195, 225, 80, "Sliding-window\ndetector", BLUE, 14)
    _box(ax, 1060, 320, 225, 100, "LLM reasoning\nBedrock · OpenAI\n· Ollama", PURPLE, 14)
    _box(ax, 1060, 470, 225, 100, "Diagnosis\n+ impact + owner\n+ next steps", TEAL, 14)
    _box(ax, 790, 600, 220, 72, "Insurer runbooks\n(search_runbooks)", GREY, 13)

    _arrow(ax, 700, 398, 1058, 110, TEAL)
    _label(ax, 800, 215, "push: live\nclaims events", TEAL, 13)
    _arrow(ax, 1172, 152, 1172, 193, GREY, 3)
    _arrow(ax, 1172, 277, 1172, 318, GREY, 3)
    _label(ax, 1240, 297, "spike", GREY, 12)
    _arrow(ax, 1172, 422, 1172, 468, GREY, 3)
    _arrow(ax, 1058, 395, 1012, 440, BLUE)
    _label(ax, 650, 568, "tool calls (Cedar-checked, PII-redacted): get_anomalies() · get_context()", BLUE, 12.5)
    _arrow(ax, 1012, 636, 1060, 412, GREY, 2.5, "<|-|>")

    _label(ax, 45, 690, "push (streaming)", TEAL, 15, ha="left")
    _label(ax, 240, 690, "pull (tool calls)", BLUE, 15, ha="left")
    _label(ax, 425, 690, "Kafka", ORANGE, 15, ha="left")
    _label(ax, 510, 690, "LLM", PURPLE, 15, ha="left")
    return _save(fig, "architecture.png")


def timeline():
    fig, ax = _canvas(1152, 470)
    ax.plot([100, 1060], [250, 250], color=GREY, lw=4, zorder=1)
    pts = [(180, ORANGE, "2:03 pm", "Hail hits DFW"), (400, RED, "2:10 pm", "FNOL volume 14×"),
           (960, TEAL, "9:00 am +1 day", "Batch ops report")]
    for x, c, t1, t2 in pts:
        ax.scatter([x], [250], s=420, color=c, zorder=3)
        _label(ax, x, 300, t1, c, 16, fontweight="bold")
        _label(ax, x, 335, t2, "#55556A", 14)
    _arrow(ax, 410, 175, 950, 175, RED, 3.5, "<|-|>")
    _label(ax, 680, 125, "19-hour blind spot", RED, 22, fontweight="bold")
    _label(ax, 576, 420, "Streaming MCP: the agent sees the surge at 2:03 pm", TEAL, 17, fontweight="bold")
    return _save(fig, "timeline.png")


def sequence():
    fig, ax = _canvas(900, 560)
    _label(ax, 250, 35, "AI Agent", TEAL, 22, fontweight="bold")
    _label(ax, 650, 35, "MCP Server", TEAL, 22, fontweight="bold")
    for x in (250, 650):
        ax.plot([x, x], [65, 540], color=TEAL, lw=4, linestyle=(0, (4, 2.5)))
    rows = [
        ("subscribe(insurance-events)", TEAL, "r"),
        ("event: FNOL CLM-#### created", ORANGE, "l"),
        ("event: payout batch rejected!", RED, "l"),
        ("get_anomalies()", BLUE, "r"),
        ("anomaly summary (PII-redacted)", BLUE, "l"),
        ("get_context(level=ERROR)", BLUE, "r"),
        ("unsubscribe", GREY, "r"),
    ]
    for i, (text, c, d) in enumerate(rows):
        y = 100 + i * 64
        _label(ax, 450, y - 22, text, c, 16)
        if d == "r":
            _arrow(ax, 270, y, 630, y, c)
        else:
            _arrow(ax, 630, y, 270, y, c)
    return _save(fig, "sequence.png")


def pipeline():
    fig, ax = _canvas(1300, 460)
    steps = [("Claims &\nTelematics", GREY), ("Kafka", ORANGE), ("MCP\nServer", TEAL),
             ("AI Agent\n+ LLM", PURPLE), ("Diagnosis\n& Owner", RED)]
    w, gap, x0 = 210, 50, 25
    for i, (t, c) in enumerate(steps):
        x = x0 + i * (w + gap)
        _box(ax, x, 160, w, 135, t, c, 19)
        if i < len(steps) - 1:
            _arrow(ax, x + w + 2, 227, x + w + gap - 2, 227, TEAL)
    _label(ax, 650, 360, "Detection < 1 s  ·  AI diagnosis in seconds", TEAL, 21, fontweight="bold")
    _label(ax, 650, 90, "FNOL · telematics · coverage · fraud · payouts", GREY, 16, style="italic")
    return _save(fig, "pipeline.png")


def hub():
    fig, ax = _canvas(1100, 560)
    _box(ax, 390, 225, 320, 110, "Streaming\nMCP + LLM", TEAL, 22)
    nodes = [(30, 40, "FNOL\nAutomation", TEAL), (420, 30, "Fraud / SIU", RED), (800, 40, "CAT Response", ORANGE),
             (30, 420, "UBI\nTelematics", BLUE), (420, 440, "Claims\nPayouts", PURPLE), (800, 420, "Platform SRE", GREY)]
    for x, y, t, c in nodes:
        _box(ax, x, y, 260, 95, t, c, 17)
        cx, cy = x + 130, y + (95 if y < 200 else 0)
        _arrow(ax, 550, 280 + (-55 if y < 200 else 55), cx, cy, c)
    return _save(fig, "hub.png")


def takeaways():
    fig, ax = _canvas(1100, 460)
    tops = [(30, "Kafka\nDurable\nStreaming", ORANGE), (300, "WebSocket\nReal-time\nPush", BLUE),
            (570, "Cedar\nAuthZ +\nPII Guardrails", RED), (840, "LLM\nRoot Cause +\nNext Steps", PURPLE)]
    for x, t, c in tops:
        _box(ax, x, 20, 230, 115, t, c, 15)
        _arrow(ax, x + 115, 137, 550 + (x - 435) * 0.25, 255, c)
    _box(ax, 410, 258, 280, 105, "Streaming MCP\nfor Insurers", TEAL, 19)
    _label(ax, 550, 410, "Live claims context → AI diagnosis → humans decide", TEAL, 20, fontweight="bold")
    return _save(fig, "takeaways.png")


ALL = [architecture, timeline, sequence, pipeline, hub, takeaways]

if __name__ == "__main__":
    for fn in ALL:
        print("wrote", fn())
