"""
Generate the GitHub social preview image (1280x640, GitHub's recommended size).

  pip install matplotlib==3.9.4
  python docs/social/make_social_preview.py     # writes docs/social/social-preview.png

Upload it in GitHub: Settings → General → Social preview → Edit → Upload an image.
"""

import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from matplotlib.patches import FancyBboxPatch  # noqa: E402

NAVY, NAVY_DARK, TEAL, ORANGE, BLUE, RED, GREY, PURPLE, WHITE = (
    "#2A2A4A", "#1E1E38", "#00D4AA", "#FF8C00", "#4488FF", "#FF4444", "#BBBBCC", "#9B59B6", "#FFFFFF")
W, H = 1280, 640
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "social-preview.png")


def box(ax, x, y, w, h, text, edge, size=17):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0,rounding_size=14",
                                facecolor=NAVY, edgecolor=edge, linewidth=4, zorder=3))
    ax.text(x + w / 2, y + h / 2, text, color=WHITE, fontsize=size, fontweight="bold",
            ha="center", va="center", zorder=4, linespacing=1.15)


def arrow(ax, x1, y, x2, color=TEAL):
    ax.annotate("", xy=(x2, y), xytext=(x1, y), zorder=2,
                arrowprops=dict(arrowstyle="-|>", color=color, lw=3.5, mutation_scale=22, shrinkA=0, shrinkB=0))


def pulse_line(ax, x0, x1, y, amp):
    """An ECG-style pulse: flat, spike, flat — the 'Pulse' in MCP Pulse."""
    xs = np.linspace(x0, x1, 400)
    t = (xs - x0) / (x1 - x0)
    ys = np.full_like(xs, y)
    for c, a in ((0.42, -0.35), (0.47, 1.0), (0.52, -0.6), (0.57, 0.25)):
        ys -= a * amp * np.exp(-((t - c) / 0.018) ** 2)
    ax.plot(xs, ys, color=TEAL, lw=5, solid_capstyle="round", zorder=2)


def main():
    fig = plt.figure(figsize=(W / 100, H / 100), dpi=100)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, W)
    ax.set_ylim(H, 0)
    ax.axis("off")
    fig.patch.set_facecolor(NAVY_DARK)

    # Title block
    pulse_line(ax, 70, 330, 120, 70)
    ax.text(360, 128, "MCP Pulse", color=WHITE, fontsize=64, fontweight="bold", va="center")
    ax.text(72, 232, "Stream live context to LLM agents — real-time anomaly detection",
            color=WHITE, fontsize=24, va="center")
    ax.text(72, 276, "and AI root-cause diagnosis over the Model Context Protocol",
            color=WHITE, fontsize=24, va="center")

    # Pipeline
    steps = [("Live\nEvents", GREY), ("Kafka", ORANGE), ("MCP\nServer", TEAL), ("LLM\nAgent", PURPLE), ("Diagnosis\n+ Owner", RED)]
    w, gap, y0, h = 190, 50, 340, 110
    x0 = (W - (5 * w + 4 * gap)) / 2  # centred: survives LinkedIn's ~1.91:1 crop
    for i, (t, c) in enumerate(steps):
        x = x0 + i * (w + gap)
        box(ax, x, y0, w, h, t, c)
        if i < len(steps) - 1:
            arrow(ax, x + w + 4, y0 + h / 2, x + w + gap - 4)

    # Tags + footer
    ax.text(72, 505, "Bedrock · OpenAI · Ollama   ·   Cedar authZ   ·   PII redaction   ·   Auto-insurance edition",
            color=GREY, fontsize=19, va="center")
    ax.plot([72, W - 72], [548, 548], color="#3A3A5C", lw=2)
    ax.text(72, 592, "github.com/kohli6/mcp-pulse", color=TEAL, fontsize=24, fontweight="bold", va="center")
    ax.text(W - 72, 592, "Fork it  ·  Apache-2.0", color=WHITE, fontsize=24, fontweight="bold",
            va="center", ha="right")

    fig.savefig(OUT, dpi=100, facecolor=fig.get_facecolor())
    plt.close(fig)
    return OUT


if __name__ == "__main__":
    print("wrote", main())
