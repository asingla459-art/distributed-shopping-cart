"""Render report charts from experiments/results/*.json into docs/img/.  python3 -m experiments.charts"""
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

R = Path(__file__).parent / "results"
OUT = Path(__file__).resolve().parent.parent / "docs" / "img"
OUT.mkdir(parents=True, exist_ok=True)

SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
S1, S2, S3 = "#2a78d6", "#eb6834", "#1baf7a"  # categorical slots 1-3 (fixed order)

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
    "text.color": INK, "font.size": 10, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
    "axes.titlesize": 11, "axes.titleweight": "bold", "legend.frameon": False,
})


def load(name):
    return json.loads((R / name).read_text())


def bar_labels(ax, bars, fmt="{:g}"):
    for b in bars:
        ax.annotate(fmt.format(b.get_height()), (b.get_x() + b.get_width() / 2, b.get_height()),
                    xytext=(0, 3), textcoords="offset points", ha="center", fontsize=8.5, color=INK2)


def correctness():
    cfgs = [("naive", "Naive + instance cache", S2), ("naive-nocache", "Naive, no cache", S1),
            ("consistent", "Consistent design", S3)]
    data = {k: {r["experiment"]: r for r in load(f"correctness_{k}.json")} for k, _, _ in cfgs}
    panels = [
        ("E1 lost updates", "lost_pct", "Lost updates (% of acked adds)"),
        ("E2 overselling", "oversold_units", "Units oversold (stock = 5)"),
        ("E3 stale reads", "stale_pct", "Stale cross-instance reads (%)"),
        ("E4 duplicate retries", "duplicates_applied", "Duplicate adds applied (of 200)"),
        ("E5 concurrent quantity edits", "silent_overwrites", "Silent overwrites (of 50)"),
    ]
    fig, axes = plt.subplots(1, 5, figsize=(15, 3.6))
    for ax, (exp, key, title) in zip(axes, panels):
        vals = [max(0, data[k][exp][key]) for k, _, _ in cfgs]
        bars = ax.bar(range(3), vals, color=[c for *_, c in cfgs], width=0.62, edgecolor=SURFACE, linewidth=2)
        bar_labels(ax, bars)
        ax.set_title(title, fontsize=10)
        ax.set_xticks([])
        ax.set_ylim(0, max(vals) * 1.18 + 1)
    handles = [plt.Rectangle((0, 0), 1, 1, color=c) for *_, c in cfgs]
    fig.legend(handles, [l for _, l, _ in cfgs], loc="upper center", ncol=3, bbox_to_anchor=(0.5, 1.02))
    fig.suptitle("Correctness: what goes wrong without the consistency design (lower is better)",
                 y=1.1, fontweight="bold")
    fig.tight_layout()
    fig.savefig(OUT / "correctness.png", dpi=160, bbox_inches="tight")
    plt.close(fig)


def load_levels():
    cfgs = [("naive_cache1", "Naive (before)", S2), ("consistent_cache0", "Consistent, no L1 cache", S1),
            ("consistent_cache1", "Consistent + L1 cache (after)", S3)]
    levels = [("low", "Low\n5 users"), ("medium", "Medium\n25 users"), ("high", "High\n75 users")]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(12, 4))
    w = 0.26
    for i, (k, label, c) in enumerate(cfgs):
        d = [load(f"load_{k}_{lv}.json") for lv, _ in levels]
        xs = [j + (i - 1) * w for j in range(3)]
        b1 = a1.bar(xs, [x["throughput_rps"] for x in d], w, color=c, label=label, edgecolor=SURFACE, linewidth=2)
        b2 = a2.bar(xs, [x["p95_ms"] for x in d], w, color=c, label=label, edgecolor=SURFACE, linewidth=2)
        bar_labels(a1, b1, "{:.0f}")
        bar_labels(a2, b2, "{:.0f}")
    for ax, t, y in [(a1, "Throughput", "requests / second"), (a2, "p95 latency", "milliseconds")]:
        ax.set_xticks(range(3), [l for _, l in levels])
        ax.set_title(t)
        ax.set_ylabel(y)
    a1.set_ylim(0, 1150)
    h, l = a1.get_legend_handles_labels()
    fig.legend(h, l, loc="upper center", ncol=3, bbox_to_anchor=(0.5, 1.0))
    fig.suptitle("Load test at three workload levels (3 instances, 20 s per level, 0 % errors in all runs)",
                 fontweight="bold", y=1.07)
    fig.tight_layout()
    fig.savefig(OUT / "load_levels.png", dpi=160, bbox_inches="tight")
    plt.close(fig)


def failure():
    fig, axes = plt.subplots(2, 1, figsize=(12, 5.6), sharex=True, gridspec_kw={"height_ratios": [2.2, 1]})
    for k, label, c in [("naive-nocache", "Naive (before)", S2), ("consistent", "Consistent (after)", S3)]:
        a = load(f"failure_{k}.json")["part_a"]
        tl = a["timeline"][:35]
        axes[0].plot([b["t"] for b in tl], [b["ok"] for b in tl], color=c, lw=2, label=label)
        axes[1].plot([b["t"] for b in tl], [b["lb_retried"] for b in tl], color=c, lw=2, label=label)
        marks = a["marks"]
    for m in marks:
        for ax in axes:
            ax.axvline(m["t"], color=INK2, lw=1, ls="--")
    axes[0].axvspan(marks[0]["t"], marks[0]["t"] + 8, color=GRID, alpha=0.6, lw=0)
    axes[0].text(marks[0]["t"] + 0.2, axes[0].get_ylim()[1] * 0.97, "app-2: crash after commit (8 s)",
                 va="top", fontsize=8.5, color=INK2)
    axes[0].text(marks[1]["t"] + 0.2, axes[0].get_ylim()[1] * 0.97, "app-3: SIGKILL", va="top",
                 fontsize=8.5, color=INK2)
    axes[0].set_ylabel("acknowledged adds / s")
    axes[0].set_title("Throughput during controlled instance failures")
    axes[0].legend(loc="lower left")
    axes[1].set_ylabel("LB retries / s")
    axes[1].set_xlabel("seconds since start")
    fig.tight_layout()
    fig.savefig(OUT / "failure_timeline.png", dpi=160, bbox_inches="tight")
    plt.close(fig)


def cache():
    cfgs = [("netlat_cache0", "No L1 cache", S2), ("netlat_cache1", "L1 cache, round-robin LB", S1),
            ("netlat_cache1_affinity", "L1 cache + user affinity", S3)]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(12, 4))
    w = 0.26
    for i, (k, label, c) in enumerate(cfgs):
        d = [load(f"{k}_{lv}.json") for lv in ("low", "medium")]
        xs = [j + (i - 1) * w for j in range(2)]
        b1 = a1.bar(xs, [x["by_op"]["get"]["p50_ms"] for x in d], w, color=c, label=label, edgecolor=SURFACE, linewidth=2)
        b2 = a2.bar(xs, [x["throughput_rps"] for x in d], w, color=c, label=label, edgecolor=SURFACE, linewidth=2)
        bar_labels(a1, b1, "{:.1f}")
        bar_labels(a2, b2, "{:.0f}")
        for j, x in enumerate(d):
            if x["cache_hit_ratio_pct"] is not None:
                a1.annotate(f"hit {x['cache_hit_ratio_pct']:.0f}%", (xs[j], 0.3), ha="center",
                            fontsize=7.5, color=SURFACE, fontweight="bold")
    for ax, t, y in [(a1, "GET /cart median latency", "milliseconds"), (a2, "Throughput", "requests / second")]:
        ax.set_xticks(range(2), ["Low (5 users)", "Medium (25 users)"])
        ax.set_title(t)
        ax.set_ylabel(y)
    a2.legend(loc="upper left", fontsize=8.5)
    fig.suptitle("Cache before/after with Redis one network hop away (+2 ms RTT)", fontweight="bold")
    fig.tight_layout()
    fig.savefig(OUT / "cache_effect.png", dpi=160, bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    correctness(); load_levels(); failure(); cache()
    print("charts written to", OUT)
