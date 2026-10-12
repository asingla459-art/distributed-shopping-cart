"""Render captured terminal output (run/demo_*.txt, results JSON) as terminal screenshots in docs/img/.

python3 -m experiments.screenshots   (needs Playwright + Chromium)
"""
import html
import json
import re
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parent.parent
IMG = ROOT / "docs" / "img"
ANSI = {"1;36": "#56c7e8", "0;33": "#e8c35a"}


def to_html(text: str) -> str:
    out, color = [], None
    for part in re.split(r"(\x1b\[[0-9;]*m)", text):
        m = re.fullmatch(r"\x1b\[([0-9;]*)m", part)
        if m:
            if color:
                out.append("</span>")
            color = ANSI.get(m.group(1))
            if color:
                out.append(f'<span style="color:{color};font-weight:{700 if "1;" in m.group(1) else 400}">')
            continue
        esc = html.escape(part)
        esc = re.sub(r"(HTTP/1.1 (?:2\d\d)[^\n]*)", r'<span style="color:#7bd88f">\1</span>', esc)
        esc = re.sub(r"(HTTP/1.1 (?:4\d\d|5\d\d)[^\n]*)", r'<span style="color:#ff7a7a">\1</span>', esc)
        esc = re.sub(r"((?:idempotent-replayed|X-Upstream-Tries: 2)[^\n]*)", r'<span style="color:#ffb86c">\1</span>', esc)
        out.append(esc)
    if color:
        out.append("</span>")
    return "".join(out)


def page(title: str, body: str) -> str:
    return f"""<!doctype html><html><head><meta charset="utf-8"><style>
body{{margin:0;background:#fcfcfb;padding:18px;font-family:Inter,Arial,sans-serif}}
.win{{background:#1e1f22;border-radius:10px;box-shadow:0 2px 10px rgba(0,0,0,.18);overflow:hidden;width:1180px}}
.bar{{background:#2d2e32;padding:9px 14px;color:#c3c2b7;font-size:12.5px;display:flex;gap:8px;align-items:center}}
.dot{{width:11px;height:11px;border-radius:50%;display:inline-block}}
pre{{margin:0;padding:14px 18px;color:#e6e6e3;font:12.5px/1.5 "DejaVu Sans Mono",Consolas,monospace;white-space:pre-wrap;word-break:break-all}}
</style></head><body><div class="win"><div class="bar"><span class="dot" style="background:#ff5f57"></span>
<span class="dot" style="background:#febc2e"></span><span class="dot" style="background:#28c840"></span>
<span style="margin-left:10px">{html.escape(title)}</span></div><pre>{body}</pre></div></body></html>"""


def shots() -> list[tuple[str, str, str]]:
    run = ROOT / "run"
    items = []
    for name, title in [("api", "experiments/demo.sh api — API execution through the load balancer"),
                        ("conflict", "experiments/demo.sh conflict — optimistic concurrency (If-Match / ETag)"),
                        ("redis", "experiments/demo.sh redis — storage state in Redis"),
                        ("oversell", "experiments/demo.sh oversell — atomic checkout & oversell protection"),
                        ("failure", "experiments/demo.sh failure — instance crash & LB failover"),
                        ("metrics", "experiments/demo.sh metrics — Prometheus metrics & audit events"),
                        ("logs", "experiments/demo.sh logs — structured JSON logs")]:
        f = run / f"demo_{name}.txt"
        if f.exists():
            items.append((f"screen_{name}.png", title, to_html(f.read_text())))
    res = ROOT / "experiments" / "results"
    lt = json.loads((res / "load_consistent_cache1_medium.json").read_text())
    lt.pop("by_op", None)
    txt = ("\x1b[0;33m$ python3 -m loadtest.loadgen --users 25 --duration 20 --label medium\x1b[0m\n"
           + json.dumps(lt, indent=2))
    items.append(("screen_loadtest.png", "load test — medium workload, consistent mode", to_html(txt)))
    fa = json.loads((res / "failure_consistent.json").read_text())
    a = {k: v for k, v in fa["part_a"].items() if k != "timeline"}
    txt = ("\x1b[0;33m$ python3 -m experiments.failure consistent\x1b[0m\n" + json.dumps(a, indent=2)
           + "\n" + json.dumps(fa["part_b"], indent=2))
    items.append(("screen_failure_experiment.png", "failure experiment — consistent mode", to_html(txt)))
    return items


def main() -> None:
    with sync_playwright() as p:
        b = p.chromium.launch()
        pg = b.new_page(viewport={"width": 1216, "height": 400}, device_scale_factor=2)
        for fname, title, body in shots():
            pg.set_content(page(title, body))
            pg.locator(".win").screenshot(path=str(IMG / fname))
            print("wrote", fname)
        b.close()


if __name__ == "__main__":
    main()
