"""Build the KPI dashboard from the Athena serving layer.

Usage:
    python deploy/build_dashboard.py            # prod (default)
    python deploy/build_dashboard.py --env dev

Runs each KPI view in Athena and writes a single self-contained HTML file (no external scripts)
to dashboard/kpi_dashboard_<env>.html. Rerun the script to refresh the numbers.
"""
from __future__ import annotations

import html
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parent.parent
REGION = "ap-southeast-2"

ENV = "prod"
if "--env" in sys.argv:
    ENV = sys.argv[sys.argv.index("--env") + 1]
DATABASE = f"retailbank_{ENV}"
WORKGROUP = f"retailbank-{ENV}"


def query(athena, sql: str) -> list[dict]:
    qid = athena.start_query_execution(
        QueryString=sql, WorkGroup=WORKGROUP,
        QueryExecutionContext={"Catalog": "AwsDataCatalog", "Database": DATABASE})["QueryExecutionId"]
    for _ in range(300):
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(f"{sql[:60]}...: {status.get('StateChangeReason', status['State'])}")
    rows, header, token = [], None, None
    while True:
        kwargs = {"QueryExecutionId": qid}
        if token:
            kwargs["NextToken"] = token
        page = athena.get_query_results(**kwargs)["ResultSet"]
        for i, row in enumerate(page["Rows"]):
            values = [c.get("VarCharValue") for c in row["Data"]]
            if header is None:
                header = values
            else:
                rows.append(dict(zip(header, values)))
        token = page.get("NextToken")
        if not token:
            break
    return rows


def num(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def fmt(v) -> str:
    return f"{num(v):,.0f}"


def table(rows: list[dict], cols: list[str], limit: int = 15) -> str:
    if not rows:
        return '<p class="muted">No rows.</p>'
    head = "".join(f"<th>{html.escape(c.replace('_', ' '))}</th>" for c in cols)
    body = []
    for r in rows[:limit]:
        cells = "".join(f"<td>{html.escape(str(r.get(c) or '—'))}</td>" for c in cols)
        body.append(f"<tr>{cells}</tr>")
    more = f'<p class="muted">Showing {min(limit, len(rows))} of {len(rows)} rows.</p>' if len(rows) > limit else ""
    return f'<div class="scroll"><table><thead><tr>{head}</tr></thead><tbody>{"".join(body)}</tbody></table></div>{more}'


def bars(items: list[tuple[str, float]], unit: str = "", width: int = 520) -> str:
    """Horizontal bar chart as inline SVG."""
    if not items:
        return '<p class="muted">No data.</p>'
    top = max(v for _, v in items) or 1
    row_h, label_w = 26, 160
    height = row_h * len(items) + 10
    parts = []
    for i, (label, v) in enumerate(items):
        y = i * row_h + 5
        w = (width - label_w - 80) * (v / top)
        parts.append(
            f'<text x="0" y="{y + 17}" class="axis">{html.escape(label)}</text>'
            f'<rect x="{label_w}" y="{y + 4}" width="{max(w, 1):.1f}" height="16" rx="3" class="bar"/>'
            f'<text x="{label_w + w + 6:.1f}" y="{y + 17}" class="val">{v:,.0f}{unit}</text>')
    return f'<svg viewBox="0 0 {width} {height}" width="100%" role="img">{"".join(parts)}</svg>'


def line_chart(series: dict[str, list[tuple[str, float]]], width: int = 720, height: int = 260) -> str:
    """Multi-series line chart (x = month labels) as inline SVG."""
    months = sorted({m for pts in series.values() for m, _ in pts})
    if not months:
        return '<p class="muted">No data.</p>'
    top = max(v for pts in series.values() for _, v in pts) or 1
    pad_l, pad_b, pad_t = 70, 36, 12
    plot_w, plot_h = width - pad_l - 10, height - pad_b - pad_t
    x_of = {m: pad_l + i * (plot_w / max(len(months) - 1, 1)) for i, m in enumerate(months)}
    palette = ["#2563eb", "#16a34a", "#dc2626", "#9333ea", "#ea580c"]
    out = []
    for k in range(5):
        y = pad_t + plot_h * (1 - k / 4)
        out.append(f'<line x1="{pad_l}" x2="{width - 10}" y1="{y:.1f}" y2="{y:.1f}" class="grid"/>')
        out.append(f'<text x="{pad_l - 8}" y="{y + 4:.1f}" class="axis" text-anchor="end">{top * k / 4:,.0f}</text>')
    for i, m in enumerate(months):
        if i % max(1, len(months) // 8) == 0 or i == len(months) - 1:
            out.append(f'<text x="{x_of[m]:.1f}" y="{height - 12}" class="axis" text-anchor="middle">{m[:7]}</text>')
    legend = []
    for n, (name, pts) in enumerate(series.items()):
        color = palette[n % len(palette)]
        coords = " ".join(f"{x_of[m]:.1f},{pad_t + plot_h * (1 - v / top):.1f}" for m, v in sorted(pts))
        out.append(f'<polyline points="{coords}" fill="none" stroke="{color}" stroke-width="2.2"/>')
        legend.append(f'<span><i style="background:{color}"></i>{html.escape(name)}</span>')
    return (f'<svg viewBox="0 0 {width} {height}" width="100%" role="img">{"".join(out)}</svg>'
            f'<div class="legend">{"".join(legend)}</div>')


def tile(label: str, value: str, note: str = "") -> str:
    return f'<div class="tile"><div class="tile-value">{value}</div><div class="tile-label">{label}</div><div class="tile-note">{note}</div></div>'


def section(title: str, body: str, kpi: str) -> str:
    return f'<section id="{kpi}"><h2>{title}</h2>{body}</section>'


def build(athena) -> str:
    k1 = query(athena, "SELECT * FROM kpi_01_top5_customers ORDER BY rnk")
    k2 = query(athena, "SELECT region, branch_id, month, volume FROM kpi_02_monthly_branch_trend ORDER BY month")
    k3 = query(athena, "SELECT category, product_id, month, revenue, category_share_pct FROM kpi_03_product_revenue")
    k4 = query(athena, "SELECT customer_id, account_id, last_txn_ts, product_type, dormant_flag FROM kpi_04_dormant_accounts")
    k5 = query(athena, "SELECT transaction_id, account_id, branch_id, txn_ts, amount, status, risk_reason FROM kpi_05_suspicious_txns")
    k6 = query(athena, "SELECT segment, customer_id, rfm_score, monetary FROM kpi_06_rfm_segments")
    k7 = query(athena, "SELECT region, branch_id, branch_name, net_volume, txn_count, rnk, performer_flag FROM kpi_07_branch_rank_region")
    k8 = query(athena, "SELECT kyc_status, is_not_verified, txn_count, total_value, pct_count, pct_value FROM kpi_08_kyc_risk_exposure")
    k9 = query(athena, "SELECT product_name, branch_id, total_count, refund_count, refund_count_pct, refund_value_pct FROM kpi_09_refund_rate")
    k10 = query(athena, "SELECT batch_id, entity, source_file, received, passed, rejected, reject_pct, top_reasons FROM kpi_10_dq_scorecard")
    k11 = query(athena, "SELECT * FROM kpi_11_day2_reconciliation ORDER BY batch_id")
    k12 = query(athena, "SELECT customer_id, customer_name, from_status, to_status, changed_on FROM kpi_12_kyc_transitions")
    k13 = query(athena, "SELECT activation_type, account_id, customer_id FROM kpi_13_day2_activation")

    # Headline tiles
    net_total = sum(num(r["net_volume"]) for r in k7)
    txn_total = sum(num(r["txn_count"]) for r in k7)
    not_verified = [r for r in k8 if r["is_not_verified"] == "true"]
    nv_pct = sum(num(r["pct_value"]) for r in not_verified)
    dq_received = sum(num(r["received"]) for r in k10)
    dq_rejected = sum(num(r["rejected"]) for r in k10)
    high_risk = sum(1 for r in k4 if r["dormant_flag"] == "High-Risk Dormant")
    tiles = "".join([
        tile("Net successful INR volume", f"₹{net_total:,.0f}", "KPI 7, all branches"),
        tile("Successful INR transactions", f"{txn_total:,.0f}", "KPI 7"),
        tile("Value with KYC not verified", f"{nv_pct:.1f}%", "KPI 8"),
        tile("Data quality reject rate", f"{100 * dq_rejected / dq_received:.1f}%" if dq_received else "—", "KPI 10, all batches"),
        tile("High-risk dormant accounts", f"{high_risk:,}", "KPI 4 (Credit Card / Loan)"),
        tile("Suspicious transaction flags", f"{len(k5):,}", "KPI 5"),
    ])

    # KPI 2: monthly volume per region
    by_region = defaultdict(lambda: defaultdict(float))
    for r in k2:
        by_region[r["region"]][r["month"][:7]] += num(r["volume"])
    series = {reg: sorted(m.items()) for reg, m in sorted(by_region.items())}
    s2 = line_chart({k: v for k, v in series.items()})

    # KPI 7: net volume by region, then top performers
    reg_totals = defaultdict(float)
    for r in k7:
        reg_totals[r["region"]] += num(r["net_volume"])
    s7 = bars(sorted(reg_totals.items(), key=lambda x: -x[1]), unit="")
    tops = [r for r in k7 if r["performer_flag"] == "TOP"]
    s7t = table(tops, ["region", "branch_id", "branch_name", "net_volume", "txn_count"], 10)

    # KPI 3: category revenue share
    cat = defaultdict(float)
    for r in k3:
        cat[r["category"]] += num(r["revenue"])
    s3 = bars(sorted(cat.items(), key=lambda x: -x[1]))

    # KPI 6: segments
    seg = defaultdict(int)
    for r in k6:
        seg[r["segment"]] += 1
    order = ["Platinum", "Gold", "Silver", "Bronze"]
    s6 = bars([(s, seg.get(s, 0)) for s in order])

    # KPI 8: KYC exposure by value
    s8 = bars([(r["kyc_status"], num(r["pct_value"])) for r in k8], unit="%")

    # KPI 5: flags by reason
    reasons = {"HIGH_VALUE": 0, "VELOCITY_3_IN_10M": 0, "NIGHT_00_05": 0}
    for r in k5:
        for name in reasons:
            if name in (r["risk_reason"] or ""):
                reasons[name] += 1
    s5 = bars(list(reasons.items()))

    # KPI 4: dormant by flag
    dorm = defaultdict(int)
    for r in k4:
        dorm[r["dormant_flag"]] += 1
    s4 = bars(list(dorm.items()))

    # KPI 11: day over day
    k11_table = table(k11, ["batch_id", "new_txns", "updated_txns", "corrected_txns", "customers_new",
                            "customers_changed", "kpi01_top5_net", "kpi01_delta", "kpi07_network_net", "kpi07_delta"], 5)

    # KPI 13: activation
    act = defaultdict(int)
    for r in k13:
        act[r["activation_type"]] += 1
    s13 = bars(list(act.items()))

    # KPI 9 refund rate, highest value share first
    k9_sorted = sorted(k9, key=lambda r: -num(r["refund_value_pct"]))

    sections = [
        section("KPI 2 · Monthly successful INR volume by region", s2, "kpi2"),
        section("KPI 7 · Net volume by region", s7, "kpi7"),
        section("KPI 7 · Top performer per region", s7t, "kpi7t"),
        section("KPI 1 · Top 5 customers by net volume", table(k1, ["rnk", "customer_id", "customer_name", "account_id", "net_volume", "txn_count"], 5), "kpi1"),
        section("KPI 3 · Revenue by category", s3, "kpi3"),
        section("KPI 6 · RFM segments (accounts)", s6, "kpi6"),
        section("KPI 8 · Share of value by KYC status", s8, "kpi8"),
        section("KPI 9 · Refund rate by product and branch (highest value share)", table(k9_sorted, ["product_name", "branch_id", "total_count", "refund_count", "refund_count_pct", "refund_value_pct"], 12), "kpi9"),
        section("KPI 5 · Suspicious transaction flags by rule", s5 + table(k5, ["transaction_id", "account_id", "branch_id", "txn_ts", "amount", "status", "risk_reason"], 10), "kpi5"),
        section("KPI 4 · Dormant accounts", s4 + table([r for r in k4 if r["dormant_flag"] == "High-Risk Dormant"], ["customer_id", "account_id", "last_txn_ts", "product_type", "dormant_flag"], 10), "kpi4"),
        section("KPI 10 · Data quality scorecard", table(k10, ["batch_id", "entity", "source_file", "received", "passed", "rejected", "reject_pct", "top_reasons"], 20), "kpi10"),
        section("KPI 11 · Day 2 reconciliation", k11_table, "kpi11"),
        section("KPI 12 · KYC status transitions (Day 1 to Day 2)", table(k12, ["customer_id", "customer_name", "from_status", "to_status", "changed_on"], 20), "kpi12"),
        section("KPI 13 · New account activation (Day 2)", s13 + table(k13, ["activation_type", "account_id", "customer_id"], 10), "kpi13"),
    ]

    stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    return PAGE.format(env=ENV, database=DATABASE, stamp=stamp, tiles=tiles, sections="".join(sections))


PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>RetailBank KPI dashboard ({env})</title>
<style>
  :root {{ --bg:#f5f7fb; --card:#ffffff; --ink:#0f172a; --muted:#64748b; --line:#e2e8f0; --bar:#2563eb; }}
  @media (prefers-color-scheme: dark) {{
    :root:not([data-theme="light"]) {{ --bg:#0b1220; --card:#131c2e; --ink:#e2e8f0; --muted:#94a3b8; --line:#1f2a3d; --bar:#60a5fa; }}
  }}
  :root[data-theme="dark"] {{ --bg:#0b1220; --card:#131c2e; --ink:#e2e8f0; --muted:#94a3b8; --line:#1f2a3d; --bar:#60a5fa; }}
  body {{ margin:0; background:var(--bg); color:var(--ink); font:14px/1.45 system-ui,-apple-system,Segoe UI,Roboto,sans-serif; }}
  header {{ padding:24px 20px 8px; }}
  h1 {{ margin:0; font-size:22px; }}
  .meta {{ color:var(--muted); font-size:13px; margin-top:4px; }}
  .tiles {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:12px; padding:12px 20px; }}
  .tile {{ background:var(--card); border:1px solid var(--line); border-radius:10px; padding:14px; }}
  .tile-value {{ font-size:22px; font-weight:600; }}
  .tile-label {{ font-size:13px; margin-top:4px; }}
  .tile-note {{ color:var(--muted); font-size:12px; }}
  main {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(420px,1fr)); gap:14px; padding:8px 20px 28px; }}
  section {{ background:var(--card); border:1px solid var(--line); border-radius:10px; padding:14px 16px; min-width:0; }}
  h2 {{ font-size:15px; margin:0 0 10px; }}
  .muted {{ color:var(--muted); font-size:12px; }}
  .scroll {{ overflow-x:auto; }}
  table {{ border-collapse:collapse; width:100%; font-size:12px; }}
  th,td {{ text-align:left; padding:5px 6px; border-bottom:1px solid var(--line); white-space:nowrap; }}
  th {{ color:var(--muted); font-weight:600; }}
  svg text.axis {{ fill:var(--muted); font-size:11px; }}
  svg text.val {{ fill:var(--ink); font-size:11px; }}
  svg .bar {{ fill:var(--bar); }}
  svg .grid {{ stroke:var(--line); }}
  .legend {{ display:flex; flex-wrap:wrap; gap:12px; font-size:12px; margin-top:6px; color:var(--muted); }}
  .legend i {{ display:inline-block; width:10px; height:10px; border-radius:2px; margin-right:5px; vertical-align:middle; }}
  @media (max-width:520px) {{ main {{ grid-template-columns:1fr; }} }}
</style></head>
<body>
<header>
  <h1>RetailBank KPI dashboard</h1>
  <div class="meta">Source: Athena database <code>{database}</code> · generated {stamp} · rerun <code>deploy/build_dashboard.py --env {env}</code> to refresh</div>
</header>
<div class="tiles">{tiles}</div>
<main>{sections}</main>
</body></html>
"""


def main():
    athena = boto3.Session(region_name=REGION).client("athena")
    page = build(athena)
    out_dir = ROOT / "dashboard"
    out_dir.mkdir(exist_ok=True)
    path = out_dir / f"kpi_dashboard_{ENV}.html"
    path.write_text(page, encoding="utf-8")
    print(f"wrote {path} ({path.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
