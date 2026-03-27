from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean
from string import Template


def fmt_int(value: float | int) -> str:
    return f"{int(round(value)):,}".replace(",", ".")


def fmt_decimal(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}".replace(".", ",")


def fmt_pct(value: float, digits: int = 2) -> str:
    return f"{value * 100:.{digits}f}%".replace(".", ",")


def svg_line_chart(points: list[float], width: int, height: int, color: str) -> str:
    if not points:
        return ""
    min_v = min(points)
    max_v = max(points)
    pad = 24
    usable_w = width - pad * 2
    usable_h = height - pad * 2
    span = max(max_v - min_v, 1)
    coords = []
    for idx, value in enumerate(points):
        x = pad + (usable_w * idx / max(len(points) - 1, 1))
        y = pad + usable_h - ((value - min_v) / span) * usable_h
        coords.append((x, y))
    polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y in coords)
    area_points = f"{pad},{height-pad} " + polyline + f" {coords[-1][0]:.1f},{height-pad}"
    circles = "\n".join(
        f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{color}" />' for x, y in coords
    )
    return f"""
    <svg viewBox="0 0 {width} {height}" class="chart">
      <defs>
        <linearGradient id="fill-{color.replace('#','')}" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="{color}" stop-opacity="0.32"/>
          <stop offset="100%" stop-color="{color}" stop-opacity="0.02"/>
        </linearGradient>
      </defs>
      <line x1="{pad}" y1="{height-pad}" x2="{width-pad}" y2="{height-pad}" class="axis"/>
      <line x1="{pad}" y1="{pad}" x2="{pad}" y2="{height-pad}" class="axis"/>
      <polygon points="{area_points}" fill="url(#fill-{color.replace('#','')})"></polygon>
      <polyline points="{polyline}" fill="none" stroke="{color}" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"></polyline>
      {circles}
    </svg>
    """


def svg_bar_chart(labels: list[str], values: list[float], width: int, height: int, color: str) -> str:
    pad = 24
    usable_w = width - pad * 2
    usable_h = height - pad * 2
    max_v = max(values) if values else 1
    bar_w = usable_w / max(len(values), 1) * 0.7
    gap = usable_w / max(len(values), 1) * 0.3
    bars = []
    texts = []
    x = pad
    for label, value in zip(labels, values):
        h = 0 if max_v == 0 else (value / max_v) * usable_h
        y = pad + usable_h - h
        bars.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{h:.1f}" rx="8" fill="{color}" opacity="0.92"></rect>'
        )
        texts.append(
            f'<text x="{x + bar_w/2:.1f}" y="{height-6}" text-anchor="middle" class="tick">{label}</text>'
        )
        x += bar_w + gap
    return f"""
    <svg viewBox="0 0 {width} {height}" class="chart">
      <line x1="{pad}" y1="{height-pad}" x2="{width-pad}" y2="{height-pad}" class="axis"/>
      <line x1="{pad}" y1="{pad}" x2="{pad}" y2="{height-pad}" class="axis"/>
      {''.join(bars)}
      {''.join(texts)}
    </svg>
    """


def scale(value: float, min_v: float, max_v: float, out_min: float, out_max: float) -> float:
    if math.isclose(max_v, min_v):
        return (out_min + out_max) / 2
    return out_min + ((value - min_v) / (max_v - min_v)) * (out_max - out_min)


def svg_map_heatmap(heat_rows: list[dict], route_rows: list[dict], width: int, height: int) -> str:
    if not heat_rows:
        return ""
    lats = [r["lat_grid"] for r in heat_rows]
    lons = [r["lon_grid"] for r in heat_rows]
    pad = 32
    circles = []
    labels = []
    max_active = max(r["active_vehicles"] for r in heat_rows) or 1
    for r in heat_rows:
        x = scale(r["lon_grid"], min(lons), max(lons), pad, width - pad)
        y = scale(r["lat_grid"], min(lats), max(lats), height - pad, pad)
        radius = 4 + (r["active_vehicles"] / max_active) * 20
        opacity = 0.22 + (r["active_vehicles"] / max_active) * 0.6
        fill = "#ff7b45" if r["active_vehicles"] / max_active > 0.75 else "#19b7a8"
        circles.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{radius:.1f}" fill="{fill}" opacity="{opacity:.2f}"></circle>'
        )
    for r in route_rows[:10]:
        x = scale(r["avg_longitude"], min(lons), max(lons), pad, width - pad)
        y = scale(r["avg_latitude"], min(lats), max(lats), height - pad, pad)
        labels.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.5" fill="#f4f8fb"></circle>'
            f'<text x="{x + 6:.1f}" y="{y - 6:.1f}" class="map-label">{r["line_code"]}</text>'
        )
    return f"""
    <svg viewBox="0 0 {width} {height}" class="chart">
      <rect x="0" y="0" width="{width}" height="{height}" rx="18" fill="#0c1c29" stroke="#264157"/>
      <path d="M70 255 C160 120, 320 96, 450 132 S710 260, 900 214 S1160 120, 1320 170" stroke="#28455b" stroke-width="4" fill="none" opacity="0.55"/>
      <path d="M92 290 C250 200, 430 316, 620 246 S1000 160, 1260 250" stroke="#1c3142" stroke-width="3" fill="none" opacity="0.5"/>
      {''.join(circles)}
      {''.join(labels)}
      <text x="36" y="36" class="map-caption">Mapa operacional aproximado da região de São Paulo com base em lat_grid/lon_grid</text>
    </svg>
    """


def build_dashboard(csv_path: Path, heatmap_csv: Path, route_csv: Path, out_path: Path) -> None:
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        rows = []
        for raw in reader:
            row = {
                "event_date": raw["event_date"],
                "event_hour": int(raw["event_hour"]),
                "active_lines": int(raw["active_lines"]),
                "total_vehicles": int(raw["total_vehicles"]),
                "total_positions": int(raw["total_positions"]),
                "accessible_positions": int(raw["accessible_positions"]),
                "accessibility_pct": float(raw["accessibility_pct"]),
                "avg_vehicles_per_line": float(raw["avg_vehicles_per_line"]),
                "max_vehicles_in_line": int(raw["max_vehicles_in_line"]),
                "sum_active_vehicles_lines": int(raw["sum_active_vehicles_lines"]),
            }
            row["snapshot_dt"] = datetime.strptime(
                f"{row['event_date']} {row['event_hour']:02d}:00", "%Y-%m-%d %H:%M"
            )
            rows.append(row)

    with heatmap_csv.open() as f:
        heat_rows = []
        for raw in csv.DictReader(f):
            heat_rows.append(
                {
                    "event_date": raw["event_date"],
                    "event_hour": int(raw["event_hour"]),
                    "lat_grid": float(raw["lat_grid"]),
                    "lon_grid": float(raw["lon_grid"]),
                    "active_vehicles": int(raw["active_vehicles"]),
                    "total_positions": int(raw["total_positions"]),
                    "active_lines": int(raw["active_lines"]),
                    "accessible_positions": int(raw["accessible_positions"]),
                    "accessibility_pct": float(raw["accessibility_pct"]),
                }
            )

    with route_csv.open() as f:
        route_rows = []
        for raw in csv.DictReader(f):
            route_rows.append(
                {
                    "event_date": raw["event_date"],
                    "event_hour": int(raw["event_hour"]),
                    "line_code": raw["line_code"],
                    "active_vehicles": int(raw["active_vehicles"]),
                    "total_positions": int(raw["total_positions"]),
                    "avg_latitude": float(raw["avg_latitude"]),
                    "avg_longitude": float(raw["avg_longitude"]),
                }
            )

    rows.sort(key=lambda r: r["snapshot_dt"])
    heat_rows.sort(key=lambda r: (r["event_date"], r["event_hour"]))
    route_rows.sort(key=lambda r: (r["event_date"], r["event_hour"]))
    total_accessibility = sum(r["accessible_positions"] for r in rows) / sum(
        r["total_positions"] for r in rows
    )
    peak_vehicles = max(rows, key=lambda r: r["total_vehicles"])
    peak_positions = max(rows, key=lambda r: r["total_positions"])
    peak_lines = max(rows, key=lambda r: r["active_lines"])
    latest = max(rows, key=lambda r: r["snapshot_dt"])
    minimal_ops = [r for r in rows if r["total_vehicles"] <= 10]

    day_positions: dict[str, int] = defaultdict(int)
    day_vehicles: dict[str, int] = defaultdict(int)
    for r in rows:
        day_positions[r["event_date"]] += r["total_positions"]
        day_vehicles[r["event_date"]] += r["total_vehicles"]
    day_labels = sorted(day_positions.keys())

    snapshot_labels = [r["snapshot_dt"].strftime("%d/%m %Hh") for r in rows]
    vehicles_series = [r["total_vehicles"] for r in rows]
    lines_series = [r["active_lines"] for r in rows]
    access_series = [r["accessibility_pct"] * 100 for r in rows]

    top_days_positions = sorted(day_positions.items(), key=lambda kv: kv[1], reverse=True)[:5]
    top_days_vehicles = sorted(day_vehicles.items(), key=lambda kv: kv[1], reverse=True)[:5]

    latest_heat = max(heat_rows, key=lambda r: (r["event_date"], r["event_hour"]))
    latest_route_key = (latest_heat["event_date"], latest_heat["event_hour"])
    latest_heat_rows = [r for r in heat_rows if (r["event_date"], r["event_hour"]) == latest_route_key]
    latest_route_rows = [r for r in route_rows if (r["event_date"], r["event_hour"]) == latest_route_key]
    hottest_cells = sorted(latest_heat_rows, key=lambda r: r["active_vehicles"], reverse=True)[:8]
    top_lines = sorted(latest_route_rows, key=lambda r: r["active_vehicles"], reverse=True)[:10]

    insights = [
        f"Pico de veículos em {peak_vehicles['event_date']} às {peak_vehicles['event_hour']:02d}h com {fmt_int(peak_vehicles['total_vehicles'])} veículos.",
        f"Maior volume de posições em {peak_positions['event_date']} às {peak_positions['event_hour']:02d}h com {fmt_int(peak_positions['total_positions'])} registros.",
        f"Maior número de linhas ativas em {peak_lines['event_date']} às {peak_lines['event_hour']:02d}h com {fmt_int(peak_lines['active_lines'])} linhas.",
        f"Acessibilidade ponderada do período: {fmt_pct(total_accessibility)}.",
        f"No snapshot geográfico mais recente ({latest_heat['event_date']} às {latest_heat['event_hour']:02d}h), a célula mais intensa registrou {fmt_int(hottest_cells[0]['active_vehicles'])} veículos ativos.",
        f"A linha mais movimentada no snapshot geográfico mais recente foi {top_lines[0]['line_code']} com {fmt_int(top_lines[0]['active_vehicles'])} veículos ativos.",
    ]
    if minimal_ops:
        insights.append(
            f"Foram encontrados {len(minimal_ops)} snapshots com operação mínima (até 10 veículos), sugerindo janelas parciais ou carga incompleta."
        )

    html = Template(
        """<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Dashboard SP Mobility KPI</title>
  <style>
    :root {
      --bg: #091521;
      --panel: #102233;
      --panel-2: #12293d;
      --line: #24435a;
      --text: #f4f8fb;
      --muted: #9bb0c2;
      --teal: #19b7a8;
      --mint: #7de0c5;
      --amber: #e8c76a;
      --orange: #f08c4a;
      --danger: #ff7a6b;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Arial, Helvetica, sans-serif;
      background: linear-gradient(180deg, #0a1723 0%, #0f2235 100%);
      color: var(--text);
    }
    .wrap {
      max-width: 1480px;
      margin: 0 auto;
      padding: 32px;
    }
    .hero {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 24px;
      margin-bottom: 24px;
    }
    .hero h1 {
      margin: 0 0 8px 0;
      font-size: 36px;
    }
    .hero p {
      margin: 0;
      color: var(--muted);
      font-size: 18px;
      line-height: 1.5;
      max-width: 980px;
    }
    .tag {
      background: rgba(25,183,168,0.12);
      border: 1px solid rgba(125,224,197,0.35);
      color: #bff6ea;
      padding: 10px 14px;
      border-radius: 14px;
      font-size: 14px;
      min-width: 220px;
      text-align: right;
    }
    .grid-4, .grid-2, .grid-3 {
      display: grid;
      gap: 18px;
    }
    .grid-4 { grid-template-columns: repeat(4, 1fr); }
    .grid-3 { grid-template-columns: 1.2fr 1.2fr 1fr; }
    .grid-2 { grid-template-columns: 2fr 1fr; margin-top: 18px; }
    .panel {
      background: linear-gradient(180deg, var(--panel-2), var(--panel));
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: 20px 22px;
      box-shadow: 0 18px 36px rgba(0,0,0,0.18);
    }
    .kpi .label {
      color: var(--muted);
      font-size: 15px;
      margin-bottom: 10px;
    }
    .kpi .value {
      font-size: 42px;
      font-weight: 700;
      margin-bottom: 8px;
    }
    .kpi .sub {
      color: #9fe6d5;
      font-size: 14px;
      line-height: 1.4;
    }
    .title {
      font-size: 22px;
      margin: 0 0 6px 0;
    }
    .subtitle {
      margin: 0 0 18px 0;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.5;
    }
    .chart { width: 100%; height: auto; }
    .axis { stroke: #3a576d; stroke-width: 1.5; }
    .tick { fill: #89a2b8; font-size: 12px; }
    .insights {
      margin: 0;
      padding-left: 18px;
      color: #d8e5ef;
      line-height: 1.6;
    }
    .insights li { margin-bottom: 10px; }
    .table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 10px;
      font-size: 14px;
    }
    .table th, .table td {
      padding: 10px 12px;
      border-bottom: 1px solid #223a4e;
      text-align: left;
    }
    .table th { color: var(--muted); font-weight: 600; }
    .note {
      background: rgba(240,140,74,0.12);
      border: 1px solid rgba(240,140,74,0.35);
      color: #ffd7bb;
      padding: 16px 18px;
      border-radius: 16px;
      line-height: 1.6;
      font-size: 14px;
    }
    .badge {
      display: inline-block;
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 12px;
      margin-right: 8px;
      background: rgba(125,224,197,0.1);
      border: 1px solid rgba(125,224,197,0.25);
      color: #c9f4ea;
    }
    .footer {
      margin-top: 18px;
      color: var(--muted);
      font-size: 13px;
    }
    @media (max-width: 1100px) {
      .grid-4, .grid-3, .grid-2 { grid-template-columns: 1fr; }
      .tag { text-align: left; }
      .hero { flex-direction: column; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div>
        <h1>Dashboard Operacional SP Mobility</h1>
        <p>Visão analítica em português baseada no dataset extraído de <strong>mobility_kpis</strong> do projeto <strong>sp-mobility-data-platform</strong>. Este painel resume volume operacional, acessibilidade, intensidade por snapshot e consolidações por dia.</p>
      </div>
      <div class="tag">
        Último snapshot: <strong>$latest_snapshot</strong><br/>
        Período coberto: <strong>$date_range</strong>
      </div>
    </section>

    <section class="grid-4">
      <div class="panel kpi">
        <div class="label">Último total de veículos</div>
        <div class="value">$latest_vehicles</div>
        <div class="sub">Snapshot mais recente disponível no arquivo</div>
      </div>
      <div class="panel kpi">
        <div class="label">Últimas linhas ativas</div>
        <div class="value">$latest_lines</div>
        <div class="sub">Linhas em operação no snapshot mais recente</div>
      </div>
      <div class="panel kpi">
        <div class="label">Acessibilidade ponderada</div>
        <div class="value">$weighted_access</div>
        <div class="sub">Acessible positions / total positions no período</div>
      </div>
      <div class="panel kpi">
        <div class="label">Pico de posições</div>
        <div class="value">$peak_positions</div>
        <div class="sub">$peak_positions_sub</div>
      </div>
    </section>

    <section class="grid-2">
      <div class="panel">
        <h2 class="title">Tendência por snapshot</h2>
        <p class="subtitle">Série temporal ordenada por data e hora do arquivo. O gráfico resume a variação do total de veículos ao longo dos snapshots disponíveis.</p>
        $vehicles_chart
      </div>
      <div class="panel">
        <h2 class="title">Insights automáticos</h2>
        <p class="subtitle">Leituras objetivas derivadas do CSV.</p>
        <ul class="insights">
          $insights
        </ul>
      </div>
    </section>

    <section class="grid-3" style="margin-top: 18px;">
      <div class="panel">
        <h2 class="title">Linhas ativas por snapshot</h2>
        <p class="subtitle">Comparativo do número de linhas ativas em cada observação.</p>
        $lines_chart
      </div>
      <div class="panel">
        <h2 class="title">Acessibilidade por snapshot</h2>
        <p class="subtitle">Percentual de posições acessíveis, em pontos percentuais.</p>
        $access_chart
      </div>
      <div class="panel">
        <h2 class="title">Cobertura do arquivo</h2>
        <p class="subtitle">Resumo da amostra exportada.</p>
        <div style="margin-top: 10px;">
          <span class="badge">$row_count snapshots</span>
          <span class="badge">$distinct_days dias</span>
          <span class="badge">$hour_span horas distintas</span>
        </div>
        <table class="table">
          <tr><th>Métrica</th><th>Valor</th></tr>
          <tr><td>Média de veículos por snapshot</td><td>$avg_vehicles</td></tr>
          <tr><td>Média de linhas ativas</td><td>$avg_lines</td></tr>
          <tr><td>Máximo de veículos em uma linha</td><td>$max_line_peak</td></tr>
          <tr><td>Soma de posições acessíveis</td><td>$accessible_total</td></tr>
        </table>
      </div>
    </section>

    <section class="grid-2">
      <div class="panel">
        <h2 class="title">Top dias por volume de posições</h2>
        <p class="subtitle">Consolidação diária do total de posições registradas.</p>
        $positions_bar
        <table class="table">
          <tr><th>Data</th><th>Total de posições</th><th>Total de veículos</th></tr>
          $day_rows
        </table>
      </div>
      <div class="panel">
        <h2 class="title">Mapa operacional da região de São Paulo</h2>
        <p class="subtitle">Visualização espacial derivada de <strong>city_heatmap</strong> e dos centróides médios de <strong>route_performance</strong>.</p>
        $map_chart
      </div>
    </section>

    <section class="grid-2">
      <div class="panel">
        <h2 class="title">Top linhas no snapshot geográfico mais recente</h2>
        <p class="subtitle">Linhas com maior número de veículos ativos em $latest_geo_label.</p>
        $top_lines_bar
        <table class="table">
          <tr><th>Linha</th><th>Veículos ativos</th><th>Posições</th></tr>
          $line_rows
        </table>
      </div>
      <div class="panel">
        <h2 class="title">Hotspots geográficos</h2>
        <p class="subtitle">Células com maior intensidade operacional no snapshot geográfico mais recente.</p>
        <table class="table">
          <tr><th>Latitude</th><th>Longitude</th><th>Veículos</th><th>Linhas</th><th>Acessibilidade</th></tr>
          $hotspot_rows
        </table>
      </div>
    </section>

    <div class="footer">
      Arquivos-fonte: $source_name, $heatmap_name, $route_name
    </div>
  </div>
</body>
</html>"""
    ).substitute(
        latest_snapshot=latest["snapshot_dt"].strftime("%d/%m/%Y %Hh"),
        date_range=f"{rows[0]['snapshot_dt'].strftime('%d/%m/%Y')} a {rows[-1]['snapshot_dt'].strftime('%d/%m/%Y')}",
        latest_vehicles=fmt_int(latest["total_vehicles"]),
        latest_lines=fmt_int(latest["active_lines"]),
        weighted_access=fmt_pct(total_accessibility),
        peak_positions=fmt_int(peak_positions["total_positions"]),
        peak_positions_sub=f"{peak_positions['event_date']} às {peak_positions['event_hour']:02d}h",
        vehicles_chart=svg_line_chart(vehicles_series, 820, 280, "#19b7a8"),
        lines_chart=svg_line_chart(lines_series, 480, 240, "#7de0c5"),
        access_chart=svg_line_chart(access_series, 480, 240, "#e8c76a"),
        insights="".join(f"<li>{item}</li>" for item in insights),
        row_count=str(len(rows)),
        distinct_days=str(len(set(r["event_date"] for r in rows))),
        hour_span=str(len(set(r["event_hour"] for r in rows))),
        avg_vehicles=fmt_int(mean(r["total_vehicles"] for r in rows)),
        avg_lines=fmt_int(mean(r["active_lines"] for r in rows)),
        max_line_peak=fmt_int(max(r["max_vehicles_in_line"] for r in rows)),
        accessible_total=fmt_int(sum(r["accessible_positions"] for r in rows)),
        positions_bar=svg_bar_chart(
            [d[0][5:].replace("-", "/") for d in top_days_positions],
            [d[1] for d in top_days_positions],
            820,
            280,
            "#f08c4a",
        ),
        map_chart=svg_map_heatmap(latest_heat_rows, top_lines, 680, 340),
        latest_geo_label=f"{latest_heat['event_date']} às {latest_heat['event_hour']:02d}h",
        top_lines_bar=svg_bar_chart(
            [r["line_code"][:6] for r in top_lines[:8]],
            [r["active_vehicles"] for r in top_lines[:8]],
            820,
            280,
            "#19b7a8",
        ),
        day_rows="".join(
            f"<tr><td>{day}</td><td>{fmt_int(day_positions[day])}</td><td>{fmt_int(day_vehicles[day])}</td></tr>"
            for day in day_labels
        ),
        line_rows="".join(
            f"<tr><td>{r['line_code']}</td><td>{fmt_int(r['active_vehicles'])}</td><td>{fmt_int(r['total_positions'])}</td></tr>"
            for r in top_lines[:10]
        ),
        hotspot_rows="".join(
            f"<tr><td>{fmt_decimal(r['lat_grid'], 2)}</td><td>{fmt_decimal(r['lon_grid'], 2)}</td><td>{fmt_int(r['active_vehicles'])}</td><td>{fmt_int(r['active_lines'])}</td><td>{fmt_pct(r['accessibility_pct'])}</td></tr>"
            for r in hottest_cells
        ),
        source_name=csv_path.name,
        heatmap_name=heatmap_csv.name,
        route_name=route_csv.name,
    )
    out_path.write_text(html)


if __name__ == "__main__":
    source = Path("/Users/leandrosantos/Downloads/part-00000-tid-1100281769103572173-de224b58-138b-4135-910d-6c892b506b5d-459-1-c000.csv")
    heatmap = Path("/Users/leandrosantos/Downloads/part-00000-tid-4741433868598757345-ed29f1ad-ba2a-4c60-bb0e-cf9b3db5fde4-2-1-c000.csv")
    route = Path("/Users/leandrosantos/Downloads/part-00000-tid-7672676803377233145-cde911cd-e714-4f50-8211-de178ff792e0-1-1-c000.csv")
    out = Path("/Users/leandrosantos/Downloads/sp_mobility_dashboard_ptbr.html")
    build_dashboard(source, heatmap, route, out)
    print(out)
