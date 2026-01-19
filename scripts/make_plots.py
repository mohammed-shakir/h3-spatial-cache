#!/usr/bin/env python3
"""
Reads:
  - runs_long.csv
  - runs_agg.csv

and generates all requested thesis plots + tables (CSV + minimal LaTeX tabular).

Default location:
  scripts/aggregated/runs_long.csv
  scripts/aggregated/runs_agg.csv

Outputs:
  scripts/aggregated/<fig_*.png>
  scripts/aggregated/table_*.csv
  scripts/aggregated/table_*.tex

Run from root:
python3 scripts/make_plots.py
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402


def warn(msg: str) -> None:
    print(f"[WARN] {msg}")


def wrote(path: Path) -> None:
    print(f"Wrote {path}")


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def coerce_numeric(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def save_fig(fig: plt.Figure, outpath: Path) -> None:
    ensure_dir(outpath.parent)
    try:
        fig.tight_layout()
    except Exception:
        pass
    fig.savefig(outpath, dpi=220, bbox_inches="tight")
    plt.close(fig)
    wrote(outpath)


def sort_by_zipf(df: pd.DataFrame) -> pd.DataFrame:
    if "zipf_s" in df.columns:
        return df.sort_values(["zipf_s"])
    return df


def get_config_label(scenario: str, h3_res: int) -> str:
    if scenario == "baseline" and h3_res == 0:
        return "baseline"
    if scenario == "cache":
        return f"cache r{int(h3_res)}"
    return f"{scenario} r{int(h3_res)}"


def filter_config(df: pd.DataFrame, scenario: str, h3_res: int) -> pd.DataFrame:
    return df[(df["scenario"] == scenario) & (df["h3_res"] == h3_res)].copy()


def line_configs_for_800() -> List[Tuple[str, int]]:
    return [
        ("baseline", 0),
        ("cache", 7),
        ("cache", 8),
        ("cache", 9),
    ]


def median_missed_tokens_from_long(df_long_800: pd.DataFrame) -> pd.DataFrame:
    needed = ["scenario", "h3_res", "zipf_s", "target_rps", "missed_tokens"]
    for c in needed:
        if c not in df_long_800.columns:
            return pd.DataFrame(columns=needed)
    g = (
        df_long_800
        .groupby(["scenario", "h3_res", "zipf_s", "target_rps"], dropna=False)["missed_tokens"]
        .median()
        .reset_index()
        .rename(columns={"missed_tokens": "missed_tokens_median"})
    )
    return g


def latex_escape(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    rep = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    for k, v in rep.items():
        s = s.replace(k, v)
    return s


def is_nan(x) -> bool:
    try:
        return x is None or (isinstance(x, float) and math.isnan(x))
    except Exception:
        return False


def format_cell(val, col: str) -> str:
    if is_nan(val):
        return "--"
    if col in {"h3_res", "target_rps", "n_reps", "errors_sum"}:
        try:
            return str(int(round(float(val))))
        except Exception:
            return latex_escape(str(val))
    if col == "zipf_s":
        try:
            return f"{float(val):.1f}"
        except Exception:
            return latex_escape(str(val))
    if col.startswith("best_h3_res_"):
        try:
            return str(int(round(float(val))))
        except Exception:
            return "--"
    if col.startswith("best_") and col.endswith("_ms"):
        try:
            return f"{float(val):.3f}"
        except Exception:
            return "--"
    try:
        f = float(val)
        return f"{f:.3f}"
    except Exception:
        return latex_escape(str(val))


def write_minimal_latex_table(df: pd.DataFrame, outpath: Path, columns: List[str]) -> None:
    ensure_dir(outpath.parent)

    numeric_like = {
        "h3_res", "zipf_s", "target_rps", "p50_ms_median", "p95_ms_median", "p99_ms_median",
        "throughput_rps_median", "errors_sum",
        "postgis_cpu_avg_pct_median", "geoserver_cpu_avg_pct_median",
        "postgis_mem_avg_mib_median", "geoserver_mem_avg_mib_median",
        "n_reps",
        "best_h3_res_p50", "best_p50_ms",
        "best_h3_res_p95", "best_p95_ms",
        "best_h3_res_p99", "best_p99_ms",
    }
    aligns = ["r" if c in numeric_like else "l" for c in columns]
    colspec = "".join(aligns)

    lines: List[str] = []
    lines.append(r"\begin{tabular}{" + colspec + r"}")
    lines.append(r"\hline")
    header = " & ".join(latex_escape(c) for c in columns) + r" \\"
    lines.append(header)
    lines.append(r"\hline")

    for _, row in df.iterrows():
        cells = [format_cell(row.get(c), c) if c in numeric_like else latex_escape(row.get(c)) for c in columns]
        lines.append(" & ".join(cells) + r" \\")
    lines.append(r"\hline")
    lines.append(r"\end{tabular}")

    outpath.write_text("\n".join(lines) + "\n", encoding="utf-8")
    wrote(outpath)


def load_csvs(indir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    long_path = indir / "runs_long.csv"
    agg_path = indir / "runs_agg.csv"

    if not long_path.exists():
        raise FileNotFoundError(f"Missing {long_path}")
    if not agg_path.exists():
        raise FileNotFoundError(f"Missing {agg_path}")

    df_long = pd.read_csv(long_path)
    df_agg = pd.read_csv(agg_path)

    long_numeric = [
        "rep", "h3_res", "zipf_s_from_folder", "duration_sec", "total", "success", "errors",
        "throughput_rps", "target_rps", "achieved_to_target_ratio", "missed_tokens",
        "max_backlog", "token_buffer",
        "p50_ms", "p95_ms", "p99_ms", "concurrency", "zipf_s", "zipf_v", "bboxes", "seed",
        "postgis_cpu_avg_pct", "geoserver_cpu_avg_pct", "postgis_mem_avg_mib", "geoserver_mem_avg_mib",
    ]
    df_long = coerce_numeric(df_long, long_numeric)

    agg_numeric = [
        "h3_res", "zipf_s", "target_rps",
        "p50_ms_median", "p95_ms_median", "p99_ms_median",
        "throughput_rps_median", "errors_sum",
        "postgis_cpu_avg_pct_median", "geoserver_cpu_avg_pct_median",
        "postgis_mem_avg_mib_median", "geoserver_mem_avg_mib_median",
        "n_reps",
    ]
    df_agg = coerce_numeric(df_agg, agg_numeric)

    return df_long, df_agg


def plot_latency_vs_zipf(df_agg: pd.DataFrame, df_long: pd.DataFrame, outdir: Path) -> None:
    dfA = df_agg[df_agg["target_rps"] == 800].copy()
    if dfA.empty:
        warn("A) No rows for target_rps==800 in runs_agg.csv; skipping A plots.")
        return

    dfL = df_long[df_long["target_rps"] == 800].copy()

    specs = [
        ("p50_ms_median", "P50 latency (ms)", "fig_latency_p50_vs_zipf_800.png"),
        ("p95_ms_median", "P95 latency (ms)", "fig_latency_p95_vs_zipf_800.png"),
        ("p99_ms_median", "P99 latency (ms)", "fig_latency_p99_vs_zipf_800.png"),
    ]

    for ycol, ylabel, fname in specs:
        fig, ax = plt.subplots()
        any_line = False

        for scenario, h3_res in line_configs_for_800():
            sub = filter_config(dfA, scenario, h3_res)
            sub = sub.dropna(subset=["zipf_s", ycol])
            if sub.empty:
                continue
            sub = sub.sort_values("zipf_s")
            ax.plot(sub["zipf_s"], sub[ycol], marker="o", label=get_config_label(scenario, h3_res))
            any_line = True

            if not dfL.empty:
                rep_y = ycol.replace("_median", "").replace("p", "p")  # p50_ms, etc.
                rep_y = rep_y.replace("p50_ms", "p50_ms").replace("p95_ms", "p95_ms").replace("p99_ms", "p99_ms")
                subL = dfL[(dfL["scenario"] == scenario) & (dfL["h3_res"] == h3_res)]
                subL = subL.dropna(subset=["zipf_s", rep_y])
                if not subL.empty:
                    ax.scatter(subL["zipf_s"], subL[rep_y], s=18, alpha=0.45)

        if not any_line:
            warn(f"A) No data for {fname}; skipping.")
            plt.close(fig)
            continue

        ax.set_xlabel("Zipf skew (s)")
        ax.set_ylabel(ylabel)
        ax.grid(True, alpha=0.25)
        ax.legend()
        save_fig(fig, outdir / fname)


def plot_latency_vs_h3res(df_agg: pd.DataFrame, outdir: Path) -> None:
    dfB = df_agg[(df_agg["target_rps"] == 800) & (df_agg["scenario"].isin(["baseline", "cache"]))].copy()
    if dfB.empty:
        warn("B) No rows for target_rps==800 in runs_agg.csv; skipping B plots.")
        return

    dfB = dfB[((dfB["scenario"] == "baseline") & (dfB["h3_res"] == 0)) |
              ((dfB["scenario"] == "cache") & (dfB["h3_res"].isin([7, 8, 9])))]
    if dfB.empty:
        warn("B) After filtering to baseline r0 + cache r7/8/9, no rows; skipping.")
        return

    specs = [
        ("p50_ms_median", "P50 latency (ms)", "fig_latency_p50_vs_h3res_800_by_zipf.png"),
        ("p95_ms_median", "P95 latency (ms)", "fig_latency_p95_vs_h3res_800_by_zipf.png"),
        ("p99_ms_median", "P99 latency (ms)", "fig_latency_p99_vs_h3res_800_by_zipf.png"),
    ]

    zipfs = sorted([z for z in dfB["zipf_s"].dropna().unique()])
    if not zipfs:
        warn("B) No zipf_s values; skipping.")
        return

    x_order = [0, 7, 8, 9]

    for ycol, ylabel, fname in specs:
        fig, ax = plt.subplots()
        any_line = False

        for z in zipfs:
            zdf = dfB[dfB["zipf_s"] == z].copy()
            if zdf.empty:
                continue

            xs, ys = [], []
            for x in x_order:
                if x == 0:
                    row = zdf[(zdf["scenario"] == "baseline") & (zdf["h3_res"] == 0)]
                else:
                    row = zdf[(zdf["scenario"] == "cache") & (zdf["h3_res"] == x)]
                if row.empty:
                    continue
                yv = row[ycol].iloc[0]
                if pd.isna(yv):
                    continue
                xs.append(x)
                ys.append(yv)

            if len(xs) >= 2:
                ax.plot(xs, ys, marker="o", label=f"zipf_s={z:.1f}")
                any_line = True
            elif len(xs) == 1:
                ax.scatter(xs, ys, label=f"zipf_s={z:.1f}")
                any_line = True

        if not any_line:
            warn(f"B) No data for {fname}; skipping.")
            plt.close(fig)
            continue

        ax.set_xlabel("H3 resolution (0=baseline)")
        ax.set_ylabel(ylabel)
        ax.set_xticks(x_order)
        ax.grid(True, alpha=0.25)
        ax.legend()
        save_fig(fig, outdir / fname)


def plot_throughput_and_errors(df_agg: pd.DataFrame, df_long: pd.DataFrame, outdir: Path) -> None:
    dfC = df_agg[df_agg["target_rps"] == 800].copy()
    if dfC.empty:
        warn("C) No rows for target_rps==800 in runs_agg.csv; skipping C plots.")
        return

    dfC["achieved_ratio_calc"] = dfC["throughput_rps_median"] / dfC["target_rps"]

    fig, ax = plt.subplots()
    any_line = False
    for scenario, h3_res in line_configs_for_800():
        sub = filter_config(dfC, scenario, h3_res).dropna(subset=["zipf_s", "achieved_ratio_calc"])
        if sub.empty:
            continue
        sub = sub.sort_values("zipf_s")
        ax.plot(sub["zipf_s"], sub["achieved_ratio_calc"], marker="o", label=get_config_label(scenario, h3_res))
        any_line = True
    if any_line:
        ax.set_xlabel("Zipf skew (s)")
        ax.set_ylabel("Achieved / target (ratio)")
        ax.grid(True, alpha=0.25)
        ax.legend()
        save_fig(fig, outdir / "fig_achieved_ratio_vs_zipf_800.png")
    else:
        warn("C7) No data for achieved ratio plot; skipping.")
        plt.close(fig)

    fig, ax = plt.subplots()
    any_line = False
    for scenario, h3_res in line_configs_for_800():
        sub = filter_config(dfC, scenario, h3_res).dropna(subset=["zipf_s", "errors_sum"])
        if sub.empty:
            continue
        sub = sub.sort_values("zipf_s")
        ax.plot(sub["zipf_s"], sub["errors_sum"], marker="o", label=get_config_label(scenario, h3_res))
        any_line = True
    if any_line:
        ax.set_xlabel("Zipf skew (s)")
        ax.set_ylabel("Errors (sum across reps)")
        ax.grid(True, alpha=0.25)
        ax.legend()
        save_fig(fig, outdir / "fig_errors_vs_zipf_800.png")
    else:
        warn("C8) No data for errors plot; skipping.")
        plt.close(fig)

    dfL = df_long[df_long["target_rps"] == 800].copy()
    if dfL.empty or "missed_tokens" not in dfL.columns:
        warn("C9) runs_long has no target_rps==800 or missed_tokens; skipping missed_tokens plot.")
        return

    mt = median_missed_tokens_from_long(dfL)
    if mt.empty:
        warn("C9) Could not compute missed_tokens median; skipping.")
        return

    fig, ax = plt.subplots()
    any_line = False
    for scenario, h3_res in line_configs_for_800():
        sub = mt[(mt["scenario"] == scenario) & (mt["h3_res"] == h3_res)].dropna(subset=["zipf_s", "missed_tokens_median"])
        if sub.empty:
            continue
        sub = sub.sort_values("zipf_s")
        ax.plot(sub["zipf_s"], sub["missed_tokens_median"], marker="o", label=get_config_label(scenario, h3_res))
        any_line = True

    if any_line:
        ax.set_xlabel("Zipf skew (s)")
        ax.set_ylabel("Missed tokens (median across reps)")
        ax.grid(True, alpha=0.25)
        ax.legend()
        save_fig(fig, outdir / "fig_missed_tokens_vs_zipf_800.png")
    else:
        warn("C9) No data for missed_tokens plot; skipping.")
        plt.close(fig)


def plot_cache_context_proxies(df_agg: pd.DataFrame, outdir: Path) -> None:
    dfD = df_agg[df_agg["target_rps"] == 800].copy()
    if dfD.empty:
        warn("D) No rows for target_rps==800; skipping speedup plots.")
        return

    base = dfD[(dfD["scenario"] == "baseline") & (dfD["h3_res"] == 0)].dropna(subset=["zipf_s"])
    if base.empty:
        warn("D) No baseline rows at 800 RPS; skipping speedup plots.")
        return

    base = base.set_index("zipf_s")

    specs = [
        ("p50_ms_median", "Speedup (baseline/cache) @ P50", "fig_speedup_p50_vs_zipf_800.png"),
        ("p95_ms_median", "Speedup (baseline/cache) @ P95", "fig_speedup_p95_vs_zipf_800.png"),
        ("p99_ms_median", "Speedup (baseline/cache) @ P99", "fig_speedup_p99_vs_zipf_800.png"),
    ]

    for metric, ylabel, fname in specs:
        fig, ax = plt.subplots()
        any_line = False

        for res in [7, 8, 9]:
            cache = dfD[(dfD["scenario"] == "cache") & (dfD["h3_res"] == res)].dropna(subset=["zipf_s", metric])
            if cache.empty:
                continue
            cache = cache.set_index("zipf_s")

            joined = base[[metric]].join(cache[[metric]], lsuffix="_base", rsuffix="_cache", how="inner").dropna()
            if joined.empty:
                continue

            denom = joined[f"{metric}_cache"].replace(0, pd.NA)
            speed = (joined[f"{metric}_base"] / denom).astype(float)
            speed = speed.dropna()

            if speed.empty:
                continue

            xs = sorted(speed.index.tolist())
            ys = [float(speed.loc[x]) for x in xs]
            ax.plot(xs, ys, marker="o", label=f"cache r{res}")
            any_line = True

        if not any_line:
            warn(f"D) No data for {fname}; skipping.")
            plt.close(fig)
            continue

        ax.set_xlabel("Zipf skew (s)")
        ax.set_ylabel(ylabel + " (ratio)")
        ax.grid(True, alpha=0.25)
        ax.legend()
        save_fig(fig, outdir / fname)


def plot_backend_load(df_agg: pd.DataFrame, outdir: Path) -> None:
    dfE = df_agg[df_agg["target_rps"] == 800].copy()
    if dfE.empty:
        warn("E) No rows for target_rps==800; skipping backend plots.")
        return

    plot_specs = [
        ("postgis_cpu_avg_pct_median", "PostGIS CPU (%)", "fig_backend_postgis_cpu_vs_zipf_800.png"),
        ("geoserver_cpu_avg_pct_median", "GeoServer CPU (%)", "fig_backend_geoserver_cpu_vs_zipf_800.png"),
        ("postgis_mem_avg_mib_median", "PostGIS memory (MiB)", "fig_backend_postgis_mem_vs_zipf_800.png"),
        ("geoserver_mem_avg_mib_median", "GeoServer memory (MiB)", "fig_backend_geoserver_mem_vs_zipf_800.png"),
    ]

    for ycol, ylabel, fname in plot_specs:
        fig, ax = plt.subplots()
        any_line = False

        for scenario, h3_res in line_configs_for_800():
            sub = filter_config(dfE, scenario, h3_res).dropna(subset=["zipf_s", ycol])
            if sub.empty:
                continue
            sub = sub.sort_values("zipf_s")
            ax.plot(sub["zipf_s"], sub[ycol], marker="o", label=get_config_label(scenario, h3_res))
            any_line = True

        if not any_line:
            warn(f"E) No data for {fname}; skipping.")
            plt.close(fig)
            continue

        ax.set_xlabel("Zipf skew (s)")
        ax.set_ylabel(ylabel)
        ax.grid(True, alpha=0.25)
        ax.legend()
        save_fig(fig, outdir / fname)

    base = dfE[(dfE["scenario"] == "baseline") & (dfE["h3_res"] == 0)].dropna(subset=["zipf_s", "postgis_cpu_avg_pct_median"])
    if base.empty:
        warn("E17) No baseline PostGIS CPU at 800 RPS; skipping offload factor plot.")
        return

    base = base.set_index("zipf_s")

    fig, ax = plt.subplots()
    any_line = False
    for res in [7, 8, 9]:
        cache = dfE[(dfE["scenario"] == "cache") & (dfE["h3_res"] == res)].dropna(subset=["zipf_s", "postgis_cpu_avg_pct_median"])
        if cache.empty:
            continue
        cache = cache.set_index("zipf_s")
        joined = base[["postgis_cpu_avg_pct_median"]].join(
            cache[["postgis_cpu_avg_pct_median"]],
            lsuffix="_base",
            rsuffix="_cache",
            how="inner",
        ).dropna()
        if joined.empty:
            continue
        denom = joined["postgis_cpu_avg_pct_median_cache"].replace(0, pd.NA)
        off = (joined["postgis_cpu_avg_pct_median_base"] / denom).astype(float).dropna()
        if off.empty:
            continue
        xs = sorted(off.index.tolist())
        ys = [float(off.loc[x]) for x in xs]
        ax.plot(xs, ys, marker="o", label=f"cache r{res}")
        any_line = True

    if not any_line:
        warn("E17) No data for offload factor plot; skipping.")
        plt.close(fig)
        return

    ax.set_xlabel("Zipf skew (s)")
    ax.set_ylabel("PostGIS CPU offload factor (baseline/cache)")
    ax.grid(True, alpha=0.25)
    ax.legend()
    save_fig(fig, outdir / "fig_backend_offload_factor_postgis_cpu_vs_zipf_800.png")


def plot_load_sensitivity(df_agg: pd.DataFrame, outdir: Path) -> None:
    dfF = df_agg[df_agg["zipf_s"] == 1.3].copy()
    if dfF.empty:
        warn("F) No rows for zipf_s==1.3 in runs_agg; skipping load sensitivity.")
        return

    base = dfF[(dfF["scenario"] == "baseline") & (dfF["h3_res"] == 0)].copy()
    cache = dfF[(dfF["scenario"] == "cache") & (dfF["h3_res"] == 8)].copy()

    wanted_rps = [600, 800, 1000]
    base = base[base["target_rps"].isin(wanted_rps)]
    cache = cache[cache["target_rps"].isin(wanted_rps)]

    if base.empty and cache.empty:
        warn("F) No baseline/cache rows for zipf_s==1.3 with rps in {600,800,1000}; skipping.")
        return

    def plot_two_lines(ycol: str, ylabel: str, fname: str) -> None:
        fig, ax = plt.subplots()
        any_line = False

        if not base.empty:
            s = base.dropna(subset=["target_rps", ycol]).sort_values("target_rps")
            if not s.empty:
                ax.plot(s["target_rps"], s[ycol], marker="o", label="baseline (r0)")
                any_line = True

        if not cache.empty:
            s = cache.dropna(subset=["target_rps", ycol]).sort_values("target_rps")
            if not s.empty:
                ax.plot(s["target_rps"], s[ycol], marker="o", label="cache (r8)")
                any_line = True

        if not any_line:
            warn(f"F) No data for {fname}; skipping.")
            plt.close(fig)
            return

        ax.set_xlabel("Target load (RPS)")
        ax.set_ylabel(ylabel)
        ax.set_xticks(wanted_rps)
        ax.grid(True, alpha=0.25)
        ax.legend()
        save_fig(fig, outdir / fname)

    plot_two_lines("p50_ms_median", "P50 latency (ms)", "fig_loadsens_p50_vs_rps_zipf1p3_r8.png")
    plot_two_lines("p95_ms_median", "P95 latency (ms)", "fig_loadsens_p95_vs_rps_zipf1p3_r8.png")
    plot_two_lines("p99_ms_median", "P99 latency (ms)", "fig_loadsens_p99_vs_rps_zipf1p3_r8.png")

    plot_two_lines("postgis_cpu_avg_pct_median", "PostGIS CPU (%)", "fig_loadsens_postgis_cpu_vs_rps_zipf1p3_r8.png")

    base2 = base.copy()
    cache2 = cache.copy()
    if not base2.empty:
        base2["achieved_ratio_calc"] = base2["throughput_rps_median"] / base2["target_rps"]
    if not cache2.empty:
        cache2["achieved_ratio_calc"] = cache2["throughput_rps_median"] / cache2["target_rps"]

    fig, ax = plt.subplots()
    any_line = False
    if not base2.empty:
        s = base2.dropna(subset=["target_rps", "achieved_ratio_calc"]).sort_values("target_rps")
        if not s.empty:
            ax.plot(s["target_rps"], s["achieved_ratio_calc"], marker="o", label="baseline (r0)")
            any_line = True
    if not cache2.empty:
        s = cache2.dropna(subset=["target_rps", "achieved_ratio_calc"]).sort_values("target_rps")
        if not s.empty:
            ax.plot(s["target_rps"], s["achieved_ratio_calc"], marker="o", label="cache (r8)")
            any_line = True

    if any_line:
        ax.set_xlabel("Target load (RPS)")
        ax.set_ylabel("Achieved / target (ratio)")
        ax.set_xticks(wanted_rps)
        ax.grid(True, alpha=0.25)
        ax.legend()
        save_fig(fig, outdir / "fig_loadsens_achieved_ratio_vs_rps_zipf1p3_r8.png")
    else:
        warn("F22) No data for achieved ratio load-sensitivity plot; skipping.")
        plt.close(fig)


def write_tables(df_agg: pd.DataFrame, df_long: pd.DataFrame, outdir: Path) -> None:
    ensure_dir(outdir)

    cols_800 = [
        "scenario", "h3_res", "zipf_s",
        "p50_ms_median", "p95_ms_median", "p99_ms_median",
        "throughput_rps_median", "errors_sum",
        "postgis_cpu_avg_pct_median", "geoserver_cpu_avg_pct_median",
        "postgis_mem_avg_mib_median", "geoserver_mem_avg_mib_median",
        "n_reps",
    ]
    t1 = df_agg[df_agg["target_rps"] == 800].copy()
    if t1.empty:
        warn("Tables) No rows for target_rps==800; skipping table_800_summary.")
    else:
        t1 = t1[cols_800].sort_values(["zipf_s", "scenario", "h3_res"])
        t1_csv = outdir / "table_800_summary.csv"
        t1.to_csv(t1_csv, index=False)
        wrote(t1_csv)

        t1_tex = outdir / "table_800_summary.tex"
        write_minimal_latex_table(t1, t1_tex, cols_800)

    cols_load = [
        "scenario", "h3_res", "target_rps",
        "p50_ms_median", "p95_ms_median", "p99_ms_median",
        "throughput_rps_median", "errors_sum",
        "postgis_cpu_avg_pct_median", "geoserver_cpu_avg_pct_median",
    ]
    t2 = df_agg[(df_agg["zipf_s"] == 1.3) &
                (((df_agg["scenario"] == "baseline") & (df_agg["h3_res"] == 0)) |
                 ((df_agg["scenario"] == "cache") & (df_agg["h3_res"] == 8))) &
                (df_agg["target_rps"].isin([600, 800, 1000]))].copy()
    if t2.empty:
        warn("Tables) No rows for load sensitivity table; skipping table_load_sensitivity.")
    else:
        t2 = t2[cols_load].sort_values(["target_rps", "scenario", "h3_res"])
        t2_csv = outdir / "table_load_sensitivity.csv"
        t2.to_csv(t2_csv, index=False)
        wrote(t2_csv)

        t2_tex = outdir / "table_load_sensitivity.tex"
        write_minimal_latex_table(t2, t2_tex, cols_load)

    t3src = df_agg[(df_agg["target_rps"] == 800) &
                   (df_agg["scenario"] == "cache") &
                   (df_agg["h3_res"].isin([7, 8, 9]))].copy()
    if t3src.empty:
        warn("Tables) No cache rows for best-config table; skipping table_best_config_by_zipf.")
        return

    t3src = t3src.dropna(subset=["zipf_s", "throughput_rps_median", "target_rps"])
    t3src = t3src[t3src["throughput_rps_median"] >= 0.95 * t3src["target_rps"]]
    if t3src.empty:
        warn("Tables) No cache configs sustain load (>=0.95*target); skipping table_best_config_by_zipf.")
        return

    rows = []
    for z in sorted(t3src["zipf_s"].unique()):
        zdf = t3src[t3src["zipf_s"] == z].copy()
        if zdf.empty:
            continue

        def best_for(metric: str) -> Tuple[Optional[int], float]:
            cand = zdf.dropna(subset=[metric])
            if cand.empty:
                return None, float("nan")
            i = cand[metric].idxmin()
            return int(cand.loc[i, "h3_res"]), float(cand.loc[i, metric])

        best_r_p50, best_p50 = best_for("p50_ms_median")
        best_r_p95, best_p95 = best_for("p95_ms_median")
        best_r_p99, best_p99 = best_for("p99_ms_median")

        rows.append({
            "zipf_s": float(z),
            "best_h3_res_p50": best_r_p50 if best_r_p50 is not None else float("nan"),
            "best_p50_ms": best_p50,
            "best_h3_res_p95": best_r_p95 if best_r_p95 is not None else float("nan"),
            "best_p95_ms": best_p95,
            "best_h3_res_p99": best_r_p99 if best_r_p99 is not None else float("nan"),
            "best_p99_ms": best_p99,
        })

    t3 = pd.DataFrame(rows)
    if t3.empty:
        warn("Tables) Best-config table ended up empty; skipping.")
        return

    cols_best = [
        "zipf_s",
        "best_h3_res_p50", "best_p50_ms",
        "best_h3_res_p95", "best_p95_ms",
        "best_h3_res_p99", "best_p99_ms",
    ]
    t3 = t3[cols_best].sort_values(["zipf_s"])

    t3_csv = outdir / "table_best_config_by_zipf.csv"
    t3.to_csv(t3_csv, index=False)
    wrote(t3_csv)

    t3_tex = outdir / "table_best_config_by_zipf.tex"
    write_minimal_latex_table(t3, t3_tex, cols_best)


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    default_dir = script_dir / "aggregated"

    ap = argparse.ArgumentParser(description="Generate thesis plots + tables from runs_long.csv and runs_agg.csv.")
    ap.add_argument("--indir", default=str(default_dir), help="Directory containing runs_long.csv and runs_agg.csv.")
    ap.add_argument("--outdir", default=str(default_dir), help="Directory to write plots and tables into.")
    args = ap.parse_args()

    indir = Path(args.indir)
    outdir = Path(args.outdir)
    ensure_dir(outdir)

    df_long, df_agg = load_csvs(indir)

    plot_latency_vs_zipf(df_agg, df_long, outdir)
    plot_latency_vs_h3res(df_agg, outdir)
    plot_throughput_and_errors(df_agg, df_long, outdir)
    plot_cache_context_proxies(df_agg, outdir)
    plot_backend_load(df_agg, outdir)
    plot_load_sensitivity(df_agg, outdir)

    write_tables(df_agg, df_long, outdir)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

