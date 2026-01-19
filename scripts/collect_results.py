#!/usr/bin/env python3
"""
Aggregate experiment results from multiple roots into:
- runs_long.csv : one row per rep folder
- runs_agg.csv  : aggregated (median/sum) per configuration

Run from root:
python3 scripts/collect_results.py \
  --outdir scripts/aggregated/
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional, Tuple


def warn(msg: str) -> None:
    print(f"[WARN] {msg}", file=sys.stderr)


def safe_float(x: Any) -> float:
    try:
        if x is None:
            return float("nan")
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s == "":
            return float("nan")
        return float(s)
    except Exception:
        return float("nan")


def safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        if isinstance(x, int):
            return x
        s = str(x).strip()
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default


def median_ignore_nan(values: Iterable[Any]) -> float:
    nums: List[float] = []
    for v in values:
        f = safe_float(v)
        if not math.isnan(f):
            nums.append(f)
    return median(nums) if nums else float("nan")


def parse_cpu_percent(s: str) -> float:
    try:
        s = (s or "").strip()
        if s.endswith("%"):
            s = s[:-1].strip()
        return float(s)
    except Exception:
        return float("nan")


_MEM_RE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([A-Za-z]+)\s*$")


def mem_to_mib(mem_used_part: str) -> float:
    try:
        s = (mem_used_part or "").strip()
        m = _MEM_RE.match(s)
        if not m:
            return float("nan")
        val = float(m.group(1))
        unit = m.group(2)

        if unit == "B":
            return val / (1024.0 * 1024.0)
        if unit == "KiB":
            return val / 1024.0
        if unit == "MiB":
            return val
        if unit == "GiB":
            return val * 1024.0
        if unit == "TiB":
            return val * 1024.0 * 1024.0

        if unit == "KB":
            return (val * 1000.0) / (1024.0 * 1024.0)
        if unit == "MB":
            return (val * 1000.0 * 1000.0) / (1024.0 * 1024.0)
        if unit == "GB":
            return (val * 1000.0 * 1000.0 * 1000.0) / (1024.0 * 1024.0)

        return float("nan")
    except Exception:
        return float("nan")


def parse_mem_usage_to_mib(mem_usage: str) -> float:
    try:
        left = (mem_usage or "").split("/", 1)[0].strip()
        return mem_to_mib(left)
    except Exception:
        return float("nan")


def parse_iso_dt(ts: str) -> datetime:
    s = (ts or "").strip()
    if not s:
        raise ValueError("empty timestamp")

    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    m = re.match(r"^(.*?)(\.\d+)?([+-]\d{2}:\d{2})$", s)
    if m:
        base, frac, tz = m.group(1), m.group(2), m.group(3)
        if frac:
            digits = frac[1:]  # drop leading '.'
            digits = (digits + "000000")[:6]  # pad/truncate to microseconds
            s = f"{base}.{digits}{tz}"
        else:
            s = f"{base}{tz}"
        return datetime.fromisoformat(s)

    if "." in s:
        base, rest = s.split(".", 1)
        digits_m = re.match(r"(\d+)(.*)$", rest)
        if digits_m:
            digits = digits_m.group(1)
            tail = digits_m.group(2)
            digits = (digits + "000000")[:6]
            s = f"{base}.{digits}{tail}"
    return datetime.fromisoformat(s)


@dataclass
class BundleMeta:
    scenario: str
    h3_res: int
    ttl: str
    hot: str
    invalidation: str
    zipf_s_from_folder: float


def parse_bundle_meta(bundle_name: str) -> BundleMeta:
    name = bundle_name.strip()

    scenario = "unknown"
    if name.startswith("baseline-"):
        scenario = "baseline"
    elif name.startswith("cache-"):
        scenario = "cache"
    else:
        if "baseline" in name:
            scenario = "baseline"
        elif "cache" in name:
            scenario = "cache"

    m_r = re.search(r"-r(\d+)-", name)
    h3_res = int(m_r.group(1)) if m_r else 0

    m_ttl = re.search(r"-ttl([^-]+)-", name)
    ttl = m_ttl.group(1) if m_ttl else ""

    m_hot = re.search(r"-hot([^-]+)-", name)
    hot = m_hot.group(1) if m_hot else ""

    m_inv = re.search(r"-inv([^-]+)-", name)
    invalidation = m_inv.group(1) if m_inv else ""

    m_zipf = re.search(r"-zipfs([0-9]+(?:\.[0-9]+)?)", name)
    zipf_s_from_folder = float(m_zipf.group(1)) if m_zipf else float("nan")

    return BundleMeta(
        scenario=scenario,
        h3_res=h3_res,
        ttl=ttl,
        hot=hot,
        invalidation=invalidation,
        zipf_s_from_folder=zipf_s_from_folder,
    )


def parse_rep_int(rep_dir_name: str) -> Optional[int]:
    m = re.match(r"^rep(\d+)$", rep_dir_name)
    if not m:
        return None
    return int(m.group(1))


def compute_docker_averages(
    docker_csv_path: Path,
    start_iso: Optional[str],
    end_iso: Optional[str],
) -> Dict[str, float]:
    out = {
        "postgis_cpu_avg_pct": float("nan"),
        "geoserver_cpu_avg_pct": float("nan"),
        "postgis_mem_avg_mib": float("nan"),
        "geoserver_mem_avg_mib": float("nan"),
    }

    start_dt: Optional[datetime] = None
    end_dt: Optional[datetime] = None
    if start_iso and end_iso:
        try:
            start_dt = parse_iso_dt(start_iso)
            end_dt = parse_iso_dt(end_iso)
        except Exception as e:
            warn(f"{docker_csv_path}: failed to parse start/end timestamps; using all docker rows ({e})")
            start_dt = None
            end_dt = None

    cpu_vals: Dict[str, List[float]] = {"postgis": [], "geoserver": []}
    mem_vals: Dict[str, List[float]] = {"postgis": [], "geoserver": []}

    try:
        with docker_csv_path.open("r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    ts_s = (row.get("ts") or "").strip()
                    container = (row.get("container") or "").strip().lower()
                    if container not in cpu_vals:
                        continue

                    try:
                        ts_dt = parse_iso_dt(ts_s)
                    except Exception:
                        continue

                    if start_dt is not None and end_dt is not None:
                        if ts_dt < start_dt or ts_dt > end_dt:
                            continue

                    cpu_vals[container].append(parse_cpu_percent(row.get("cpu_perc", "")))
                    mem_vals[container].append(parse_mem_usage_to_mib(row.get("mem_usage", "")))
                except Exception:
                    continue
    except Exception as e:
        warn(f"{docker_csv_path}: failed to read docker_stats.csv ({e})")
        return out

    def avg(xs: List[float]) -> float:
        ys = [x for x in xs if not math.isnan(x)]
        return sum(ys) / len(ys) if ys else float("nan")

    out["postgis_cpu_avg_pct"] = avg(cpu_vals["postgis"])
    out["geoserver_cpu_avg_pct"] = avg(cpu_vals["geoserver"])
    out["postgis_mem_avg_mib"] = avg(mem_vals["postgis"])
    out["geoserver_mem_avg_mib"] = avg(mem_vals["geoserver"])
    return out


RUNS_LONG_COLUMNS: List[str] = [
    "root_id",
    "bundle_name",
    "rep",
    "scenario",
    "h3_res",
    "ttl",
    "hot",
    "invalidation",
    "zipf_s_from_folder",
    "start",
    "end",
    "duration_sec",
    "total",
    "success",
    "errors",
    "throughput_rps",
    "target_rps",
    "achieved_to_target_ratio",
    "missed_tokens",
    "max_backlog",
    "token_buffer",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "concurrency",
    "zipf_s",
    "zipf_v",
    "bboxes",
    "layer",
    "target",
    "seed",
    "postgis_cpu_avg_pct",
    "geoserver_cpu_avg_pct",
    "postgis_mem_avg_mib",
    "geoserver_mem_avg_mib",
]

RUNS_AGG_COLUMNS: List[str] = [
    "scenario",
    "h3_res",
    "zipf_s",
    "target_rps",
    "ttl",
    "hot",
    "invalidation",
    "p50_ms_median",
    "p95_ms_median",
    "p99_ms_median",
    "throughput_rps_median",
    "errors_sum",
    "postgis_cpu_avg_pct_median",
    "geoserver_cpu_avg_pct_median",
    "postgis_mem_avg_mib_median",
    "geoserver_mem_avg_mib_median",
    "n_reps",
]


def load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        warn(f"{path}: failed to read/parse JSON ({e})")
        return None


def collect_runs(roots: List[Path]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for root in roots:
        if not root.exists() or not root.is_dir():
            warn(f"Root not found or not a directory: {root}")
            continue

        root_id = root.name

        for bundle_dir in sorted(root.iterdir()):
            if not bundle_dir.is_dir():
                continue

            bundle_name = bundle_dir.name
            meta = parse_bundle_meta(bundle_name)

            rep_dirs = [p for p in bundle_dir.iterdir() if p.is_dir()]
            for rep_dir in sorted(rep_dirs):
                rep_num = parse_rep_int(rep_dir.name)
                if rep_num is None:
                    continue

                scenario = meta.scenario
                summary_path = rep_dir / f"{scenario}_summary.json"
                docker_path = rep_dir / "docker_stats.csv"

                if not summary_path.exists():
                    warn(f"Missing summary JSON, skipping rep: {summary_path}")
                    continue
                if not docker_path.exists():
                    warn(f"Missing docker_stats.csv, skipping rep: {docker_path}")
                    continue

                summary = load_json(summary_path)
                if not summary:
                    warn(f"Empty/invalid summary JSON, skipping rep: {summary_path}")
                    continue

                row: Dict[str, Any] = {c: "" for c in RUNS_LONG_COLUMNS}

                row["root_id"] = root_id
                row["bundle_name"] = bundle_name
                row["rep"] = rep_num
                row["scenario"] = scenario
                row["h3_res"] = meta.h3_res
                row["ttl"] = meta.ttl
                row["hot"] = meta.hot
                row["invalidation"] = meta.invalidation
                row["zipf_s_from_folder"] = meta.zipf_s_from_folder

                for k in [
                    "start", "end", "duration_sec",
                    "total", "success", "errors",
                    "throughput_rps", "target_rps", "achieved_to_target_ratio",
                    "missed_tokens", "max_backlog", "token_buffer",
                    "p50_ms", "p95_ms", "p99_ms",
                    "concurrency",
                    "zipf_s", "zipf_v",
                    "bboxes",
                    "layer", "target",
                    "seed",
                ]:
                    if k in summary:
                        row[k] = summary.get(k)

                if row.get("achieved_to_target_ratio", "") in ("", None):
                    thr = safe_float(row.get("throughput_rps"))
                    tgt = safe_float(row.get("target_rps"))
                    row["achieved_to_target_ratio"] = (thr / tgt) if (tgt and not math.isnan(tgt) and tgt != 0) else float("nan")

                if row.get("zipf_s", "") in ("", None):
                    row["zipf_s"] = meta.zipf_s_from_folder

                docker_avgs = compute_docker_averages(
                    docker_csv_path=docker_path,
                    start_iso=str(row.get("start") or "") or None,
                    end_iso=str(row.get("end") or "") or None,
                )
                row.update(docker_avgs)

                rows.append(row)

    return rows


def write_csv(path: Path, columns: List[str], rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            out_row: Dict[str, Any] = {}
            for c in columns:
                v = r.get(c, "")
                if isinstance(v, float) and math.isnan(v):
                    out_row[c] = "nan"
                else:
                    out_row[c] = v
            w.writerow(out_row)


def aggregate_runs(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = defaultdict(list)

    for r in rows:
        key = (
            r.get("scenario"),
            safe_int(r.get("h3_res"), default=0),
            safe_float(r.get("zipf_s")),
            safe_int(r.get("target_rps"), default=0),
            str(r.get("ttl") or ""),
            str(r.get("hot") or ""),
            str(r.get("invalidation") or ""),
        )
        groups[key].append(r)

    agg_rows: List[Dict[str, Any]] = []

    for key, rs in sorted(groups.items(), key=lambda kv: kv[0]):
        scenario, h3_res, zipf_s, target_rps, ttl, hot, invalidation = key

        agg: Dict[str, Any] = {}
        agg["scenario"] = scenario
        agg["h3_res"] = h3_res
        agg["zipf_s"] = zipf_s
        agg["target_rps"] = target_rps
        agg["ttl"] = ttl
        agg["hot"] = hot
        agg["invalidation"] = invalidation

        agg["p50_ms_median"] = median_ignore_nan(r.get("p50_ms") for r in rs)
        agg["p95_ms_median"] = median_ignore_nan(r.get("p95_ms") for r in rs)
        agg["p99_ms_median"] = median_ignore_nan(r.get("p99_ms") for r in rs)
        agg["throughput_rps_median"] = median_ignore_nan(r.get("throughput_rps") for r in rs)

        agg["errors_sum"] = sum(safe_int(r.get("errors"), default=0) for r in rs)

        agg["postgis_cpu_avg_pct_median"] = median_ignore_nan(r.get("postgis_cpu_avg_pct") for r in rs)
        agg["geoserver_cpu_avg_pct_median"] = median_ignore_nan(r.get("geoserver_cpu_avg_pct") for r in rs)
        agg["postgis_mem_avg_mib_median"] = median_ignore_nan(r.get("postgis_mem_avg_mib") for r in rs)
        agg["geoserver_mem_avg_mib_median"] = median_ignore_nan(r.get("geoserver_mem_avg_mib") for r in rs)

        agg["n_reps"] = len(rs)
        agg_rows.append(agg)

    return agg_rows


def main() -> int:
    default_roots = [
        "results/20260118_210726Z",
        "results/20260119_080715Z",
        "results/20260119_081312Z",
    ]

    ap = argparse.ArgumentParser(description="Collect experiment results into analysis-ready CSVs.")
    ap.add_argument(
        "--roots",
        nargs="+",
        default=default_roots,
        help="One or more result root directories.",
    )
    ap.add_argument(
        "--outdir",
        default="results/aggregated/",
        help="Output directory for aggregated CSVs.",
    )
    args = ap.parse_args()

    roots = [Path(r) for r in args.roots]
    outdir = Path(args.outdir)

    runs_long = collect_runs(roots)
    if not runs_long:
        warn("No runs collected. Check your --roots paths.")
    long_path = outdir / "runs_long.csv"
    write_csv(long_path, RUNS_LONG_COLUMNS, runs_long)

    runs_agg = aggregate_runs(runs_long)
    agg_path = outdir / "runs_agg.csv"
    write_csv(agg_path, RUNS_AGG_COLUMNS, runs_agg)

    print(f"Wrote: {long_path}")
    print(f"Wrote: {agg_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

