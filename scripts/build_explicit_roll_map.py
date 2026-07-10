#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.contract_mapping import build_rule_based_roll_map, write_roll_map
from hydra.data.databento_loader import load_cached_ohlcv, request_from_config
from hydra.data.roll_audit import audit_roll_discontinuities, synchronized_pair_audit
from hydra.utils.config import load_config, project_path
from hydra.utils.time import utc_now_iso


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a roll-aware contract map for cached Databento futures data.")
    parser.add_argument("--dataset", default="GLBX.MDP3")
    parser.add_argument("--schema", default="ohlcv-1m")
    parser.add_argument("--symbols", nargs="+", default=["ES", "MES", "NQ", "MNQ"])
    parser.add_argument("--start", default="2024-01-01")
    parser.add_argument("--end", default="2024-07-01")
    parser.add_argument("--report-tag", default="q1_q2_roll_map_v1")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    roll_map = build_rule_based_roll_map(args.symbols, start=args.start, end=args.end, dataset=args.dataset, schema=args.schema)
    map_path, digest = write_roll_map(roll_map)
    data = load_period(args)
    audit = audit_roll_discontinuities(data, roll_map)
    timestamps = list(pd.to_datetime(data[data["symbol"].isin(["NQ", "ES"])]["timestamp"], utc=True).sample(min(200, len(data)), random_state=17)) if len(data) else []
    pair_audit = synchronized_pair_audit(roll_map, timestamps, pair=("NQ", "ES"))
    summary = {
        "created_at": utc_now_iso(),
        "map_path": str(map_path),
        "roll_map_hash": digest,
        "map_status": "RULE_BASED_PROXY_EXPLICIT_METADATA_MISSING",
        "new_databento_request": False,
        "databento_spend": 0.0,
        "audit": audit,
        "nq_es_pair_audit": pair_audit,
        "notes": [
            "This map is reproducible and roll-aware but not official raw-contract metadata.",
            "It is sufficient to flag roll-sensitive candidates, not to grant final promotion.",
        ],
    }
    report_dir = project_path("reports", "roll_audit")
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"roll_audit_{utc_now_iso().replace('-', '').replace(':', '').replace('+00:00', 'Z')}_{args.report_tag}.md"
    report_path.write_text(to_markdown(summary), encoding="utf-8")
    print(json.dumps({"report_path": str(report_path), **summary}, indent=2, sort_keys=True, default=str))
    return 0


def load_period(args: argparse.Namespace) -> pd.DataFrame:
    cfg = load_config()
    frames = []
    for start, end in [("2024-01-01", "2024-03-31"), ("2024-04-01", "2024-07-01")]:
        if pd.Timestamp(end, tz="UTC") <= pd.Timestamp(args.start, tz="UTC") or pd.Timestamp(start, tz="UTC") >= pd.Timestamp(args.end, tz="UTC"):
            continue
        request = request_from_config(cfg, symbols=args.symbols, start=start, end=end, schema=args.schema, dataset=args.dataset)
        path = Path(request.output_path)
        if path.exists():
            frames.append(load_cached_ohlcv(path, timeframe=request.timeframe))
    if not frames:
        return pd.DataFrame()
    raw = pd.concat(frames, ignore_index=True)
    ts = pd.to_datetime(raw["timestamp"], utc=True)
    return raw[(ts >= pd.Timestamp(args.start, tz="UTC")) & (ts < pd.Timestamp(args.end, tz="UTC"))].reset_index(drop=True)


def to_markdown(summary: dict) -> str:
    return "\n".join(
        [
            "# Roll Audit",
            "",
            "This is a roll-aware validation artifact, not live trading approval.",
            "",
            "```json",
            json.dumps(summary, indent=2, sort_keys=True, default=str),
            "```",
            "",
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())
