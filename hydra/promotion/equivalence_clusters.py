from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from typing import Any


def economic_strategy_unit(row: dict[str, Any]) -> str:
    payload = {
        "family": row.get("family"),
        "symbol": row.get("symbol"),
        "lane": row.get("research_lane"),
        "parameter_zone": row.get("parameter_zone"),
        "parent": row.get("parent_candidate_id"),
        "mutation": row.get("mutation_type"),
    }
    return hashlib.sha1(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16]


def build_equivalence_clusters(rows: list[dict[str, Any]]) -> dict[str, list[str]]:
    clusters: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        clusters[economic_strategy_unit(row)].append(str(row["candidate_id"]))
    return dict(clusters)


def cluster_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_cluster = build_equivalence_clusters(rows)
    lookup = {str(row["candidate_id"]): row for row in rows}
    out = []
    for cluster_id, ids in by_cluster.items():
        members = [lookup[i] for i in ids if i in lookup]
        out.append(
            {
                "cluster_id": cluster_id,
                "member_count": len(ids),
                "best_candidate_id": max(members, key=lambda r: float(r.get("promotion_score") or 0.0))["candidate_id"] if members else "",
                "best_promotion_score": max(float(r.get("promotion_score") or 0.0) for r in members) if members else 0.0,
                "families": sorted({str(r.get("family")) for r in members}),
                "symbols": sorted({str(r.get("symbol")) for r in members}),
            }
        )
    return sorted(out, key=lambda r: (r["best_promotion_score"], r["member_count"]), reverse=True)

