#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from hydra.governance.cohort_authorization import issue_cohort_authorization
from hydra.governance.invariants import governance_semantic_hash


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Issue one manifest-bound Q4 token; the controller normally performs this action."
    )
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--access-ledger", default="reports/data_access/data_access_ledger.jsonl")
    parser.add_argument("--authorization-root", default="mission/state/q4_one_shot")
    parser.add_argument("--token-output", required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    manifest_path = Path(args.manifest)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    summary = {
        "cohort_id": manifest.get("cohort_id"),
        "candidate_count": manifest.get("candidate_count"),
        "manifest_hash": manifest.get("manifest_hash"),
        "source_commit": manifest.get("source_commit"),
        "q4_access_count_before": manifest.get("q4_access_count_before"),
        "execute": bool(args.execute),
    }
    if not args.execute:
        print(json.dumps({**summary, "status": "VALIDATION_ONLY_NO_TOKEN_ISSUED"}, indent=2, sort_keys=True))
        return 0
    governance_yaml = Path("config/governance/hydra_governance_v1.yaml")
    issued = issue_cohort_authorization(
        cohort_manifest_path=manifest_path,
        cohort_manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        cohort_manifest_hash=str(manifest["manifest_hash"]),
        source_commit=str(manifest["source_commit"]),
        governance_semantic_hash=governance_semantic_hash(),
        governance_yaml_sha256=hashlib.sha256(governance_yaml.read_bytes()).hexdigest(),
        authorization_root=args.authorization_root,
        access_ledger_path=args.access_ledger,
    )
    token_path = Path(args.token_output)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(issued.token + "\n", encoding="utf-8")
    token_path.chmod(0o600)
    print(
        json.dumps(
            {
                **summary,
                "status": "AUTHORIZED_SINGLE_USE",
                "token_id": issued.token_id,
                "authorization_path": issued.authorization_path,
                "authorization_hash": issued.authorization_hash,
                "token_output": str(token_path),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
