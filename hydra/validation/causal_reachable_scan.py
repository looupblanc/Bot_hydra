"""Bounded future-dependency scan for the frozen active-risk execution path.

The scanner is deliberately not a repository-wide keyword search.  Its scope
is an explicit inventory of the feature, sleeve replay, account replay,
active-risk governor, parity, and append-only execution functions reachable by
the six frozen books.  Suspicious syntax is classified only by an explicit
module/function/primitive rule carrying a human-readable rationale.  An
unmatched finding fails closed as ``UNRESOLVED``.

This is a static guard, not a claim that static analysis can prove arbitrary
Python causal.  Its value is that additions to the frozen reachable surface
cannot silently introduce a recognized future-data primitive.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import subprocess
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Mapping, Sequence


SCAN_SCHEMA = "hydra_causal_reachable_future_dependency_scan_v1"
SCAN_PASS = "CAUSAL_REACHABLE_SCAN_PASS"
SCAN_FAILED = "CAUSAL_REACHABLE_SCAN_FAILED"
DEFAULT_PACKAGE_GLOB = (
    "reports/economic_evolution/active_risk_pool_target_velocity_0026_revision_02/"
    "forward_shadow/*/shadow_package.json"
)
DEFAULT_OUTPUT_PATH = Path(
    "reports/operating/hydra_operating_package_v1/"
    "CAUSAL_REACHABLE_FUTURE_DEPENDENCY_SCAN.json"
)
EXPECTED_BOOK_COUNT = 6
EXPECTED_SLEEVE_COUNT = 18


class FindingClassification(StrEnum):
    OUTCOME_LABEL_ONLY = "OUTCOME_LABEL_ONLY"
    KNOWN_CALENDAR_INFORMATION = "KNOWN_CALENDAR_INFORMATION"
    LOOKAHEAD_DEFECT = "LOOKAHEAD_DEFECT"
    UNRESOLVED = "UNRESOLVED"


BLOCKING_CLASSIFICATIONS = {
    FindingClassification.LOOKAHEAD_DEFECT,
    FindingClassification.UNRESOLVED,
}


class CausalReachableScanError(RuntimeError):
    """The bounded scan scope, frozen inventory, or receipt is invalid."""


@dataclass(frozen=True, slots=True)
class ScanScope:
    module_path: str
    functions: tuple[str, ...]
    role: str
    rationale: str

    def __post_init__(self) -> None:
        if not self.module_path.endswith(".py"):
            raise CausalReachableScanError("scan scope must name a Python module")
        if not self.functions or any(not value for value in self.functions):
            raise CausalReachableScanError("scan scope functions must be explicit")
        if len(set(self.functions)) != len(self.functions):
            raise CausalReachableScanError("scan scope functions must be unique")
        if not self.role.strip() or not self.rationale.strip():
            raise CausalReachableScanError("scan scope requires role and rationale")


@dataclass(frozen=True, slots=True)
class ClassificationRule:
    rule_id: str
    module_path: str
    function: str
    primitive: str
    classification: FindingClassification
    rationale: str
    source_pattern: str = ".*"

    def __post_init__(self) -> None:
        if not all(
            value.strip()
            for value in (
                self.rule_id,
                self.module_path,
                self.function,
                self.primitive,
                self.rationale,
                self.source_pattern,
            )
        ):
            raise CausalReachableScanError(
                "classification rules require explicit fields and rationale"
            )
        try:
            re.compile(self.source_pattern)
        except re.error as exc:
            raise CausalReachableScanError("invalid rule source pattern") from exc


DEFAULT_SCOPES = (
    ScanScope(
        "hydra/research/turbo_feature_builder.py",
        ("build_or_open_turbo_feature_bundles", "_market_arrays"),
        "FEATURE_CONSTRUCTION",
        "Builds the exact feature matrices consumed by every frozen sleeve.",
    ),
    ScanScope(
        "hydra/research/causal_sleeve_replay.py",
        (
            "FrozenSleeveDecisionKernel.eligible",
            "CausalSleeveStreamingKernel.step",
            "CausalSleeveStreamingKernel._resolve_open_exit_boundary",
            "CausalSleeveStreamingKernel._resolve_pending_entry_boundary",
            "CausalSleeveStreamingKernel._mark_open_position",
            "CausalSleeveStreamingKernel._complete_open",
            "CausalSleeveStreamingKernel._censor_pending",
            "CausalSleeveStreamingKernel._censor_open",
            "replay_causal_sleeve_batch",
            "replay_causal_sleeve_streaming",
            "iter_causal_bar_records",
            "_completed_trajectory",
            "_censored_trajectory",
            "_signal_evidence",
            "_initial_unrealized",
            "_next_executable_boundary",
            "_record_matches_pending_path",
        ),
        "CAUSAL_SLEEVE_DECISION_AND_OUTCOME",
        "The sole sleeve decision loop and its post-decision causal fill materializer.",
    ),
    ScanScope(
        "hydra/account_policy/active_risk_pool.py",
        (
            "active_risk_utilisation",
            "route_active_risk_pool_entry",
            "_maximum_admissible_declared_risk",
        ),
        "ACTIVE_RISK_GOVERNOR",
        "Makes the frozen entry admission, sizing, and conflict decisions.",
    ),
    ScanScope(
        "hydra/account_policy/causal_active_pool_replay.py",
        (
            "run_causal_shared_account_episode",
            "evaluate_causal_account_policy",
            "_force_liquidate_at_current_bound",
            "_live_equity",
            "_open_unrealized",
        ),
        "CAUSAL_ACTIVE_POOL_ACCOUNT_REPLAY",
        "Advances marks, MLL, governor state and realized PnL in availability order.",
    ),
    ScanScope(
        "hydra/account_policy/router.py",
        ("route_entry", "static_route_entry"),
        "BASE_ACCOUNT_ROUTER",
        "Defines the account-state contract consumed by the active-risk governor.",
    ),
    ScanScope(
        "hydra/propfirm/rolling_combine.py",
        ("select_episode_starts", "evaluate_rolling_combine"),
        "EPISODE_SELECTION_AND_REPLAY",
        "Selects starts and invokes account episodes without retuning.",
    ),
    ScanScope(
        "hydra/propfirm/mll_variants.py",
        ("advance_intraday_floor", "advance_end_of_day_floor"),
        "MLL_FLOOR_RULES",
        "Advances the frozen loss-limit floor from chronological account state.",
    ),
)


def _rule(
    rule_id: str,
    module: str,
    function: str,
    primitive: str,
    classification: FindingClassification,
    rationale: str,
    source_pattern: str = ".*",
) -> ClassificationRule:
    return ClassificationRule(
        rule_id=rule_id,
        module_path=module,
        function=function,
        primitive=primitive,
        classification=classification,
        rationale=rationale,
        source_pattern=source_pattern,
    )


_TURBO = "hydra/research/turbo_feature_builder.py"
_CAUSAL = "hydra/research/causal_sleeve_replay.py"
_ROLLING_EPISODES = "hydra/propfirm/rolling_combine.py"

DEFAULT_RULES = (
    _rule(
        "turbo-next-row-entry-price",
        _TURBO,
        "_market_arrays",
        "NEXT_ROW_EXECUTION_PRICE",
        FindingClassification.OUTCOME_LABEL_ONLY,
        "The legacy matrix column is retained as an outcome oracle but the causal "
        "decision/materialization path never loads it.",
    ),
    _rule(
        "turbo-future-label-shifts",
        _TURBO,
        "_market_arrays",
        "NEGATIVE_SHIFT",
        FindingClassification.OUTCOME_LABEL_ONLY,
        "Exit timestamp/price and continuity shifts construct evaluation labels; "
        "they are not safe decision inputs.",
    ),
    _rule(
        "turbo-forward-label-output",
        _TURBO,
        "_market_arrays",
        "FORWARD_LABEL_REFERENCE",
        FindingClassification.OUTCOME_LABEL_ONLY,
        "The forward move array is an economic outcome label at construction time.",
    ),
    _rule(
        "episode-start-block-edge",
        _ROLLING_EPISODES,
        "select_episode_starts",
        "FORWARD_INDEX_ACCESS",
        FindingClassification.KNOWN_CALENDAR_INFORMATION,
        "The positive offset selects the next boundary in an already materialized "
        "eligible-session partition; it reads no price or outcome.",
    ),
)


def stable_hash(value: Any) -> str:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CausalReachableScanError("scan payload is not canonical JSON") from exc
    return hashlib.sha256(encoded).hexdigest()


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def scan_scoped_sources(
    *,
    repository_root: str | Path,
    scopes: Sequence[ScanScope],
    rules: Sequence[ClassificationRule],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Scan only the explicitly supplied functions and classify every primitive."""

    root = Path(repository_root).resolve()
    _validate_rules(rules)
    findings: list[dict[str, Any]] = []
    coverage: list[dict[str, Any]] = []
    for scope in scopes:
        source_path = _inside(root, scope.module_path)
        source = source_path.read_text(encoding="utf-8")
        try:
            tree = ast.parse(source, filename=scope.module_path)
        except SyntaxError as exc:
            raise CausalReachableScanError(
                f"cannot parse reachable module: {scope.module_path}"
            ) from exc
        definitions = _definition_map(tree)
        missing = sorted(set(scope.functions) - set(definitions))
        if missing:
            raise CausalReachableScanError(
                f"reachable functions absent from {scope.module_path}: {missing}"
            )
        module_findings: list[dict[str, Any]] = []
        lines = source.splitlines()
        for function in scope.functions:
            raw = _detect_primitives(
                module_path=scope.module_path,
                function=function,
                node=definitions[function],
                source=source,
                lines=lines,
            )
            for finding in raw:
                rule = _matching_rule(finding, rules)
                if rule is None:
                    finding.update(
                        {
                            "classification": FindingClassification.UNRESOLVED.value,
                            "rule_id": None,
                            "rationale": (
                                "No explicit module/function/primitive rule classifies "
                                "this reachable future-dependency syntax."
                            ),
                        }
                    )
                else:
                    finding.update(
                        {
                            "classification": rule.classification.value,
                            "rule_id": rule.rule_id,
                            "rationale": rule.rationale,
                        }
                    )
                module_findings.append(finding)
        findings.extend(module_findings)
        coverage.append(
            {
                "module_path": scope.module_path,
                "module_sha256": sha256_file(source_path),
                "role": scope.role,
                "rationale": scope.rationale,
                "functions": list(scope.functions),
                "finding_count": len(module_findings),
            }
        )
    findings.sort(
        key=lambda row: (
            row["module_path"],
            row["function"],
            int(row["line"]),
            row["primitive"],
            row["source"],
        )
    )
    return findings, coverage


def run_causal_reachable_scan(
    *,
    repository_root: str | Path,
    package_glob: str = DEFAULT_PACKAGE_GLOB,
    scopes: Sequence[ScanScope] = DEFAULT_SCOPES,
    rules: Sequence[ClassificationRule] = DEFAULT_RULES,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    """Audit the six-book reachable surface and return a stable-hashed receipt."""

    root = Path(repository_root).resolve()
    inventory = _frozen_inventory(root, package_glob)
    findings, coverage = scan_scoped_sources(
        repository_root=root,
        scopes=scopes,
        rules=rules,
    )
    counts = Counter(row["classification"] for row in findings)
    blocking = [
        row
        for row in findings
        if FindingClassification(row["classification"]) in BLOCKING_CLASSIFICATIONS
    ]
    timestamp = (created_at or datetime.now(UTC)).astimezone(UTC)
    receipt: dict[str, Any] = {
        "schema": SCAN_SCHEMA,
        "status": SCAN_FAILED if blocking else SCAN_PASS,
        "created_at_utc": timestamp.isoformat().replace("+00:00", "Z"),
        "git_commit": _git_head(root),
        "scope_policy": {
            "kind": "EXPLICIT_REACHABLE_FUNCTION_ALLOWLIST",
            "repository_wide_scan": False,
            "unrelated_modules_scanned": 0,
            "rule_requirement": "EXACT_MODULE_FUNCTION_PRIMITIVE_WITH_RATIONALE",
            "blocking_classifications": sorted(
                value.value for value in BLOCKING_CLASSIFICATIONS
            ),
        },
        "frozen_inventory": inventory,
        "coverage": coverage,
        "rules": [
            {
                **asdict(rule),
                "classification": rule.classification.value,
            }
            for rule in rules
        ],
        "findings": findings,
        "classification_counts": {
            value.value: int(counts.get(value.value, 0))
            for value in FindingClassification
        },
        "blocking_finding_count": len(blocking),
        "blocking_findings": blocking,
        "safety": {
            "market_data_files_read": 0,
            "post_freeze_bars_read": 0,
            "engine_files_modified": 0,
            "book_mutations": 0,
            "orders": 0,
            "broker_connections": 0,
            "q4_access_count": 0,
            "data_purchase_usd": 0.0,
        },
    }
    receipt["scan_hash"] = stable_hash(receipt)
    verify_causal_reachable_scan(receipt)
    return receipt


def write_causal_reachable_scan(
    *,
    repository_root: str | Path,
    output_path: str | Path = DEFAULT_OUTPUT_PATH,
    package_glob: str = DEFAULT_PACKAGE_GLOB,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    root = Path(repository_root).resolve()
    output = _output_path(root, output_path)
    receipt = run_causal_reachable_scan(
        repository_root=root,
        package_glob=package_glob,
        created_at=created_at,
    )
    _atomic_json(output, receipt)
    return receipt


def verify_causal_reachable_scan(
    receipt_or_path: Mapping[str, Any] | str | Path,
) -> dict[str, Any]:
    receipt = (
        dict(receipt_or_path)
        if isinstance(receipt_or_path, Mapping)
        else _json(Path(receipt_or_path))
    )
    expected = str(receipt.get("scan_hash") or "")
    unhashed = dict(receipt)
    unhashed.pop("scan_hash", None)
    if not expected or expected != stable_hash(unhashed):
        raise CausalReachableScanError("causal reachable scan hash drift")
    if receipt.get("schema") != SCAN_SCHEMA:
        raise CausalReachableScanError("causal reachable scan schema drift")
    findings = receipt.get("findings") or []
    blocking = [
        row
        for row in findings
        if row.get("classification")
        in {value.value for value in BLOCKING_CLASSIFICATIONS}
    ]
    if int(receipt.get("blocking_finding_count", -1)) != len(blocking):
        raise CausalReachableScanError("blocking finding count drift")
    expected_status = SCAN_FAILED if blocking else SCAN_PASS
    if receipt.get("status") != expected_status:
        raise CausalReachableScanError("causal reachable scan status drift")
    if any(
        not row.get("rationale")
        or row.get("classification") not in {value.value for value in FindingClassification}
        for row in findings
    ):
        raise CausalReachableScanError("unclassified finding or missing rationale")
    inventory = receipt.get("frozen_inventory") or {}
    if int(inventory.get("book_count", -1)) != EXPECTED_BOOK_COUNT:
        raise CausalReachableScanError("frozen book count drift")
    if int(inventory.get("sleeve_count", -1)) != EXPECTED_SLEEVE_COUNT:
        raise CausalReachableScanError("frozen sleeve count drift")
    scope_policy = receipt.get("scope_policy") or {}
    if scope_policy.get("repository_wide_scan") is not False:
        raise CausalReachableScanError("scan escaped its bounded scope")
    return receipt


def _definition_map(tree: ast.AST) -> dict[str, ast.AST]:
    definitions: dict[str, ast.AST] = {}

    class Collector(ast.NodeVisitor):
        def __init__(self) -> None:
            self.classes: list[str] = []

        def visit_ClassDef(self, node: ast.ClassDef) -> None:
            self.classes.append(node.name)
            self.generic_visit(node)
            self.classes.pop()

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            name = ".".join((*self.classes, node.name))
            definitions[name] = node
            self.generic_visit(node)

        visit_AsyncFunctionDef = visit_FunctionDef

    Collector().visit(tree)
    return definitions


def _detect_primitives(
    *,
    module_path: str,
    function: str,
    node: ast.AST,
    source: str,
    lines: Sequence[str],
) -> list[dict[str, Any]]:
    parents = {child: parent for parent in ast.walk(node) for child in ast.iter_child_nodes(parent)}
    output: list[dict[str, Any]] = []
    seen: set[tuple[int, int, str, str]] = set()

    def add(raw: ast.AST, primitive: str) -> None:
        segment = ast.get_source_segment(source, raw) or lines[raw.lineno - 1].strip()
        key = (int(raw.lineno), int(raw.col_offset), primitive, segment)
        if key in seen:
            return
        seen.add(key)
        output.append(
            {
                "module_path": module_path,
                "function": function,
                "line": int(raw.lineno),
                "column": int(raw.col_offset),
                "primitive": primitive,
                "source": " ".join(segment.split()),
            }
        )

    for raw in ast.walk(node):
        if isinstance(raw, (ast.Assign, ast.AnnAssign)):
            value = raw.value
            if isinstance(value, ast.Call) and _negative_shift(value):
                targets = raw.targets if isinstance(raw, ast.Assign) else [raw.target]
                target_names = {_target_name(target) for target in targets}
                add(
                    raw,
                    "NEXT_ROW_EXECUTION_PRICE"
                    if "entry_price" in target_names
                    else "NEGATIVE_SHIFT",
                )
        if isinstance(raw, ast.Call) and _negative_shift(raw):
            parent = parents.get(raw)
            if not isinstance(parent, (ast.Assign, ast.AnnAssign)):
                add(raw, "NEGATIVE_SHIFT")
        if isinstance(raw, ast.Call) and _is_future_eligibility_guard(raw):
            add(raw, "FUTURE_ELIGIBILITY_GUARD")
        if isinstance(raw, ast.Call) and _call_references_forward_label(raw):
            add(raw, "FORWARD_LABEL_REFERENCE")
        if isinstance(raw, ast.Subscript) and _subscript_references_forward_label(raw):
            add(raw, "FORWARD_LABEL_REFERENCE")
        if isinstance(raw, ast.Subscript) and _subscript_key(raw) == "horizon_available":
            add(raw, "HORIZON_AVAILABILITY_REFERENCE")
        if isinstance(raw, ast.Name) and isinstance(raw.ctx, ast.Load) and raw.id == "forward":
            add(raw, "FUTURE_VALUE_REFERENCE")
        if isinstance(raw, ast.Subscript) and _positive_offset_index(raw.slice):
            add(raw, "FORWARD_INDEX_ACCESS")
    return output


def _matching_rule(
    finding: Mapping[str, Any], rules: Sequence[ClassificationRule]
) -> ClassificationRule | None:
    matches = [
        rule
        for rule in rules
        if rule.module_path == finding["module_path"]
        and rule.function == finding["function"]
        and rule.primitive == finding["primitive"]
        and re.search(rule.source_pattern, str(finding["source"]))
    ]
    if len(matches) > 1:
        raise CausalReachableScanError(
            f"overlapping classification rules for {finding['module_path']}:"
            f"{finding['function']}:{finding['primitive']}"
        )
    return matches[0] if matches else None


def _validate_rules(rules: Sequence[ClassificationRule]) -> None:
    ids = [rule.rule_id for rule in rules]
    if len(ids) != len(set(ids)):
        raise CausalReachableScanError("classification rule IDs must be unique")


def _negative_shift(node: ast.Call) -> bool:
    return (
        isinstance(node.func, ast.Attribute)
        and node.func.attr == "shift"
        and bool(node.args)
        and _is_negative_expression(node.args[0])
    )


def _is_negative_expression(node: ast.AST) -> bool:
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return node.value < 0
    return isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub)


def _is_future_eligibility_guard(node: ast.Call) -> bool:
    return (
        isinstance(node.func, ast.Attribute)
        and node.func.attr == "isfinite"
        and bool(node.args)
        and isinstance(node.args[0], ast.Name)
        and node.args[0].id == "forward"
    )


def _call_references_forward_label(node: ast.Call) -> bool:
    if not (
        isinstance(node.func, ast.Attribute)
        and node.func.attr == "array"
        and node.args
    ):
        return False
    return "forward_move__" in ast.unparse(node.args[0])


def _subscript_key(node: ast.Subscript) -> str | None:
    if isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str):
        return node.slice.value
    return None


def _subscript_references_forward_label(node: ast.Subscript) -> bool:
    return "forward_move__" in ast.unparse(node.slice)


def _positive_offset_index(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.BinOp)
        and isinstance(node.op, ast.Add)
        and isinstance(node.right, ast.Constant)
        and isinstance(node.right.value, int)
        and node.right.value > 0
    )


def _target_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, (ast.Tuple, ast.List)):
        return ",".join(_target_name(value) for value in node.elts)
    return ast.unparse(node)


def _frozen_inventory(root: Path, package_glob: str) -> dict[str, Any]:
    package_paths = sorted(root.glob(package_glob))
    if len(package_paths) != EXPECTED_BOOK_COUNT:
        raise CausalReachableScanError("expected exactly six frozen package files")
    packages: list[dict[str, Any]] = []
    common_sleeves: set[str] | None = None
    for path in package_paths:
        package = _json(path)
        _verify_embedded_hash(package, "package_hash", "shadow package")
        sleeves = set(
            ((package.get("signal_policy") or {}).get("signal_ledger_sha256") or {})
        )
        if len(sleeves) != EXPECTED_SLEEVE_COUNT:
            raise CausalReachableScanError("package does not bind all 18 sleeves")
        if common_sleeves is None:
            common_sleeves = sleeves
        elif common_sleeves != sleeves:
            raise CausalReachableScanError("frozen package sleeve inventory drift")
        packages.append(
            {
                "candidate_id": str(package["candidate_id"]),
                "path": path.resolve().relative_to(root).as_posix(),
                "file_sha256": sha256_file(path),
                "package_hash": str(package["package_hash"]),
                "freeze_timestamp_utc": str(package["freeze_timestamp_utc"]),
            }
        )
    return {
        "book_count": len(packages),
        "sleeve_count": len(common_sleeves or ()),
        "sleeve_ids": sorted(common_sleeves or ()),
        "packages": packages,
    }


def _verify_embedded_hash(payload: Mapping[str, Any], field: str, label: str) -> None:
    expected = str(payload.get(field) or "")
    unhashed = dict(payload)
    unhashed.pop(field, None)
    if not expected or expected != stable_hash(unhashed):
        raise CausalReachableScanError(f"{label} hash drift")


def _git_head(root: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise CausalReachableScanError("cannot resolve Git HEAD") from exc
    return result.stdout.strip()


def _inside(root: Path, raw: str | Path) -> Path:
    path = (root / raw).resolve() if not Path(raw).is_absolute() else Path(raw).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise CausalReachableScanError("scan path escapes repository root") from exc
    if not path.is_file():
        raise CausalReachableScanError(f"reachable source is absent: {path}")
    return path


def _output_path(root: Path, raw: str | Path) -> Path:
    path = (root / raw).resolve() if not Path(raw).is_absolute() else Path(raw).resolve()
    allowed = (root / "reports/operating").resolve()
    try:
        path.relative_to(allowed)
    except ValueError as exc:
        raise CausalReachableScanError(
            "scan output must remain under reports/operating"
        ) from exc
    return path


def _json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CausalReachableScanError(f"cannot read JSON object: {path}") from exc
    if not isinstance(value, dict):
        raise CausalReachableScanError(f"JSON object required: {path}")
    return value


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(
                value,
                handle,
                indent=2,
                sort_keys=True,
                ensure_ascii=True,
                allow_nan=False,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


__all__ = [
    "BLOCKING_CLASSIFICATIONS",
    "CausalReachableScanError",
    "ClassificationRule",
    "DEFAULT_OUTPUT_PATH",
    "DEFAULT_RULES",
    "DEFAULT_SCOPES",
    "FindingClassification",
    "SCAN_FAILED",
    "SCAN_PASS",
    "ScanScope",
    "run_causal_reachable_scan",
    "scan_scoped_sources",
    "stable_hash",
    "verify_causal_reachable_scan",
    "write_causal_reachable_scan",
]
