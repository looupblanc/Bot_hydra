from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from hydra.mission.mission_state import get_kv
from hydra.mission.v7_falsification_controller import (
    V7ControllerIntegrityError,
    V7FalsificationController,
)


BOOK_IDS = (
    "active_pool_014dffb40e99814612d78c51",
    "active_pool_070e391c7586ba1fac2f5494",
    "active_pool_14e275fa8d869c28b1f27f78",
    "active_pool_186a4177401aab223b0a21fa",
    "active_pool_2287bfb0b1c6f07930150102",
    "active_pool_2377af7025aadf9aaf456a7e",
)
MANIFEST_HASH = "a" * 64
RECEIPT_HASH = "b" * 64


def _connection() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT NOT NULL, "
        "updated_at TEXT NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "event_type TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL)"
    )
    return conn


def _controller_tree(root: Path) -> SimpleNamespace:
    package_dir = root / "reports/operating/hydra_operating_package_v1"
    package_dir.mkdir(parents=True)
    (package_dir / "OPERATING_PACKAGE_V1.json").write_text(
        json.dumps(
            {
                "manifest_hash": MANIFEST_HASH,
                "books": [
                    {"policy_id": policy_id}
                    for policy_id in reversed(BOOK_IDS)
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (package_dir / "OPERATING_PACKAGE_V1_seal_receipt.json").write_text(
        "{}\n", encoding="utf-8"
    )
    state_dir = root / "mission/state"
    receipt = (
        state_dir
        / "operating_package_v1_parity"
        / "f0_single_source_engine_parity_receipt.json"
    )
    receipt.parent.mkdir(parents=True)
    receipt.write_text("{}\n", encoding="utf-8")
    return SimpleNamespace(root=root, paths=SimpleNamespace(state_dir=state_dir))


def test_terminal_contamination_gate_precedes_feed_and_processor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller = _controller_tree(tmp_path)
    conn = _connection()
    observed_verifier: dict[str, object] = {}

    monkeypatch.setattr(
        "hydra.operating.package_v1.verify_operating_package_seal",
        lambda *_args, **_kwargs: {},
    )

    def verify(path: Path, **kwargs: object) -> dict[str, object]:
        observed_verifier.update(path=path, **kwargs)
        return {
            "status": "DEVELOPMENT_EVIDENCE_CONTAMINATED",
            "receipt_hash": RECEIPT_HASH,
        }

    monkeypatch.setattr(
        "hydra.shadow.f0_single_source_parity.verify_f0_contamination_receipt",
        verify,
    )
    monkeypatch.setattr(
        "hydra.shadow.databento_forward_feed.run_databento_forward_update",
        lambda *_args, **_kwargs: pytest.fail("feed must remain unopened"),
    )
    monkeypatch.setattr(
        "hydra.shadow.active_risk_forward_processor.run_active_risk_forward_processor",
        lambda *_args, **_kwargs: pytest.fail("processor must remain unopened"),
    )

    result = V7FalsificationController._advance_operating_package_forward(
        controller,
        conn,
        {"action_type": "MANIFEST_QUEUE_AWAITING_APPEND"},
    )

    forward = result["operating_package_forward"]
    assert forward["state"] == "DEVELOPMENT_EVIDENCE_CONTAMINATED_FAIL_CLOSED"
    assert forward["f0_contamination_receipt_hash"] == RECEIPT_HASH
    assert forward["incremental_spend_usd"] == 0.0
    assert forward["signals_emitted"] == 0
    assert forward["virtual_fills"] == 0
    assert forward["account_mutations"] == 0
    assert forward["outbound_orders"] == 0
    assert observed_verifier["repository_root"] == tmp_path
    assert observed_verifier["expected_package_manifest_hash"] == MANIFEST_HASH
    assert observed_verifier["expected_package_ids"] == BOOK_IDS
    assert get_kv(conn, "operating_forward_status") == (
        "DEVELOPMENT_EVIDENCE_CONTAMINATED_FAIL_CLOSED"
    )
    event_type, payload = conn.execute(
        "SELECT event_type, payload FROM events"
    ).fetchone()
    assert event_type == "OPERATING_PACKAGE_FORWARD_CONTAMINATION_GATE"
    event = json.loads(payload)
    assert event["f0_contamination_receipt_hash"] == RECEIPT_HASH
    assert event["incremental_spend_usd"] == 0.0
    assert event["signals"] == event["fills"] == event["account_mutations"] == 0


def test_new_terminal_receipt_overrides_a_future_append_poll(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller = _controller_tree(tmp_path)
    conn = _connection()
    conn.execute(
        "INSERT INTO kv(key, value, updated_at) VALUES (?, ?, ?)",
        (
            "operating_forward_next_check_at_utc",
            json.dumps("2099-01-01T00:00:00+00:00"),
            "2026-07-17T00:00:00Z",
        ),
    )
    conn.execute(
        "INSERT INTO kv(key, value, updated_at) VALUES (?, ?, ?)",
        (
            "operating_forward_status",
            json.dumps("WAITING_FOR_NEXT_APPEND_TICK"),
            "2026-07-17T00:00:00Z",
        ),
    )
    conn.commit()
    monkeypatch.setattr(
        "hydra.operating.package_v1.verify_operating_package_seal",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        "hydra.shadow.f0_single_source_parity.verify_f0_contamination_receipt",
        lambda *_args, **_kwargs: {
            "status": "DEVELOPMENT_EVIDENCE_CONTAMINATED",
            "receipt_hash": RECEIPT_HASH,
        },
    )

    result = V7FalsificationController._advance_operating_package_forward(
        controller, conn, {"action_type": "MANIFEST_QUEUE_AWAITING_APPEND"}
    )

    assert result["operating_package_forward"]["state"] == (
        "DEVELOPMENT_EVIDENCE_CONTAMINATED_FAIL_CLOSED"
    )
    assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1


def test_invalid_contamination_receipt_is_integrity_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller = _controller_tree(tmp_path)
    conn = _connection()
    monkeypatch.setattr(
        "hydra.operating.package_v1.verify_operating_package_seal",
        lambda *_args, **_kwargs: {},
    )

    def reject(*_args: object, **_kwargs: object) -> None:
        raise ValueError("receipt hash drift")

    monkeypatch.setattr(
        "hydra.shadow.f0_single_source_parity.verify_f0_contamination_receipt",
        reject,
    )

    with pytest.raises(
        V7ControllerIntegrityError,
        match="F0 contamination receipt integrity failure: receipt hash drift",
    ):
        V7FalsificationController._advance_operating_package_forward(
            controller,
            conn,
            {"action_type": "MANIFEST_QUEUE_AWAITING_APPEND"},
        )

    assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
    assert get_kv(conn, "operating_forward_status", None) is None
