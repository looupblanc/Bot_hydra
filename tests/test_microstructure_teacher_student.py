from __future__ import annotations

import numpy as np
import pytest

from hydra.production.microstructure_teacher_student import (
    FeatureTable,
    L1_FEATURES,
    L2_FEATURES,
    MBO_ONLY_FEATURES,
    TeacherStudentError,
    build_mbo_teacher_labels,
    score_frozen_student,
    train_deployable_students,
)


def _table() -> FeatureTable:
    names = tuple(dict.fromkeys(
        L2_FEATURES + MBO_ONLY_FEATURES
        + ("price_response_per_signed_contract", "depth_withdrawal_rate")
    ))
    count = 600
    rng = np.random.default_rng(31_031)
    values = rng.normal(size=(count, len(names)))
    # Add enough structured teacher positives for stable chronological fitting.
    for name in ("aggressor_delta", "trade_arrival_rate", "replenishment_rate", "queue_persistence"):
        values[::5, names.index(name)] += 6.0
    values[::5, names.index("price_response_per_signed_contract")] = 0.0
    roles = np.asarray(
        ["DISCOVERY"] * 360 + ["VALIDATION"] * 120 + ["FINAL_DEVELOPMENT"] * 120
    )
    markout = rng.normal(scale=0.2, size=count)
    markout[::5] = -1.0
    return FeatureTable(
        names=names,
        values=values,
        decision_ns=np.arange(count, dtype=np.int64) + 10,
        available_ns=np.arange(count, dtype=np.int64) + 9,
        roles=roles,
        market=np.asarray(["NQ" if i % 2 else "YM" for i in range(count)]),
        future_markout=markout,
        favorable_before_adverse=rng.random(count) > 0.5,
    )


def test_teacher_labels_are_separate_and_students_use_whitelists() -> None:
    table = _table()
    teachers = build_mbo_teacher_labels(table)
    assert set(teachers.labels) == {
        "ABSORPTION", "DEPLETION", "LIQUIDITY_VACUUM", "EXHAUSTION", "QUEUE_STATE"
    }
    students = train_deployable_students(table, teachers)
    assert students
    for result in students:
        allowed = set(L1_FEATURES if result.student.tier == "L1" else L2_FEATURES)
        assert set(result.student.feature_names) <= allowed
        assert not set(result.student.feature_names) & set(MBO_ONLY_FEATURES)
        scores = score_frozen_student(
            result.student, feature_names=table.names, values=table.values
        )
        assert scores.shape == (len(table.decision_ns),)
        assert np.all((0 <= scores) & (scores <= 1))


def test_future_availability_and_outcome_features_fail_closed() -> None:
    table = _table()
    with pytest.raises(TeacherStudentError, match="not yet available"):
        FeatureTable(
            table.names, table.values, table.decision_ns, table.decision_ns + 1,
            table.roles, table.market, table.future_markout,
            table.favorable_before_adverse,
        )
    with pytest.raises(TeacherStudentError, match="outcome labels"):
        FeatureTable(
            table.names + ("future_markout",),
            np.column_stack((table.values, table.future_markout)),
            table.decision_ns, table.available_ns, table.roles, table.market,
            table.future_markout, table.favorable_before_adverse,
        )
