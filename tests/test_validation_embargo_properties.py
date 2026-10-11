"""Prove label purging and post-test exclusion across a deterministic grid."""
from __future__ import annotations

import math

import pytest

from core.validation import purged_walk_forward_splits, split_diagnostics


@pytest.mark.parametrize("n", (0, 1, 2, 7, 31, 90, 301))
@pytest.mark.parametrize("folds", (1, 2, 5, 12))
@pytest.mark.parametrize("horizon", (1, 2, 7, 30, 400))
def test_every_training_label_precedes_test_and_every_embargo_is_excluded(n, folds, horizon):
    baseline = purged_walk_forward_splits(n, folds, horizon, 0)
    for fraction in (-0.1, 0, 0.01, 0.1, 1.0, 2.0):
        splits = purged_walk_forward_splits(n, folds, horizon, fraction)
        assert splits == baseline, "Past-only folds already exclude every post-test embargo"
        assert split_diagnostics(splits, horizon)["clean"]
        length = math.ceil(max(0, fraction) * n)
        for train, test in splits:
            assert train and test
            assert len(set(train)) == len(train)
            assert len(set(test)) == len(test)
            assert set(train).isdisjoint(test)
            assert all(0 <= i < n for i in train + test)
            # A training label covers [i, i+horizon-1]; it cannot touch test.
            assert all(i + horizon - 1 < min(test) for i in train)
            embargo = set(range(max(test) + 1, min(n, max(test) + 1 + length)))
            assert set(train).isdisjoint(embargo)
            assert max(train) < min(test)


def test_embargo_and_purge_proof_has_nonempty_training_folds():
    splits = purged_walk_forward_splits(301, 5, 30, 2)
    assert len(splits) == 5
    assert splits[0] == (list(range(21)), list(range(50, 100)))


@pytest.mark.parametrize("fraction,error", ((float("inf"), OverflowError), ("invalid", TypeError)))
def test_compatibility_parameter_retains_legacy_validation(fraction, error):
    with pytest.raises(error):
        purged_walk_forward_splits(301, 5, 30, fraction)
