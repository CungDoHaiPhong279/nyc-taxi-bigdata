"""Tiny synthetic byte quotas; no Parquet sources, services, network or model."""

import io
import json
import os
from pathlib import Path

import pytest

from nyc_taxi.ml.pretrip_budget import (
    MAX_METADATA_DOCUMENT_BYTES,
    METADATA_RESERVE_BYTES,
    ManagedBudgetExceeded,
    ManagedFileBudget,
)


def budget_attempt(tmp_path: Path, *, capacity: int = 8, source_bytes: bytes = b"source"):
    source = tmp_path / "source.parquet"
    source.write_bytes(source_bytes)
    root = tmp_path / "attempt"
    budget = ManagedFileBudget([source], root, len(source_bytes) + METADATA_RESERVE_BYTES + capacity)
    # The ledger's constructor must not create an output directory itself.
    assert not root.exists()
    root.mkdir()
    return budget, source, root


def assert_saved_usage_matches_files(budget, checkpoint, *, sources_bytes):
    document = json.loads(checkpoint.read_text(encoding="utf-8"))
    usage = document["disk_budget"]
    actual_managed = sum(path.stat().st_size for path in checkpoint.parent.iterdir() if path.is_file())
    assert usage == budget.snapshot()
    assert usage["sources_bytes"] == sources_bytes
    assert usage["managed_bytes"] == actual_managed
    assert usage["used_bytes"] == sources_bytes + actual_managed
    assert usage["remaining_bytes"] == usage["limit_bytes"] - usage["used_bytes"]
    return document


def test_existing_sources_count_reused_paths_and_hardlinks_only_once(tmp_path):
    source = tmp_path / "source.parquet"
    source.write_bytes(b"existing-source")
    alias = tmp_path / "hardlink.parquet"
    os.link(source, alias)
    root = tmp_path / "not-created"
    ledger = ManagedFileBudget([source, source, source.resolve(), alias], root,
                               METADATA_RESERVE_BYTES + source.stat().st_size + 8)
    usage = ledger.snapshot()
    assert usage["sources_bytes"] == len(b"existing-source")
    assert usage["used_bytes"] == len(b"existing-source")
    assert usage["available_write_bytes"] == 8
    assert usage["managed_bytes"] == 0
    assert not root.exists()


def test_sources_larger_than_budget_fail_before_creating_any_output(tmp_path):
    source = tmp_path / "source.parquet"
    source.write_bytes(b"12345678")
    root = tmp_path / "attempt"
    with pytest.raises(ManagedBudgetExceeded) as caught:
        ManagedFileBudget([source], root, 7)
    assert caught.value.stop_reason == "source_files_exceed_budget"
    assert caught.value.budget_usage["sources_bytes"] == 8
    assert not root.exists()
    assert source.read_bytes() == b"12345678"
    assert list(tmp_path.iterdir()) == [source]


def test_sources_without_full_metadata_reserve_fail_before_output_creation(tmp_path):
    source = tmp_path / "source.parquet"
    source.write_bytes(b"12345678")
    root = tmp_path / "attempt"
    with pytest.raises(ManagedBudgetExceeded) as caught:
        ManagedFileBudget([source], root, METADATA_RESERVE_BYTES + 7)
    assert caught.value.stop_reason == "metadata_reserve_unavailable"
    assert not root.exists()
    assert source.read_bytes() == b"12345678"
    assert list(tmp_path.iterdir()) == [source]


@pytest.mark.parametrize("limit", [None, 0, -1, True, 1.0])
def test_budget_is_an_explicit_positive_integer_before_output_creation(tmp_path, limit):
    source = tmp_path / "source.parquet"
    source.write_bytes(b"x")
    root = tmp_path / "attempt"
    with pytest.raises(ValueError, match="explicit positive integer"):
        ManagedFileBudget([source], root, limit)
    assert not root.exists()


def test_exact_remaining_bytes_succeed_and_next_byte_is_blocked_before_write(tmp_path):
    budget, source, root = budget_attempt(tmp_path)
    target = root / "data.parquet"
    with budget.open_file(target) as stream:
        assert stream.write(b"12345678") == 8
        assert budget.snapshot()["available_write_bytes"] == 0
        assert budget.snapshot()["reserved_bytes"] == METADATA_RESERVE_BYTES
        before = target.read_bytes()
        with pytest.raises(ManagedBudgetExceeded) as caught:
            stream.write(b"9")
        assert caught.value.stop_reason == "managed_file_budget_exceeded"
        assert stream.tell() == 8
        assert target.read_bytes() == before == b"12345678"
    usage = budget.snapshot()
    assert usage["managed_bytes"] == 8
    assert usage["used_bytes"] == source.stat().st_size + 8


def test_seek_overwrite_preserves_budget_and_sparse_extension_checks_full_length(tmp_path):
    budget, _, root = budget_attempt(tmp_path)
    target = root / "data.parquet"
    with budget.open_file(target) as stream:
        stream.write(b"abcdef")
        assert stream.seek(2, io.SEEK_SET) == 2
        assert stream.write(b"XY") == 2
        assert target.read_bytes() == b"abXYef"
        assert budget.snapshot()["available_write_bytes"] == 2
        stream.seek(7)
        stream.write(b"Z")
        assert target.read_bytes() == b"abXYef\x00Z"
        assert budget.snapshot()["available_write_bytes"] == 0
        before = target.read_bytes()
        with pytest.raises(ManagedBudgetExceeded):
            stream.write(b"extra")
        assert target.read_bytes() == before


def test_truncate_growth_is_checked_and_shrinking_returns_available_bytes(tmp_path):
    budget, _, root = budget_attempt(tmp_path)
    target = root / "data.parquet"
    with budget.open_file(target) as stream:
        stream.write(b"abc")
        assert stream.truncate(8) == 8
        assert target.stat().st_size == 8
        assert budget.snapshot()["available_write_bytes"] == 0
        before = target.read_bytes()
        with pytest.raises(ManagedBudgetExceeded):
            stream.truncate(9)
        assert target.read_bytes() == before
        assert stream.truncate(4) == 4
        assert budget.snapshot()["available_write_bytes"] == 4
        stream.seek(4)
        assert stream.write(b"WXYZ") == 4
        assert target.stat().st_size == 8


def test_temporary_and_final_data_files_share_the_same_total_quota(tmp_path):
    budget, source, root = budget_attempt(tmp_path)
    temporary = root / "data.tmp"
    final = root / "data.parquet"
    with budget.open_file(temporary) as stream:
        stream.write(b"12345")
    with budget.open_file(final) as stream:
        stream.write(b"abc")
        with pytest.raises(ManagedBudgetExceeded):
            stream.write(b"d")
    assert temporary.read_bytes() == b"12345"
    assert final.read_bytes() == b"abc"
    usage = budget.snapshot()
    assert usage["managed_bytes"] == temporary.stat().st_size + final.stat().st_size == 8
    assert usage["used_bytes"] == source.stat().st_size + 8


def test_atomic_json_counts_old_and_temporary_peak_and_saves_actual_post_replace_usage(tmp_path):
    budget, source, root = budget_attempt(tmp_path)
    checkpoint = root / "checkpoint.json"
    budget.write_json(checkpoint, {"status": "INCOMPLETE", "padding": "old"})
    old_size = checkpoint.stat().st_size
    budget.write_json(checkpoint, {"status": "INCOMPLETE", "padding": "new" * 100})
    new_size = checkpoint.stat().st_size
    document = assert_saved_usage_matches_files(budget, checkpoint, sources_bytes=source.stat().st_size)
    assert document["padding"] == "new" * 100
    assert document["disk_budget"]["peak_used_bytes"] >= source.stat().st_size + old_size + new_size
    assert not checkpoint.with_name(checkpoint.name + ".tmp").exists()
    assert list(root.iterdir()) == [checkpoint]


def test_compact_failed_checkpoint_remains_writable_after_data_budget_is_exhausted(tmp_path):
    budget, source, root = budget_attempt(tmp_path)
    checkpoint = root / "checkpoint.json"
    budget.write_json(checkpoint, {"status": "INCOMPLETE"})
    with budget.open_file(root / "data.parquet") as stream:
        stream.write(b"12345678")
        with pytest.raises(ManagedBudgetExceeded) as caught:
            stream.write(b"9")
    budget.write_json(checkpoint, {"status": "FAILED", "code": caught.value.stop_reason})
    document = assert_saved_usage_matches_files(budget, checkpoint, sources_bytes=source.stat().st_size)
    assert document["status"] == "FAILED"
    assert document["code"] == "managed_file_budget_exceeded"
    assert document["disk_budget"]["stop_reason"] == "managed_file_budget_exceeded"
    assert document["disk_budget"]["available_write_bytes"] == 0
    assert (root / "data.parquet").read_bytes() == b"12345678"
    assert not list(root.glob("*.tmp"))


def test_oversized_json_is_rejected_before_write_and_compact_failed_metadata_still_fits(tmp_path):
    budget, source, root = budget_attempt(tmp_path)
    checkpoint = root / "checkpoint.json"
    budget.write_json(checkpoint, {"status": "INCOMPLETE"})
    before = checkpoint.read_bytes()
    used_before = budget.snapshot()["used_bytes"]
    with pytest.raises(ManagedBudgetExceeded) as caught:
        budget.write_json(checkpoint, {"status": "INCOMPLETE", "padding": "x" * MAX_METADATA_DOCUMENT_BYTES})
    assert caught.value.stop_reason == "metadata_reserve_exceeded"
    assert checkpoint.read_bytes() == before
    assert budget.snapshot()["used_bytes"] == used_before
    assert not list(root.glob("*.tmp"))
    budget.write_json(checkpoint, {"status": "FAILED", "code": caught.value.stop_reason})
    document = assert_saved_usage_matches_files(budget, checkpoint, sources_bytes=source.stat().st_size)
    assert document["status"] == "FAILED"
    assert document["disk_budget"]["stop_reason"] == "metadata_reserve_exceeded"
    assert not list(root.glob("*.tmp"))


def test_failed_atomic_metadata_write_removes_only_its_temporary_file(tmp_path):
    budget, _, root = budget_attempt(tmp_path)
    checkpoint = root / "checkpoint.json"
    budget.write_json(checkpoint, {"status": "INCOMPLETE"})
    before = checkpoint.read_bytes()
    # Saturate most of the metadata pool with a tiny owned fixture. An atomic
    # replacement must count the old checkpoint AND its new temporary document.
    with budget.open_file(root / "other-metadata.bin", metadata=True) as stream:
        stream.write(b"m" * (METADATA_RESERVE_BYTES - len(before) - 128))
    files_before = {path.name: path.read_bytes() for path in root.iterdir()}
    with pytest.raises(ManagedBudgetExceeded) as caught:
        budget.write_json(checkpoint, {"status": "UPDATED"})
    assert caught.value.stop_reason == "metadata_reserve_exceeded"
    assert {path.name: path.read_bytes() for path in root.iterdir()} == files_before
    assert checkpoint.read_bytes() == before
    assert not list(root.glob("*.tmp"))
    assert all(path.exists() for path in budget.files)


def test_source_growth_is_observed_before_the_next_owned_write(tmp_path):
    budget, source, root = budget_attempt(tmp_path, source_bytes=b"s")
    target = root / "data.parquet"
    with budget.open_file(target) as stream:
        stream.write(b"1234567")
        source.write_bytes(b"sss")
        before = target.read_bytes()
        with pytest.raises(ManagedBudgetExceeded) as caught:
            stream.write(b"8")
        assert caught.value.stop_reason == "managed_file_budget_exceeded"
        assert caught.value.budget_usage["sources_bytes"] == 3
        assert target.read_bytes() == before
        assert stream.tell() == 7
    assert source.read_bytes() == b"sss"
    assert budget.snapshot()["sources_bytes"] == 3


def test_sources_consuming_the_reserve_block_even_an_overwrite_before_it_happens(tmp_path):
    budget, source, root = budget_attempt(tmp_path, source_bytes=b"s")
    target = root / "data.parquet"
    with budget.open_file(target) as stream:
        stream.write(b"abc")
        source.write_bytes(b"s" * 10)
        stream.seek(0)
        with pytest.raises(ManagedBudgetExceeded) as caught:
            stream.write(b"z")
        assert caught.value.stop_reason == "metadata_reserve_unavailable"
        assert target.read_bytes() == b"abc"
        assert stream.tell() == 0


def test_metadata_cannot_replace_owned_data_file_or_spend_its_accounting(tmp_path):
    budget, _, root = budget_attempt(tmp_path)
    target = root / "data.parquet"
    with budget.open_file(target) as stream:
        stream.write(b"original")
    before = budget.snapshot()
    with pytest.raises(ValueError, match="metadata cannot replace"):
        budget.write_json(target, {"status": "FAILED"})
    assert target.read_bytes() == b"original"
    assert budget.snapshot() == before
    assert not list(root.glob("*.tmp"))
