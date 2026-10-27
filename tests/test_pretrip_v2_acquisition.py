"""Synthetic HTTP streams and tiny Parquet footers; never uses live network."""

from copy import deepcopy
from datetime import datetime, timezone
from io import BytesIO
import hashlib
import json
import ssl
from unittest.mock import Mock
from urllib.error import HTTPError, URLError

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nyc_taxi.ml import pretrip_acquisition as acquisition
from nyc_taxi.ml.pretrip_budget import METADATA_RESERVE_BYTES, ManagedBudgetExceeded


def parquet_body(*, missing=None, aware=False, empty=False):
    schema = pa.schema([
        ("VendorID", pa.int64()),
        ("tpep_pickup_datetime", pa.timestamp("us", tz="UTC" if aware else None)),
        ("tpep_dropoff_datetime", pa.timestamp("us")),
        ("PULocationID", pa.int64()), ("DOLocationID", pa.int64()),
        ("RatecodeID", pa.float64()), ("fare_amount", pa.float64()),
    ])
    records = [{
        "VendorID": 1,
        "tpep_pickup_datetime": datetime(2026, 6, 5, 10, tzinfo=timezone.utc if aware else None),
        "tpep_dropoff_datetime": datetime(2026, 6, 5, 10, 15),
        "PULocationID": 164, "DOLocationID": 43,
        "RatecodeID": 1.0, "fare_amount": 12.5,
    }]
    table = pa.Table.from_pylist([] if empty else records, schema=schema)
    if missing is not None:
        table = table.drop([missing])
    stream = BytesIO()
    pq.write_table(table, stream)
    return stream.getvalue()


@pytest.fixture
def project(tmp_path):
    config = tmp_path / "config"
    config.mkdir()
    original = json.loads((acquisition.PROJECT_ROOT / "config/pretrip_v2_sources.json").read_text(encoding="utf-8"))
    body = parquet_body()
    original["version"] += "-synthetic"
    for row in original["files"]:
        row["content_length_bytes"] = len(body)
    (config / "pretrip_v2_sources.json").write_text(json.dumps(original), encoding="utf-8")
    return tmp_path, original, body


class Response:
    def __init__(self, body, source, *, status=200, length=None, etag=None):
        self.status = status
        self.headers = {
            "Content-Length": str(source["content_length_bytes"] if length is None else length),
            "ETag": source["etag"] if etag is None else etag,
        }
        self.stream = BytesIO(body)
        self.read_sizes = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.stream.close()
        return False

    def read(self, size):
        assert 0 < size <= acquisition.CHUNK_BYTES
        self.read_sizes.append(size)
        return self.stream.read(size)


def source_dir(root):
    return root / ".sandbox/pretrip-v2/sources"


def invoke(project, *, opener=None, validator=None, budget=2 * acquisition.MIB):
    root, manifest, body = project
    if opener is None:
        opener = Mock(side_effect=[Response(body, source) for source in manifest["files"]])
    return acquisition.acquire_sources(
        manifest, source_dir(root), disk_budget_bytes=budget,
        project_root=root, opener=opener, validator=validator,
    )


def checkpoint(root):
    paths = list(source_dir(root).glob("acquisition-*.checkpoint.json"))
    assert len(paths) == 1
    return json.loads(paths[0].read_text(encoding="utf-8"))


def test_default_plan_is_offline_no_directory_footer_or_body_reads(project):
    root, _, _ = project
    transport = Mock(side_effect=AssertionError("PLAN must not request"))
    validator = Mock(side_effect=AssertionError("PLAN must not open Parquet"))
    report = acquisition.run(acquisition.parser().parse_args([]), project_root=root,
                             opener=transport, validator=validator)
    assert report["status"] == "PLAN_NOT_EXECUTED"
    assert report["source_body_bytes_downloaded"] == 0
    assert report["plan_writes_files"] is False
    assert report["budget_granted"] is False
    assert not (root / ".sandbox").exists()
    transport.assert_not_called()
    validator.assert_not_called()


def test_execute_requires_explicit_budget_before_any_http_or_runtime_creation(project):
    root, _, _ = project
    transport = Mock()
    with pytest.raises(acquisition.AcquisitionError, match="explicit_acquisition_disk_budget_required"):
        acquisition.run(acquisition.parser().parse_args(["--execute"]),
                        project_root=root, opener=transport)
    transport.assert_not_called()
    assert not (root / ".sandbox").exists()


def test_success_uses_exactly_three_verified_gets_pins_headers_hashes_and_footer(project):
    root, manifest, body = project
    responses = [Response(body, source) for source in manifest["files"]]
    transport = Mock(side_effect=responses)
    report = invoke(project, opener=transport)
    assert report["status"] == "COMPLETE"
    assert report["requests"] == {"GET": 3, "HEAD": 0, "retries": 0}
    assert transport.call_count == 3
    for call, source in zip(transport.call_args_list, manifest["files"], strict=True):
        request = call.args[0]
        assert request.get_method() == "GET" and request.full_url == source["url"]
        assert request.data is None
        assert call.kwargs["timeout"] == acquisition.REQUEST_TIMEOUT_SECONDS
        assert request.get_header("Accept-encoding") == "identity"
        entry = report["files"][source["month"]]
        assert entry["status"] == "COMPLETE" and entry["reused"] is False
        assert entry["bytes"] == len(body)
        assert entry["sha256"] == hashlib.sha256(body).hexdigest()
        assert entry["etag"] == source["etag"]
        assert entry["footer"]["rows"] == 1 and entry["footer"]["row_groups"] == 1
        assert [field["name"] for field in entry["footer"]["required_columns"]] == list(acquisition.READ_COLUMNS)
        assert (source_dir(root) / entry["filename"]).read_bytes() == body
    assert all(response.read_sizes for response in responses)
    assert not list(source_dir(root).glob("*.parquet.tmp"))
    assert len(list(source_dir(root).glob("*.report.json"))) == 1
    assert len(list(source_dir(root).glob("*.checkpoint.json"))) == 1
    assert all(path.stat().st_size <= 16 * 1024 for path in source_dir(root).glob("*.json"))
    assert "not provider SHA256" in report["source_snapshot_kind"]
    assert "no trip rows" in report["files"]["2026-06"]["footer"]["verification"]


def test_existing_complete_files_rehash_footer_validate_and_reuse_without_get(project):
    root, _, body = project
    original = invoke(project)
    original_sources = {row["filename"]: (source_dir(root) / row["filename"]).read_bytes()
                        for row in original["files"].values()}
    transport = Mock(side_effect=AssertionError("reuse must not GET"))
    validator = Mock(wraps=acquisition.validate_parquet_footer)
    report = invoke(project, opener=transport, validator=validator)
    transport.assert_not_called()
    assert validator.call_count == 3
    assert report["requests"]["GET"] == 0
    assert all(row["reused"] for row in report["files"].values())
    assert all(row["sha256"] == hashlib.sha256(body).hexdigest() for row in report["files"].values())
    assert report["disk_budget"]["sources_bytes"] >= 3 * len(body)
    for filename, content in original_sources.items():
        assert (source_dir(root) / filename).read_bytes() == content


def test_existing_unrecorded_or_partial_source_is_preserved_and_rejected_without_get(project):
    root, manifest, _ = project
    directory = source_dir(root)
    directory.mkdir(parents=True)
    partial = directory / "yellow_tripdata_2026-06.parquet"
    partial.write_bytes(b"preserve-partial")
    transport = Mock()
    with pytest.raises(acquisition.AcquisitionError, match="existing_source_unverified_or_partial"):
        invoke(project, opener=transport)
    transport.assert_not_called()
    assert partial.read_bytes() == b"preserve-partial"
    assert checkpoint(root)["status"] == "FAILED"


def test_reuse_rejects_same_size_corruption_before_footer_or_network(project):
    root, _, _ = project
    first = invoke(project)
    entry = first["files"]["2026-06"]
    path = source_dir(root) / entry["filename"]
    body = path.read_bytes()
    path.write_bytes(bytes([body[0] ^ 1]) + body[1:])
    corrupted = path.read_bytes()
    transport = Mock()
    validator = Mock()
    with pytest.raises(acquisition.AcquisitionError, match="existing_source_checksum_mismatch"):
        invoke(project, opener=transport, validator=validator)
    transport.assert_not_called()
    validator.assert_not_called()
    assert path.read_bytes() == corrupted


def test_reuse_requires_exact_source_manifest_version(project):
    invoke(project)
    changed = deepcopy(project[1])
    changed["version"] += "-different"
    transport = Mock()
    with pytest.raises(acquisition.AcquisitionError, match="existing_source_unverified_or_partial"):
        invoke((project[0], changed, project[2]), opener=transport)
    transport.assert_not_called()


@pytest.mark.parametrize(("kind", "expected"), [
    ("status", "http_status_rejected"),
    ("length", "content_length_mismatch"),
    ("etag", "etag_mismatch"),
])
def test_header_or_status_rejection_stops_after_one_get_without_body_or_final_file(project, kind, expected):
    root, manifest, body = project
    response = Response(body, manifest["files"][0])
    if kind == "status":
        response.status = 403
    elif kind == "length":
        response.headers["Content-Length"] = str(len(body) + 1)
    else:
        response.headers["ETag"] = '"different-opaque-version"'
    transport = Mock(return_value=response)
    with pytest.raises(acquisition.AcquisitionError, match=expected):
        invoke(project, opener=transport)
    assert transport.call_count == 1 and response.read_sizes == []
    assert not list(source_dir(root).glob("*.parquet"))
    assert checkpoint(root)["stop_reason"] == expected


@pytest.mark.parametrize(("body_change", "expected"), [
    ("truncated", "response_truncated"), ("overlong", "response_overlong"),
])
def test_truncated_and_overlong_streams_keep_partial_without_retry_or_overwrite(project, body_change, expected):
    root, manifest, body = project
    response_body = body[:len(body) // 2] if body_change == "truncated" else body + b"extra"
    response = Response(response_body, manifest["files"][0])
    transport = Mock(return_value=response)
    validator = Mock()
    with pytest.raises(acquisition.AcquisitionError, match=expected):
        invoke(project, opener=transport, validator=validator)
    assert transport.call_count == 1
    validator.assert_not_called()
    partial = source_dir(root) / "yellow_tripdata_2026-06.parquet"
    assert partial.exists() and partial.stat().st_size <= len(body)
    if body_change == "truncated":
        assert partial.read_bytes() == response_body
    assert checkpoint(root)["status"] == "FAILED"


@pytest.mark.parametrize(("error", "expected"), [
    (TimeoutError("PRIVATE network detail"), "http_transport_failed"),
    (ssl.SSLCertVerificationError("PRIVATE certificate detail"), "tls_verification_failed"),
    (URLError(ssl.SSLCertVerificationError("PRIVATE certificate detail")), "tls_verification_failed"),
    (HTTPError("https://invalid.example/PRIVATE", 503, "PRIVATE response", {}, None), "http_status_rejected"),
])
def test_http_tls_and_transport_failures_do_not_retry_or_persist_exception_text(project, error, expected):
    root, _, _ = project
    transport = Mock(side_effect=error)
    with pytest.raises(acquisition.AcquisitionError, match=expected):
        invoke(project, opener=transport)
    assert transport.call_count == 1
    metadata = checkpoint(root)
    assert metadata["stop_reason"] == expected
    assert "PRIVATE" not in json.dumps(metadata)
    assert metadata["requests"]["retries"] == 0


def test_budget_guard_counts_existing_files_and_stops_before_excess_write(project):
    root, manifest, body = project
    directory = source_dir(root)
    directory.mkdir(parents=True)
    existing = directory / "unrelated-retained-fixture.bin"
    existing.write_bytes(b"preserve")
    limit = METADATA_RESERVE_BYTES + len(body) + existing.stat().st_size
    responses = [Response(body, source) for source in manifest["files"]]
    transport = Mock(side_effect=responses)
    with pytest.raises(ManagedBudgetExceeded) as caught:
        invoke(project, opener=transport, budget=limit)
    assert caught.value.stop_reason == "managed_file_budget_exceeded"
    assert caught.value.budget_usage["sources_bytes"] == len(b"preserve")
    assert transport.call_count == 2
    assert existing.read_bytes() == b"preserve"
    assert sum(path.stat().st_size for path in directory.rglob("*") if path.is_file()) <= limit
    assert checkpoint(root)["status"] == "FAILED"
    assert not list(directory.glob("*.tmp"))


@pytest.mark.parametrize("body_kwargs", [{"missing": "VendorID"}, {"aware": True}, {"empty": True}])
def test_missing_required_columns_and_aware_timestamp_schema_fail_after_one_download(project, body_kwargs):
    root, manifest, _ = project
    body = parquet_body(**body_kwargs)
    changed = deepcopy(manifest)
    for row in changed["files"]:
        row["content_length_bytes"] = len(body)
    transport = Mock(return_value=Response(body, changed["files"][0]))
    with pytest.raises(acquisition.AcquisitionError, match="invalid_parquet_footer_or_schema"):
        invoke((root, changed, body), opener=transport)
    assert transport.call_count == 1
    assert (source_dir(root) / "yellow_tripdata_2026-06.parquet").read_bytes() == body
    assert checkpoint(root)["status"] == "FAILED"


def test_footer_inspection_never_reads_record_batches(project, monkeypatch):
    root, _, body = project
    path = root / "synthetic.parquet"
    path.write_bytes(body)
    original = pq.ParquetFile

    class FooterOnly:
        def __init__(self, target):
            self.inner = original(target)
            self.schema_arrow = self.inner.schema_arrow
            self.metadata = self.inner.metadata

        def read(self, *args, **kwargs):
            raise AssertionError("must not read trip records")

        def iter_batches(self, *args, **kwargs):
            raise AssertionError("must not stream trip records")

        def close(self):
            self.inner.close()

    monkeypatch.setattr(pq, "ParquetFile", FooterOnly)
    assert acquisition.validate_parquet_footer(path)["rows"] == 1


def test_redirect_handler_and_runtime_path_guards(project, tmp_path_factory):
    root, _, _ = project
    handler = acquisition._NoRedirect()
    assert handler.redirect_request(None, None, 302, None, {}, "https://invalid.example") is None
    for target in (root / "data/new", root / ".sandbox/pretrip-v2", root / ".sandbox/pretrip-v2/../../escape"):
        with pytest.raises(acquisition.AcquisitionError, match="source_path_guard_failed"):
            acquisition.guarded_source_directory(target, root)
    outside = tmp_path_factory.mktemp("outside-acquisition")
    (root / ".sandbox").symlink_to(outside, target_is_directory=True)
    with pytest.raises(acquisition.AcquisitionError, match="source_path_guard_failed"):
        acquisition.guarded_source_directory(source_dir(root), root)


def test_main_failure_only_outputs_safe_code_status_and_budget_counters(monkeypatch, capsys):
    error = acquisition.AcquisitionError("etag_mismatch", http_status=200)
    monkeypatch.setattr(acquisition, "run", Mock(side_effect=error))
    assert acquisition.main([]) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["code"] == "etag_mismatch" and report["http_status"] == 200
    assert "url" not in report and "body" not in report
