import pytest

from ingress_server.queue_storage import QueueStorage


def _storage(tmp_path) -> QueueStorage:
    storage = QueueStorage(str(tmp_path / "queue"))
    storage.ensure_layout()
    return storage


def test_local_layout(tmp_path):
    storage = _storage(tmp_path)
    assert storage.url == f"file://{tmp_path / 'queue'}"
    for queue_name in ("accepted", "success", "failed"):
        assert (tmp_path / "queue" / queue_name).is_dir()


def test_dest_ref_requires_accepted_prefix():
    assert QueueStorage.dest_ref("accepted/ep_x.json", "success") == "success/ep_x.json"
    with pytest.raises(ValueError):
        QueueStorage.dest_ref("success/ep_x.json", "failed")
    with pytest.raises(ValueError):
        QueueStorage.dest_ref("accepted", "failed")


def test_put_list_read_roundtrip(tmp_path):
    storage = _storage(tmp_path)

    ref = storage.put_item("bukov_20250919T111523_aaa.json", b"payload", b'{"m": 1}')
    assert ref == "accepted/bukov_20250919T111523_aaa.json"
    assert storage.list_accepted() == [ref]
    assert storage.read_bytes(ref) == b"payload"
    assert storage.read_meta_text(ref) == '{"m": 1}'


def test_list_skips_sidecars_markers_and_orders_by_name(tmp_path):
    storage = _storage(tmp_path)

    late = storage.put_item("ep_b_20250919T2_bbb.json", b"2", b"{}")
    early = storage.put_item("ep_a_20250919T1_aaa.json", b"1", b"{}")
    # Leftover of the previous, non fsspec local backend.
    (tmp_path / "queue" / "accepted" / "inflight.json.tmp").write_bytes(b"x")

    # The layout marker, the .meta.json sidecars and the .tmp file are excluded.
    assert storage.list_accepted() == [early, late]


def test_list_handles_legacy_endpoint_subdirs(tmp_path):
    """Items written by the previous per-endpoint directory layout stay processable."""
    storage = _storage(tmp_path)

    legacy_dir = tmp_path / "queue" / "accepted" / "bukov"
    legacy_dir.mkdir(parents=True)
    (legacy_dir / "20250919T111523_aaa.json").write_bytes(b"payload")
    (legacy_dir / "20250919T111523_aaa.json.meta.json").write_bytes(b"{}")

    ref = "accepted/bukov/20250919T111523_aaa.json"
    assert storage.list_accepted() == [ref]
    assert storage.read_bytes(ref) == b"payload"

    storage.move(ref, "success")
    assert (tmp_path / "queue" / "success" / "bukov" / "20250919T111523_aaa.json").exists()


def test_move_carries_the_meta_sidecar(tmp_path):
    storage = _storage(tmp_path)
    ref = storage.put_item("bukov_20250919T111523_aaa.json", b"payload", b"{}")

    dest = storage.move(ref, "success")

    assert dest == "success/bukov_20250919T111523_aaa.json"
    assert storage.list_accepted() == []
    success_dir = tmp_path / "queue" / "success"
    assert (success_dir / "bukov_20250919T111523_aaa.json").read_bytes() == b"payload"
    assert (success_dir / "bukov_20250919T111523_aaa.json.meta.json").exists()


def test_move_tolerates_missing_meta(tmp_path):
    storage = _storage(tmp_path)
    ref = "accepted/orphan.json"
    (tmp_path / "queue" / ref).write_bytes(b"x")

    storage.move(ref, "failed")

    assert (tmp_path / "queue" / "failed" / "orphan.json").exists()


def test_recover_failed(tmp_path):
    storage = _storage(tmp_path)
    ref_a = storage.put_item("ep_a_a.json", b"a", b"{}")
    ref_b = storage.put_item("ep_b_b.json", b"b", b"{}")
    storage.move(ref_a, "failed")
    storage.move(ref_b, "failed")
    assert storage.list_accepted() == []

    storage.recover_failed()

    assert storage.list_accepted() == [ref_a, ref_b]
    for ref in (ref_a, ref_b):
        assert storage.read_meta_text(ref) == "{}"
    assert not any((tmp_path / "queue" / "failed").rglob("*.json"))
