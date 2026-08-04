import pytest

from ingress_server.queue_storage import LocalQueueStorage, QueueStorage


def _storage(tmp_path) -> QueueStorage:
    storage = QueueStorage.from_url(str(tmp_path / "queue"))
    storage.ensure_layout()
    return storage


def test_from_url_local(tmp_path):
    storage = _storage(tmp_path)
    assert isinstance(storage, LocalQueueStorage)
    for queue_name in ("accepted", "success", "failed"):
        assert (tmp_path / "queue" / queue_name).is_dir()


def test_dest_key_requires_accepted_prefix():
    assert QueueStorage.dest_key("accepted/ep/x.json", "success") == "success/ep/x.json"
    with pytest.raises(ValueError):
        QueueStorage.dest_key("success/ep/x.json", "failed")
    with pytest.raises(ValueError):
        QueueStorage.dest_key("accepted", "failed")


def test_put_list_read_roundtrip(tmp_path):
    storage = _storage(tmp_path)

    key = storage.put_item("bukov", "20250919T111523_aaa.json", b"payload", b'{"m": 1}')
    assert key == "accepted/bukov/20250919T111523_aaa.json"
    assert storage.list_accepted() == [key]
    assert storage.read_bytes(key) == b"payload"
    assert storage.read_meta_text(key) == '{"m": 1}'


def test_list_skips_sidecars_and_orders_by_basename(tmp_path):
    storage = _storage(tmp_path)

    late = storage.put_item("ep_b", "20250919T2_bbb.json", b"2", b"{}")
    early = storage.put_item("ep_a", "20250919T1_aaa.json", b"1", b"{}")
    (tmp_path / "queue" / "accepted" / "ep_a" / "inflight.json.tmp").write_bytes(b"x")

    # Sorted by basename across endpoint subtrees; .meta.json and .tmp excluded.
    assert storage.list_accepted() == [early, late]


def test_move_preserves_subtree_and_meta(tmp_path):
    storage = _storage(tmp_path)
    key = storage.put_item("bukov", "20250919T111523_aaa.json", b"payload", b"{}")

    storage.move(key, "success")

    assert storage.list_accepted() == []
    success_dir = tmp_path / "queue" / "success" / "bukov"
    assert (success_dir / "20250919T111523_aaa.json").read_bytes() == b"payload"
    assert (success_dir / "20250919T111523_aaa.json.meta.json").exists()


def test_move_tolerates_missing_meta(tmp_path):
    storage = _storage(tmp_path)
    key = "accepted/bukov/orphan.json"
    payload_path = tmp_path / "queue" / key
    payload_path.parent.mkdir(parents=True)
    payload_path.write_bytes(b"x")

    storage.move(key, "failed")

    assert (tmp_path / "queue" / "failed" / "bukov" / "orphan.json").exists()


def test_recover_failed_restores_subtree(tmp_path):
    storage = _storage(tmp_path)
    key_a = storage.put_item("ep_a", "a.json", b"a", b"{}")
    key_b = storage.put_item("ep_b", "b.json", b"b", b"{}")
    storage.move(key_a, "failed")
    storage.move(key_b, "failed")

    storage.recover_failed()

    assert storage.list_accepted() == [key_a, key_b]
    for key in (key_a, key_b):
        assert storage.read_meta_text(key) == "{}"
    # failed/ subtree is pruned to (at most) the bare queue directory.
    assert not any((tmp_path / "queue" / "failed").rglob("*.json"))
