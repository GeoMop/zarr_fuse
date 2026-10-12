from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from dashboard.map_views import _cluster_points
from dashboard.multi_time_views import _resolve_tap_targets


def _make_df(lons, lats, entity_indices=None):
    data = {"lon": lons, "lat": lats, "entity": [f"site_{i}" for i in range(len(lons))]}
    if entity_indices is not None:
        data["entity_index"] = entity_indices
    return pd.DataFrame(data)


def _ranges(width_deg=10.0):
    x_range = (0.0, width_deg)
    y_range = (0.0, width_deg)
    return x_range, y_range


def test_no_range_returns_each_point_as_single_member() -> None:
    df = _make_df([1.0, 2.0, 3.0], [1.0, 2.0, 3.0], entity_indices=[10, 20, 30])
    clustered, members, radii = _cluster_points(None, None, df, "lon", "lat", "entity")
    assert len(clustered) == 3
    assert all(clustered["merged_count"] == 1)
    assert members == [[10], [20], [30]]
    assert radii == [0.0, 0.0, 0.0]


def test_member_lists_without_entity_index_fall_back_to_row_indices() -> None:
    df = _make_df([1.0, 2.0, 3.0], [1.0, 2.0, 3.0])
    clustered, members, radii = _cluster_points(None, None, df, "lon", "lat", "entity")
    assert len(clustered) == 3
    assert members == [[0], [1], [2]]
    assert radii == [0.0, 0.0, 0.0]


def test_proximity_group_merges_all_members_into_one_cluster() -> None:
    df = _make_df([1.0, 1.001, 1.002], [1.0, 1.001, 1.002], entity_indices=[7, 8, 9])
    x_range, y_range = _ranges(width_deg=10.0)
    eps_factor = 1.0  # eps = view_width -> all points share one grid cell
    clustered, members, radii = _cluster_points(
        x_range, y_range, df, "lon", "lat", "entity", eps_factor=eps_factor
    )
    assert len(clustered) == 1
    assert int(clustered["merged_count"].iloc[0]) == 3
    assert members == [[7, 8, 9]]
    assert np.isclose(radii[0], np.sqrt(2) * 0.001, atol=1e-6)  # centroid to corner


def test_distant_points_form_separate_clusters() -> None:
    df = _make_df([1.0, 5.0, 9.0], [1.0, 5.0, 9.0], entity_indices=[1, 2, 3])
    x_range, y_range = _ranges(width_deg=10.0)
    eps_factor = 0.05  # eps = 0.5 deg -> one cell per point
    clustered, members, radii = _cluster_points(
        x_range, y_range, df, "lon", "lat", "entity", eps_factor=eps_factor
    )
    assert len(clustered) == 3
    assert members == [[1], [2], [3]]
    assert radii == [0.0, 0.0, 0.0]


def test_empty_df_returns_empty_clusters_and_members() -> None:
    df = _make_df([], [], entity_indices=[])
    x_range, y_range = _ranges()
    clustered, members, radii = _cluster_points(x_range, y_range, df, "lon", "lat", "entity")
    assert len(clustered) == 0
    assert members == []
    assert radii == []


def test_all_points_outside_view_yield_empty_result() -> None:
    df = _make_df([100.0, 101.0], [100.0, 101.0], entity_indices=[1, 2])
    x_range, y_range = _ranges(width_deg=10.0)
    clustered, members, radii = _cluster_points(x_range, y_range, df, "lon", "lat", "entity")
    assert len(clustered) == 0
    assert members == []
    assert radii == []


def test_cluster_centroid_is_group_mean_and_label_prefixed() -> None:
    df = _make_df([1.0, 1.0, 1.01], [2.0, 2.0, 2.01], entity_indices=[5, 6, 7])
    x_range, y_range = _ranges(width_deg=10.0)
    eps_factor = 1.0
    clustered, members, radii = _cluster_points(
        x_range, y_range, df, "lon", "lat", "entity", eps_factor=eps_factor
    )
    assert len(clustered) == 1
    assert np.isclose(clustered["lon"].iloc[0], 1.003333, atol=1e-5)
    assert np.isclose(clustered["lat"].iloc[0], 2.003333, atol=1e-5)
    assert int(clustered["merged_count"].iloc[0]) == 3
    assert members == [[5, 6, 7]]
    assert clustered["label"].iloc[0] == "site_0"


def _make_cluster_metas(*clusters):
    """Build current_clusters-style dicts from (lon, lat, entity_indices) tuples."""
    return [
        {"lon": lon, "lat": lat, "entity_indices": idx, "radius": 0.0}
        for lon, lat, idx in clusters
    ]


def test_tap_on_merged_cluster_centroid_selects_all_members() -> None:
    clusters = _make_cluster_metas((1.005, 1.005, [7, 8, 9]))
    clusters[0]["radius"] = 0.001
    all_meta = [{"entity_index": 7}, {"entity_index": 8}, {"entity_index": 9}]
    # Click dead-center on the centroid: nearest single member is ~3.5e-4 deg
    # away (> threshold_deg), yet the cluster hit must select all members.
    targets = _resolve_tap_targets(1.005, 1.005, clusters, all_meta, 0, 1e-6, 0.0002)
    assert targets == [{"entity_index": 7}, {"entity_index": 8}, {"entity_index": 9}]


def test_tap_near_merged_cluster_within_radius_selects_all_members() -> None:
    clusters = _make_cluster_metas((1.0, 1.0, [7, 8, 9]))
    clusters[0]["radius"] = 0.0008
    all_meta = [{"entity_index": 8}]
    targets = _resolve_tap_targets(1.0007, 1.0, clusters, all_meta, 0, 1e-6, 0.0002)
    assert targets == [{"entity_index": 7}, {"entity_index": 8}, {"entity_index": 9}]


def test_tap_outside_cluster_radius_falls_back_to_single_marker() -> None:
    clusters = _make_cluster_metas((1.0, 1.0, [7, 8, 9]))
    clusters[0]["radius"] = 0.0003
    all_meta = [{"entity_index": 5}]
    # Far from cluster centroid AND from every single marker -> None.
    targets = _resolve_tap_targets(1.9, 1.9, clusters, all_meta, 0, 0.01, 0.0002)
    assert targets is None


def test_single_member_cluster_without_clusters_keeps_single_behavior() -> None:
    clusters = []
    all_meta = [{"entity_index": 3}]
    targets = _resolve_tap_targets(1.0, 1.0, clusters, all_meta, 0, 1e-8, 0.0002)
    assert targets == [{"entity_index": 3}]


def test_single_marker_far_away_returns_none() -> None:
    clusters = _make_cluster_metas((1.0, 1.0, [3]))
    all_meta = [{"entity_index": 3}]
    targets = _resolve_tap_targets(2.0, 2.0, clusters, all_meta, 0, 0.5, 0.0002)
    assert targets is None