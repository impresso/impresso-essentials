import dask.bag as db

from impresso_essentials.versioning.aggregators import compute_stats_in_lingproc_bag
from impresso_essentials.versioning.compute_manifest import extract_ci_id_only


def test_extract_ci_id_only():
    assert extract_ci_id_only('{"ci_id":"NZZ-1900-01-01-a-i0001","tokens":[1,2]}') == (
        "NZZ-1900-01-01-a-i0001"
    )
    assert extract_ci_id_only('{"id":"IMP-1901-01-01-a-i0002","meta":{"x":1}}') == (
        "IMP-1901-01-01-a-i0002"
    )
    assert extract_ci_id_only('{"tokens":[1,2]}') is None


def test_compute_stats_in_lingproc_bag_accepts_ci_id_strings():
    lingproc_records = db.from_sequence(
        [
            "NZZ-1900-01-01-a-i0001",
            "NZZ-1900-01-01-a-i0002",
            {"ci_id": "NZZ-1900-01-02-a-i0001"},
            {"id": "IMP-1901-01-01-a-i0001"},
            {"tokens": []},
        ],
        npartitions=2,
    )

    stats = compute_stats_in_lingproc_bag(lingproc_records, title="test")

    assert stats == [
        {"media_alias": "IMP", "year": "1901", "issues": 1, "content_items_out": 1},
        {"media_alias": "NZZ", "year": "1900", "issues": 2, "content_items_out": 3},
    ]
