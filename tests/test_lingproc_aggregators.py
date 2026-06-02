import dask.bag as db

from impresso_essentials.versioning.aggregators import (
    compute_stats_in_lingproc_bag,
    extract_lingproc_ci_id,
)


def test_extract_lingproc_ci_id_prefers_ci_id_without_decoding_payload():
    line = (
        '{"id":"nested-looking-id","sents":[{"tokens":[{"id":"token-1"}]}],'
        '"ci_id":"NZZ-1900-01-01-a-i0001"}'
    )

    assert extract_lingproc_ci_id(line) == "NZZ-1900-01-01-a-i0001"


def test_compute_stats_in_lingproc_bag_accepts_ci_id_strings():
    records = db.from_sequence(
        [
            "NZZ-1900-01-01-a-i0001",
            "NZZ-1900-01-01-a-i0002",
            "NZZ-1900-01-02-a-i0001",
            "GDL-1901-01-01-a-i0001",
        ],
        npartitions=2,
    )

    assert compute_stats_in_lingproc_bag(records, title="test") == [
        {
            "media_alias": "GDL",
            "year": "1901",
            "issues": 1,
            "content_items_out": 1,
        },
        {
            "media_alias": "NZZ",
            "year": "1900",
            "issues": 2,
            "content_items_out": 3,
        },
    ]
