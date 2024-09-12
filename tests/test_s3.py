#!/usr/bin/env python3
# coding: utf-8
import pytest
from impresso_essentials.io.s3 import s3_iter_bucket

s3_iter_bucket_testdata = [
    ("11-canonical-staging", "DLE/issues/DLE-1910", "", None, "not None"),
    ("11-canonical-staging", "", ".json", None, "not None"),
    ("11-canonical-staging", "AV", ".jsonl.bz2", None, "both"),
    ("11-canonical-staging", "AV", "", lambda x: "jsonl.bz2" not in x, "empty"),
    ("21-rebuilt-staging", "DLE/DLE-1910", "", None, "not None"),
    ("21-rebuilt-staging", "DLE", "", lambda x: "1910" in x, "only one"),
    ("21-rebuilt-staging", "DLE", ".json", None, "empty"),
]


@pytest.mark.parametrize(
    "bucket,prefix,suffix,accept_key,expected", s3_iter_bucket_testdata
)
def test_impresso_iter_bucket(bucket, prefix, suffix, accept_key, expected):

    if accept_key is not None:
        iter_items = s3_iter_bucket(
            bucket_name=bucket, prefix=prefix, suffix=suffix, accept_key=accept_key
        )
    else:
        iter_items = s3_iter_bucket(bucket_name=bucket, prefix=prefix, suffix=suffix)

    if expected == "not None":
        assert iter_items is not None
        assert len(iter_items) > 0
    elif expected == "empty":
        assert len(iter_items) == 0
    elif expected == "both":
        page_files = [x for x in iter_items if "pages" in x]
        issue_files = [x for x in iter_items if "issues" in x]
        assert len(page_files) > 0
        assert len(issue_files) > 0
    elif expected == "only one":
        assert len(iter_items) == 1
