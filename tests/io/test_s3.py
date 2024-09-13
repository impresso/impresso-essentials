#!/usr/bin/env python3
# coding: utf-8
from contextlib import ExitStack
from datetime import date
import json
from unittest import mock
import pytest
import dask.bag as db
import botocore
from impresso_essentials.io import s3
from impresso_essentials.utils import IssueDir, get_pkg_resource


def test_get_storage_options():
    storage = s3.get_storage_options()
    assert "client_kwargs" in storage
    assert storage["client_kwargs"] == {
        "endpoint_url": "https://os.zhdk.cloud.switch.ch"
    }
    assert "key" in storage and storage["key"] is not None
    assert "secret" in storage and storage["secret"] is not None


get_or_create_bucket_testdata = [
    ("11-canonical-staging", False),
    ("test-bucket-canonical-sandbox", True),
]


@pytest.mark.parametrize("bucket_name,create", get_or_create_bucket_testdata)
def test_get_or_create_bucket(bucket_name, create):
    # Test fetching an existing bucket
    bucket = s3.get_or_create_bucket(bucket_name, create=create)
    assert bucket.name == bucket_name


read_jsonlines_testdata = [
    ("11-canonical-staging", "DLE/issues/DLE-1910-issues.jsonl.bz2", "issues"),
    ("11-canonical-staging", "DLE/issues/DLE-1900-issues.jsonl.bz2", None),
    (
        "11-canonical-staging",
        "DLE/pages/DLE-1910/DLE-1910-01-08-a-pages.jsonl.bz2",
        "pages",
    ),
    ("21-rebuilt-staging", "DLE/DLE-1910.jsonl.bz2", "rebuilt"),
    ("rebuilt-data", "DLE/DLE-1910.jsonl.bz2", "rebuilt"),
    ("21-rebuilt-staging", "DLE/DLE-1900.jsonl.bz2", None),
]


@pytest.mark.parametrize("bucket,key,expected", read_jsonlines_testdata)
def test_read_jsonlines(bucket, key, expected):

    try:
        lines = db.from_sequence(s3.read_jsonlines(key, bucket))
    except ValueError:
        assert expected is None
    else:
        count_lines = lines.count().compute()
        if expected == "issues":
            some_lines = lines.map(json.loads).pluck("i").take(10)
        if expected == "pages":
            some_lines = lines.map(json.loads).pluck("r").take(10)
        if expected == "rebuilt":
            some_lines = lines.map(json.loads).pluck("ppreb").take(10)

        assert count_lines is not None
        assert count_lines > 0
        assert some_lines is not None
        assert len(some_lines) > 0


readtext_jsonlines_testdata = [
    (
        "21-rebuilt-staging",
        "DLE/DLE-1910.jsonl.bz2",
        None,
        ["id", "pp", "ts", "lg", "tp", "t", "ft"],
    ),
    ("21-rebuilt-staging", "DLE/DLE-1900.jsonl.bz2", None, None),
    ("21-rebuilt-staging", "DLE/DLE-1910.jsonl.bz2", ["id", "ft"], ["id", "ft"]),
    ("21-rebuilt-staging", "DLE/DLE-1910.jsonl.bz2", ["id", "i"], ["id"]),
]


@pytest.mark.parametrize("bucket, key, to_keep, expected", readtext_jsonlines_testdata)
def test_readtext_jsonlines(bucket, key, to_keep, expected):

    try:
        lines = db.from_sequence(s3.readtext_jsonlines(key, bucket, to_keep))
    except ValueError:
        assert expected is None
    else:
        count_lines = lines.count().compute()
        some_lines = lines.map(json.loads).take(10)

        assert count_lines is not None
        assert count_lines > 0
        assert some_lines is not None
        assert len(some_lines) > 0
        assert all(list(l.keys()) == expected for l in some_lines)


@mock.patch("impresso_essentials.io.s3.get_bucket")
def test_upload_to_s3(mock_get_bucket):
    # Create a mock bucket
    mock_bucket = mock.Mock()
    mock_get_bucket.return_value = mock_bucket

    # get the files from the package
    file_mng = ExitStack()
    local_filepath = get_pkg_resource(file_mng, "data/canonical_v4-5-0.json")
    bucket_name = "10-canonical-sandbox"
    s3_path = "canonical_v4-5-0.json"

    # Test the success case
    mock_bucket.upload_file.return_value = None  # Upload succeeds

    assert s3.upload_to_s3(local_filepath, s3_path, bucket_name) is True
    mock_bucket.upload_file.assert_called_once_with(local_filepath, s3_path)

    # Test the failure case
    mock_bucket.upload_file.side_effect = Exception("Upload failed")  # Upload fails
    assert s3.upload_to_s3(local_filepath, s3_path, bucket_name) is False


@mock.patch("impresso_essentials.io.s3.get_s3_resource")
def test_get_bucket(mock_get_s3_resource):
    # Create a mock S3 resource
    mock_s3_resource = mock.Mock()
    mock_bucket = mock.Mock()
    mock_s3_resource.Bucket.return_value = mock_bucket
    mock_get_s3_resource.return_value = mock_s3_resource

    # Call the function
    result = s3.get_bucket("10-canonical-sandbox")

    # Assert that the correct bucket was retrieved
    mock_s3_resource.Bucket.assert_called_once_with("10-canonical-sandbox")
    assert result == mock_bucket


fixed_s3fs_glob_testdata = [
    (
        "11-canonical-staging",
        "DLE/issues/DLE-1910*",
        None,
        ["s3://11-canonical-staging/DLE/issues/DLE-1910-issues.jsonl.bz2"],
    ),
    (
        "11-canonical-staging",
        "Charivari/issues/Charivari-184*",
        None,
        [
            "s3://11-canonical-staging/Charivari/issues/Charivari-1840-issues.jsonl.bz2",
            "s3://11-canonical-staging/Charivari/issues/Charivari-1841-issues.jsonl.bz2",
        ],
    ),
    (
        None,
        "s3://11-canonical-staging/DLE/issues/DLE-1910*",
        None,
        ["s3://11-canonical-staging/DLE/issues/DLE-1910-issues.jsonl.bz2"],
    ),
    ("11-canonical-staging", "", ".json", "canonical_v"),
    ("11-canonical-staging", "DLE/*", "DLE-1910", "both"),
    ("11-canonical-staging", "DLE/issues/*", None, 6),
]


@pytest.mark.parametrize("bucket,path,suffix,expected", fixed_s3fs_glob_testdata)
def test_fixed_s3fs_glob(bucket, path, suffix, expected):

    if bucket:
        bucket = s3.get_bucket(bucket)
    # Call the function
    result = s3.fixed_s3fs_glob(path, suffix, bucket)

    if isinstance(expected, list):
        assert all(r in expected for r in result)
        print(result)
        assert len(result) == len(expected)
    elif isinstance(expected, int):
        assert len(result) == expected
    else:
        assert isinstance(expected, str)
        print(result)
        if expected == "both":
            assert all("issues" in r or "pages" in r for r in result)
            assert any("issues" in r for r in result)
            assert any("pages" in r for r in result)


s3_glob_with_size_testdata = [
    # found these values on Cyberduck
    ("s3://11-canonical-staging/Charivari/issues*", lambda x: x[1] > 1e-3),
    ("s3://11-canonical-staging/canonical_v*.json", lambda x: x[1] > 1),
    ("s3://11-canonical-staging/Charivari/pages*", lambda x: x[1] > 0.01),
    ("s3://11-canonical-staging/Charivari/pages/Charivari-19*", None),
]


@pytest.mark.parametrize("path,expected", s3_glob_with_size_testdata)
def test_s3_glob_with_size(path, expected):
    result = s3.s3_glob_with_size(path)

    if expected is None:
        assert len(result) == 0
    else:
        assert all(expected(r) for r in result)


@mock.patch("impresso_essentials.io.s3.s_open")
@mock.patch("boto3.Session")
def test_alternative_read_text(mock_boto_session, mock_s_open):
    # Mock the session client
    mock_client = mock.Mock()
    mock_boto_session.return_value.client.return_value = mock_client

    # Mock the smart_open function
    mock_file = mock.Mock()
    mock_file.readlines.return_value = ["line1\n", "line2\n"]
    mock_s_open.return_value.__enter__.return_value = mock_file

    # Call the function
    result = s3.alternative_read_text(
        "s3://22-rebuilt-final/GDL/GDL-1950.jsonl.bz2",
        s3.IMPRESSO_STORAGEOPT,
    )

    # Assert that it returns the correct lines
    assert result == ["line1\n", "line2\n"]
    mock_s_open.assert_called_once_with(
        "s3://22-rebuilt-final/GDL/GDL-1950.jsonl.bz2",
        "r",
        transport_params={"client": mock_client},
    )


@mock.patch("impresso_essentials.io.s3.get_s3_client")
def test_list_s3_directories(mock_get_s3_client):
    # Mock the client and list_objects_v2 response
    mock_s3 = mock.Mock()
    mock_get_s3_client.return_value = mock_s3
    mock_s3.list_objects_v2.return_value = {
        "CommonPrefixes": [{"Prefix": "Charivari-1940/"}, {"Prefix": "Charivari-1941/"}]
    }

    # Call the function
    result = s3.list_s3_directories("11-canonical-staging", "Charivari/pages")

    # Check that the result is as expected
    mock_s3.list_objects_v2.assert_called_once_with(
        Bucket="11-canonical-staging", Prefix="Charivari/pages", Delimiter="/"
    )
    assert result == ["Charivari-1940", "Charivari-1941"]


@mock.patch("impresso_essentials.io.s3.get_s3_client")
def test_get_s3_object_size(mock_get_s3_client):
    # Mock the S3 client and its head_object method
    mock_s3 = mock.Mock()
    mock_get_s3_client.return_value = mock_s3
    mock_s3.head_object.return_value = {"ContentLength": 1024}

    # Call the function
    result = s3.get_s3_object_size("11-canonical-staging", "GDL/GDL-1950.jsonl.bz2")

    # Assertions
    mock_s3.head_object.assert_called_once_with(
        Bucket="11-canonical-staging", Key="GDL/GDL-1950.jsonl.bz2"
    )
    assert result == 1024


@mock.patch("impresso_essentials.io.s3.get_s3_client")
def test_get_s3_object_size_client_error(mock_get_s3_client):
    # Mock the S3 client to raise a ClientError
    mock_s3 = mock.Mock()
    mock_get_s3_client.return_value = mock_s3
    mock_s3.head_object.side_effect = botocore.exceptions.ClientError(
        {"Error": {"Code": "404"}}, "head_object"
    )

    # Call the function
    result = s3.get_s3_object_size("11-canonical-staging", "my_key")

    # Assertions
    mock_s3.head_object.assert_called_once_with(
        Bucket="11-canonical-staging", Key="my_key"
    )
    assert result is None


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
def test_s3_iter_bucket(bucket, prefix, suffix, accept_key, expected):

    if accept_key is not None:
        iter_items = s3.s3_iter_bucket(
            bucket_name=bucket, prefix=prefix, suffix=suffix, accept_key=accept_key
        )
    else:
        iter_items = s3.s3_iter_bucket(bucket_name=bucket, prefix=prefix, suffix=suffix)

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


read_s3_issues_testdata = [
    ("11-canonical-staging", "DLE", "1910", "not None"),
    ("11-canonical-staging", "DLE", "1900", []),
    (
        "11-canonical-staging",
        "DLE",
        "1910",
        IssueDir(
            journal="DLE",
            date=date(1910, 1, 1),
            edition="a",
            path="s3://11-canonical-staging/DLE/issues/DLE-1910-issues.jsonl.bz2",
        ),
    ),
    (
        "11-canonical-staging",
        "DLE",
        "1910",
        {"id": "DLE-1910-01-01-a", "cdt": "2019-10-07 22:56:18"},
    ),
]


@pytest.mark.parametrize("bucket,newspaper,year,expected", read_s3_issues_testdata)
def test_read_s3_issues(bucket, newspaper, year, expected):

    result = s3.read_s3_issues(newspaper, year, bucket)

    if expected == "not None":
        assert result is not None
        assert len(result) > 1
        assert all(f"{newspaper}-{year}" in r[1]["id"] for r in result)
    elif isinstance(expected, list):
        assert len(result) == len(expected)
    elif isinstance(expected, IssueDir):
        assert result[0][0] == expected
    elif isinstance(expected, dict):
        assert result[0][1]["id"] == expected["id"]
        assert result[0][1]["cdt"] == expected["cdt"]


@mock.patch("impresso_essentials.io.s3.get_s3_client")
def test_list_newspapers(mock_get_s3_client):
    # Mock the S3 client and paginator
    mock_s3 = mock.Mock()
    mock_get_s3_client.return_value = mock_s3
    mock_paginator = mock.Mock()
    mock_s3.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [
        {"Contents": [{"Key": "ACI/issues/"}, {"Key": "BLB/issues/"}]}
    ]

    # Call the function
    result = s3.list_newspapers("11-canonical-staging")

    # Assertions
    mock_s3.get_paginator.assert_called_once_with("list_objects")
    assert result == ["ACI", "BLB"]


@mock.patch("impresso_essentials.io.s3.list_newspapers")
@mock.patch("impresso_essentials.io.s3.fixed_s3fs_glob")
def test_list_files(mock_fixed_s3fs_glob, mock_list_newspapers):
    # Mock the newspaper listing and glob output
    mock_list_newspapers.return_value = ["ACI", "BLB"]
    mock_fixed_s3fs_glob.side_effect = [
        ["ACI/issues/file1.jsonl.bz2", "ACI/issues/file2.jsonl.bz2"],
        ["BLB/issues/file3.jsonl.bz2"],
    ]

    # Call the function
    result = s3.list_files("11-canonical-staging", "issues")

    # Assertions
    mock_list_newspapers.assert_called_once_with("11-canonical-staging")
    mock_fixed_s3fs_glob.assert_any_call("11-canonical-staging/ACI/issues/*")
    assert result == (
        [
            "ACI/issues/file1.jsonl.bz2",
            "ACI/issues/file2.jsonl.bz2",
            "BLB/issues/file3.jsonl.bz2",
        ],
        None,
    )


@mock.patch("impresso_essentials.io.s3.list_newspapers")
@mock.patch("impresso_essentials.io.s3.fixed_s3fs_glob")
def test_list_files_pages(mock_fixed_s3fs_glob, mock_list_newspapers):
    # Mock the newspaper listing and glob output
    mock_list_newspapers.return_value = ["ACI", "BLB"]
    mock_fixed_s3fs_glob.side_effect = [
        ["ACI/pages/file1.jsonl.bz2", "ACI/pages/file2.jsonl.bz2"],
        ["BLB/pages/file3.jsonl.bz2"],
    ]

    # Call the function
    result = s3.list_files("11-canonical-staging", "pages")

    # Assertions
    mock_list_newspapers.assert_called_once_with("11-canonical-staging")
    mock_fixed_s3fs_glob.assert_any_call("11-canonical-staging/ACI/pages/*")
    assert result == (
        None,
        [
            "ACI/pages/file1.jsonl.bz2",
            "ACI/pages/file2.jsonl.bz2",
            "BLB/pages/file3.jsonl.bz2",
        ],
    )


@mock.patch("impresso_essentials.io.s3.list_files")
@mock.patch("impresso_essentials.io.s3.db.read_text")
def test_fetch_files(mock_read_text, mock_list_files):
    # Mock the list_files and Dask read_text methods
    mock_list_files.return_value = (
        ["file1.jsonl.bz2", "file2.jsonl.bz2"],  # issues
        ["page1.jsonl.bz2", "page2.jsonl.bz2"],  # pages
    )
    mock_issue_bag = mock.Mock()
    mock_page_bag = mock.Mock()
    mock_read_text.side_effect = [mock_issue_bag, mock_page_bag]

    # Call the function with compute=False
    result = s3.fetch_files("11-canonical-staging", compute=False, file_type="both")

    # Assertions
    mock_list_files.assert_called_once_with("11-canonical-staging", "both", None)
    mock_read_text.assert_any_call(
        ["file1.jsonl.bz2", "file2.jsonl.bz2"], storage_options=s3.IMPRESSO_STORAGEOPT
    )
    mock_read_text.assert_any_call(
        ["page1.jsonl.bz2", "page2.jsonl.bz2"], storage_options=s3.IMPRESSO_STORAGEOPT
    )
    assert result == (mock_issue_bag, mock_page_bag)
