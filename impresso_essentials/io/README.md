# How to run S3 I/O scripts

Mutliple scripts in the IO module which allow to perform operations on a given partition of our S3 storage.
Impresso uses a very specific file structure on S3, which has recently been updated:

- A protocol has been devised to switch from one release to the preparation of the next, without losing any information nor blocking processes which might still call for the "older version" of the data.
  - In particular, the data within a bucket partition should be copied to the corresponding partition in a bucket corresponding to the next release.
  - In order to keep the information relative to the creation or last modification of data being copied, it was decided to use S3's metadata object.
  - Hence, a metadata object should be attached to all data files, **containing the latest creation timestamp** found in the data contained in this file
  - This process is done with the [s3_set_timestamp.py](https://github.com/impresso/impresso-essentials/blob/main/impresso_essentials/io/s3_set_timestamp.py) script.
  - Once the the data is attached to its timestamps, it can be copied to its new destination, keeping the metadata object attached with the following:
    ```
    rclone copy --progress --metadata --exclude="*.json" s3-impresso:[PREV-RELEASE_BUCKET-NAME]/ s3-impresso:[NEXT-RELEASE_BUCKET-NAME]/
    ``` 
- The "provider level" was added to the file structure within the partition of each format by aggregating all the media aliases belonging to the same provider under the same directory, allowing us to have a clearer view of the data.
  - This process is meant to be applied only once, since all S3 and I/O helper functions have been adapted to handle and generate data with this newly added level.
  - The script [s3_add_provider.py](https://github.com/impresso/impresso-essentials/blob/main/impresso_essentials/io/s3_add_provider.py) allows to copy data inplace or to a different destination, adding this new level.

## How to run `s3_set_timestamp.py`

Usage of the script:
```
Usage:
    python s3_set_timestamp.py --s3-file s3://bucket/path/file.jsonl.bz2 \
        --metadata-key impresso-last-ts \
        --ts-key ts \
        --all-lines \
        --output s3://bucket/path/output.jsonl
    python s3_set_timestamp.py --s3-prefix s3://bucket/path/ \
        --metadata-key impresso-last-ts \
        --ts-key ts \
        --all-lines
Arguments:
    --s3-prefix: The S3 prefix to process multiple .jsonl.bz2 files.
    --s3-file: The S3 URI of a single .jsonl.bz2 file to process.
    --metadata-key: The metadata key to update with the latest timestamp
        (default: impresso-last-ts).
    --ts-key: The key in the JSONL records to extract the timestamp from
        (default: ts).
    --all-lines: If False, only the first timestamp is considered.
    --output: Optional S3 URI for the output file with updated metadata (only for --s3-file).
    --force: Force reprocessing even if metadata is already up-to-date (default: False).
```

- The script can be used to add the metadata in two main modes:
  - to one single file: using the `--s3-file` parameter
  - to all the files within a partition: using the `--s3-prefix` parameter
- The most important parameter to ensure is correct is `--ts-key`. This should match the property name containing a timestamp in your data.
  - It defaults to `ts`, but other values previously encountered are `cdt`, and `timestamp`. We are aiming towards a unification of this field.

This script can be run from the CLI or as a module as long as you have the impresso-essentials package installed.

In general, this script **should be used whenever data needs to be moved or copied to a different destination on S3**, since these operations do not maintain creation time information.

Indeed, some process in the Impresso pipeline directly rely on the creation/last modification dates to trigger the (re)computation of subsequent data stages. Hence keeping the timestamp metadata of files on S3 correct and up to date is good practice.

## How to run `s3_add_provider.py`

Usage of the script:
```
Usage:
    s3_add_provider.py --s3-partition-path=<pp> --log-file=<lf> [--dest-bucket=<db> --remove-src-keys --no-copy --verbose]

Options:
--s3-partition-path=<pp>  S3 path to the partition to which the provider level should be added.
    Corresponds to the last partition before the list of media titles - where to add the provider.
    Eg. ""s3://122-rebuilt-final" or "s3://142-processed-data-final/langident/langident_v1-4-4"
--log-file=<lf>  Path to log file to use.
--dest-bucket=<db>  Destination bucket in which to copy the data with the provider layer. If not
    defined, will default to the input bucket (corresponding to simply adding the provider).
    Eg. "122-rebuilt-staging".
--remove-src-keys   Whether to remove the source keys which don't have the provider after performing the
    addition and/or copy. If True, the old keys without the provider will be removed. Defaults to False.
--no-copy   Launch the scrip in debug mode - will not perform copies but list which would have been done
--verbose   Set logging level to DEBUG (by default is INFO).
```

- The provider data can either be added "in place" or as the data is copied to another destination.
  - To perform this addition in place: leave the `--dest-bucket` undefined and activate the `--remove-src-keys` flag:
    ```
    s3_add_provider.py --s3-partition-path=s3://path/to/bucket/or/partition --log-file=/path/to/logfile.log --remove-src-keys
    ```
  - To directly copy the data with the added provider level to another location (Eg. the corresponding bucket for the next release): define the `--dest-bucket`:
    ```
    s3_add_provider.py --s3-partition-path=s3://path/to/bucket/or/partition --log-file=/path/to/logfile.log --dest-bucket=s3://path/to/new/bucket/or/partition
    ```
  - In general, the flag `--no-copy` can always be set first to ensure the rest of the parameters are correct and the script has the desired behavior. This is recommended as a sanitycheck.
 
Note that this script is especially useful during the transition period between the two file-structure organizations. As soon as all the data stored on S3 is organized according to the newly defined convention, this script won't be necessary anymore.

## Example use-cases

Hence to copy a specific dataset which was just released to the corresponding bucket for the next release in preparation, the following steps could be taken:
1. Running `s3_set_timestamp.py` to ensure we can keep track of when the data was generated.
2. If the existing data does *not* have the provider level yet, running `s3_add_provider.py`, directly adding

(TODO finish)
