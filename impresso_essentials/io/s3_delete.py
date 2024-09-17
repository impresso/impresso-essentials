"""
Simple CLI script to delete keys from S3.

Usage:
    impresso_commons/utils/s3_delete.py --bucket=<b> --prefix=<p>

Options:
    --bucket=<b>    Target S3 bucket
    --prefix=<p>    Prefix of keys to delete
"""

from docopt import docopt
from botocore.client import BaseClient
from impresso_essentials.io.s3 import get_s3_resource
from impresso_essentials.utils import user_confirmation

# from impresso_commons.utils.s3 import get_s3_resource
# from impresso_commons.utils import user_confirmation


def delete_versioned_keys(
    client: BaseClient,
    bucket: str,
    prefix: str,
    max_keys: int = 1000,
):
    """Delete all the keys within a bucket based on a given prefix.

    Args:
        client (BaseClient): S3 client.
        bucket (str): Name of the bucket to delete keys from.
        prefix (str): Prefix to the partition from which to delete keys.
        max_keys (int, optional): Max number of keys to delete at once. Defaults to 1000.
    """
    # initialize the first values of is_truncated and next_token.
    next_token = None
    is_truncated = True
    while is_truncated:
        # list the objects to delete from the bucket's partition
        if not next_token:
            objects_list = client.list_objects_v2(
                Bucket=bucket, MaxKeys=max_keys, Prefix=prefix
            )
        else:
            objects_list = client.list_objects_v2(
                Bucket=bucket, MaxKeys=max_keys, Prefix=prefix, StartAfter=next_token
            )

        # delete identified objects
        try:
            objects = [{"Key": c["Key"]} for c in objects_list["Contents"]]
            response = client.delete_objects(Bucket=bucket, Delete={"Objects": objects})
            print(f"Deleted {len(response['Deleted'])} keys")
        except Exception:
            pass

        # if more keys remain in the partition, continue process
        is_truncated = objects_list["IsTruncated"]
        try:
            next_token = objects_list["NextContinuationToken"]
        except KeyError:
            print("Done!")


def main():
    args = docopt(__doc__)
    b = args["--bucket"]
    p = args["--prefix"]
    q_1 = f"all keys with prefix `{p}`"
    q = f"\nAre you sure you want to delete {q_1} from bucket `s3://{b}` ?"

    if user_confirmation(question=q):
        print("Ok, let's start (it will take a while!)")
        s3_client = get_s3_resource().meta.client
        delete_versioned_keys(client=s3_client, bucket=b, prefix=p)
    else:
        print("Ok then, see ya!")


if __name__ == "__main__":
    main()
