"""
Simple CLI script to delete versioned keys from S3

Usage:
    impresso_commons/utils/s3_delete.py --bucket=<b> --prefix=<p>

Options:
    --bucket=<b>    Target S3 bucket
    --prefix=<p>    Prefix of keys to delete
"""

from docopt import docopt

from impresso_essentials.io.s3 import get_s3_resource
from impresso_essentials.utils import user_confirmation


def delete_versioned_keys(
    client, bucket, prefix, is_truncated=True, max_keys=1000, next_token=None
):
    """TODO"""
    while is_truncated:
        if not next_token:
            """version_list = client.list_object_versions(
                Bucket=bucket, MaxKeys=max_keys, Prefix=prefix
            )"""
            objects_list = client.list_objects_v2(
                Bucket=bucket, MaxKeys=max_keys, Prefix=prefix
            )
        else:
            objects_list = client.list_objects_v2(
                Bucket=bucket, MaxKeys=max_keys, Prefix=prefix, StartAfter=next_token
            )
            """version_list = client.list_object_versions(
                Bucket=bucket, MaxKeys=max_keys, Prefix=prefix, KeyMarker=key_marker
            )"""

        try:
            objects = [{"Key": c["Key"]} for c in objects_list["Contents"]]
            # versions = version_list["Versions"]
            # for v in versions:
            #    objects.append({"VersionId": v["VersionId"], "Key": v["Key"]})
            response = client.delete_objects(Bucket=bucket, Delete={"Objects": objects})
            print(f"Deleted {len(response['Deleted'])} keys")
        except Exception:
            pass

        """try:
            objects = []
            delete_markers = objects_list["DeleteMarkers"]  # todo fix
            for d in delete_markers:
                objects.append({"VersionId": d["VersionId"], "Key": d["Key"]})
            response = client.delete_objects(Bucket=bucket, Delete={"Objects": objects})
            print(f"Deleted {len(response['Deleted'])} keys")
        except Exception:
            pass"""

        is_truncated = objects_list["IsTruncated"]
        try:
            next_token = objects_list["NextContinuationToken"]  # todo fix!
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
