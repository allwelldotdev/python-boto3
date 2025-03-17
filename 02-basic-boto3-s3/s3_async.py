import random
from time import perf_counter

import boto3

session = boto3.Session(profile_name="boto3_101")
s3_client = session.client("s3")

list_s3_bucket = lambda: [
    bucket["Name"] for bucket in s3_client.list_buckets()["Buckets"]
]


def bucket_name_generator(prefix: str = "boto3-bucket", random_digits: int = 6) -> str:
    """Generate random names for aws s3 buckets in format: "prefix-random_digits",
    then create state that holds bucket names generated via `.names` attr.

    Parameters
    ----------
    `prefix` : str
        Bucket name prefix.
    `random_digits` : int
        Bucket name suffix i.e. generated random number with specified number of digits.

    Returns
    -------
    str
        Bucket name.
    """
    rn = random.randrange(10 ** (random_digits - 1), 10 ** (random_digits))
    bucket_name = f"{prefix}-{rn}"

    # create state that holds bucket names generated
    if not hasattr(bucket_name_generator, "names"):
        bucket_name_generator.names = []
    bucket_name_generator.names.append(bucket_name)

    return bucket_name


def create_buckets(
    number_of_buckets: int, bucket_prefix: str, random_digits: int
) -> None:
    """Create aws s3 buckets with in format "prefix-random_digits" in `num` number of times.

    Parameters
    ----------
    `number_of_buckets` : int
        Number of buckets to create in AWS S3 through iteration.
    `bucket_prefix` : str
        Bucket name prefix.
    `random_digits` : int
        Bucket name suffix i.e. generated random number with specified number of digits.

    Returns
    -------
    None
    """
    for _ in range(number_of_buckets):
        s3_client.create_bucket(
            Bucket=bucket_name_generator(
                prefix=bucket_prefix, random_digits=random_digits
            )
        )


def delete_buckets(bucket_names: list, contains_name: str | None = None) -> list:
    """Delete aws s3 buckets from generated bucket names and/or buckets containing the name in `contains_name` param.

    Also cleans the `bucket_names` container after bucket deletion.

    Parameters
    ----------
    `bucket_names` : list
        Buckets names to be deleted from AWS S3 bucket. These bucket names must already exist in AWS S3.
    `contains_name` : str | None
        Delete bucket names not included in `bucket_names`. Defaults to `None`

    Returns
    -------
    list
        Current AWS S3 buckets.
    """

    # delete buckets from `bucket_names` seq
    for _, bucket_name in enumerate(bucket_names):
        s3_client.delete_bucket(Bucket=bucket_name)

    s3_buckets = lambda: [
        bucket["Name"] for bucket in s3_client.list_buckets()["Buckets"]
    ]

    # delete buckets using `contains_name` value
    if contains_name:
        for bucket_name in s3_buckets():
            if contains_name in bucket_name:
                s3_client.delete_bucket(Bucket=bucket_name)
            else:
                raise ValueError(f"{contains_name} is not in S3 buckets.")

    bucket_names.clear()

    return s3_buckets()


def time_logger(fn):
    """Decorator function decorates `main()` function with timer."""

    def inner():
        start_time = perf_counter()
        # result = fn()
        fn()
        print(f"Time elapsed: {perf_counter() - start_time} seconds")
        # return result

    return inner


@time_logger
def main():
    print(list_s3_bucket())  # show current list of s3 buckets
    create_buckets(10, "boto3-bucket", 4)  # create s3 buckets
    print(bucket_name_generator.names)  # show bucket names created
    delete_buckets(bucket_name_generator.names)  # delete newly-created s3 buckets
    print(list_s3_bucket())  # show current list of s3 buckets


if __name__ == "__main__":
    main()
