import asyncio
import random
from time import perf_counter

import aioboto3
import boto3

# synchronous session and client
session = boto3.Session(profile_name="boto3_101")
s3_client = session.client("s3")

# asynchronous session
async_session = aioboto3.Session(profile_name="boto3_101")


list_s3_bucket = lambda: [
    bucket["Name"] for bucket in s3_client.list_buckets()["Buckets"]
]


async def list_s3_bucket_async():
    async with async_session.client("s3") as s3:
        response = await s3.list_buckets()
        return [bucket["Name"] for bucket in response["Buckets"]]


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


async def bucket_name_generator_async(
    prefix: str = "boto3-bucket", random_digits: int = 6
) -> str:
    """ASYNC: Generate random names for aws s3 buckets in format: "prefix-random_digits",
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
    if not hasattr(bucket_name_generator_async, "names"):
        bucket_name_generator_async.names = []
    bucket_name_generator_async.names.append(bucket_name)

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


async def create_buckets_async(
    number_of_buckets: int, bucket_prefix: str, random_digits: int
) -> None:
    """ASYNC: Create aws s3 buckets with in format "prefix-random_digits" in `num` number of times.

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
    async with async_session.client("s3") as s3:
        await asyncio.gather(
            *[
                s3.create_bucket(
                    Bucket=await bucket_name_generator_async(
                        prefix=bucket_prefix, random_digits=random_digits
                    )
                )
                for _ in range(number_of_buckets)
            ]
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


async def delete_buckets_async(
    bucket_names: list, contains_name: str | None = None
) -> list:
    """ASYNC: Delete aws s3 buckets from generated bucket names and/or buckets containing the name in `contains_name` param.

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
    async with async_session.client("s3") as s3:
        # delete buckets from `bucket_names` sequence *asynchronously
        await asyncio.gather(
            *[s3.delete_bucket(Bucket=bucket_name) for bucket_name in bucket_names]
        )

        # delete buckets using `contains_name` value *asynchronously
        if contains_name:
            response = await s3.list_buckets()
            buckets_to_delete = [
                bucket["Name"]
                for bucket in response["Buckets"]
                if contains_name in bucket["Name"]
            ]
            # buckets_to_delete = [
            #     bucket for bucket in s3_buckets if contains_name in bucket
            # ]
            if not buckets_to_delete:
                raise ValueError(f"{contains_name} is not in S3 buckets.")
            await asyncio.gather(
                *[s3.delete_bucket(Bucket=bucket) for bucket in buckets_to_delete]
            )

        response = await s3.list_buckets()
        current_buckets = [bucket["Name"] for bucket in response["Buckets"]]

    bucket_names.clear()
    return current_buckets


def time_logger(fn):
    """Decorator function decorates `main()` and `main_async()` function with timer."""

    # check if free variable `fn` is a coroutine or normal function, then assign async closure or func closure
    if asyncio.iscoroutinefunction(fn):

        async def inner():
            start_time = perf_counter()
            await fn()
            print(
                "\n"
                + "\033[91m"
                + f"Time elapsed: {perf_counter() - start_time:.2f} seconds"
                + "\033[0m"
                + "\n"
            )  # red color

        return inner
    else:

        def inner():
            start_time = perf_counter()
            fn()
            print(
                "\n"
                + "\033[91m"
                + f"Time elapsed: {perf_counter() - start_time:.2f} seconds"
                + "\033[0m"
            )  # red color

        return inner


@time_logger
def main():
    print("\n" + "\033[36m" + "running synchronous functions")  # cyan color
    print(f"{'-' * 25}" + "\n" + "\033[0m")
    print(
        "pre-existed s3_buckets:", list_s3_bucket()
    )  # show current list of s3 buckets
    create_buckets(10, "boto3-bucket", 4)  # create s3 buckets
    print(
        "\n" + "generated_bucket_names before deletion:", bucket_name_generator.names
    )  # show bucket names created
    delete_buckets(bucket_name_generator.names)  # delete newly-created s3 buckets
    print(
        "\n" + "generated_bucket_names after deletion:", bucket_name_generator.names
    )  # bucket names should be empty now
    print(
        "\n" + "current s3_buckets:", list_s3_bucket()
    )  # show current list of s3 buckets


@time_logger
async def main_async():
    print("\n" + "\033[36m" + "running asynchronous functions")  # cyan color
    print(f"{'-' * 25}" + "\n" + "\033[0m")
    print(
        "pre-existed s3_buckets:", await list_s3_bucket_async()
    )  # show current list of s3 buckets
    await create_buckets_async(10, "async-boto3-bucket", 4)  # create s3 buckets
    print(
        "\n" + "generated_bucket_names before deletion:",
        bucket_name_generator_async.names,
    )  # show bucket names created
    await delete_buckets_async(
        bucket_name_generator_async.names
    )  # delete newly-created s3 buckets
    print(
        "\n" + "generated_bucket_names after deletion:",
        bucket_name_generator_async.names,
    )  # bucket names should be empty now
    print(
        "\n" + "current s3_buckets:", await list_s3_bucket_async()
    )  # show current list of s3 buckets


if __name__ == "__main__":
    main()
    asyncio.run(main_async())
