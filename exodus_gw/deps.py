"""Functions intended for use with fastapi.Depends."""

from fastapi import Depends, Path, Request

from .auth import call_context
from .aws.client import S3ClientWrapper
from .settings import Environment, Settings, get_environment


def get_db(request: Request):
    """DB session accessor for use with FastAPI's dependency injection system."""
    return request.state.db


def get_settings(request: Request):
    return request.app.state.settings


def get_environment_from_path(
    env: str = Path(
        ...,
        title="environment",
        description="[Environment](#section/Environments) on which to operate.",
    ),  # pylint: disable=redefined-outer-name
    settings: Settings = Depends(get_settings),
):
    return get_environment(env, settings)


s3_queues = {}
import asyncio


async def queue_for_profile(profile, maxsize=3):
    queue = asyncio.LifoQueue(maxsize=maxsize)
    while not queue.full():
        client = await S3ClientWrapper(profile).__aenter__()
        queue.put_nowait(client)
    return queue


async def get_s3_client(
    request: Request,
    env: Environment = Depends(get_environment_from_path),
):
    if env.aws_profile not in s3_queues:
        s3_queues[env.aws_profile] = await queue_for_profile(env.aws_profile)

    queue = s3_queues[env.aws_profile]

    client = await queue.get()

    try:
        yield client
    finally:
        await queue.put(client)


# These are the preferred objects for use in endpoints,
# e.g.
#
#   db: Session = deps.db
#
db = Depends(get_db)
call_context = Depends(call_context)
env = Depends(get_environment_from_path)
settings = Depends(get_settings)
s3 = Depends(get_s3_client)
