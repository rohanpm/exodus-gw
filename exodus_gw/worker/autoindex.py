import asyncio
import gzip
import logging
from typing import Optional

from repo_autoindex import Fetcher, GeneratedIndex, autoindex
from sqlalchemy import inspect
from sqlalchemy.orm import Session

from exodus_gw.aws.client import S3ClientWrapper
from exodus_gw.models import Item, Publish
from exodus_gw.settings import Environment, Settings

LOG = logging.getLogger("exodus-gw")
INDEX_FILENAME = "index.html"


class PublishContentFetcher:
    def __init__(
        self,
        db: Session,
        publish: Publish,
        s3_client,
        environment: Environment,
    ):
        self.db = db
        self.publish = publish
        self.s3_client = s3_client
        self.environment = environment

    async def __call__(self, uri: str) -> Optional[str]:
        LOG.debug("Requested to fetch: %s", uri)

        matched = (
            self.db.query(Item)
            .filter(Item.publish_id == self.publish.id, Item.web_uri == uri)
            .all()
        )
        if not matched:
            LOG.debug("%s: no content available", uri)
            return None

        item: Item = matched[0]
        key: str = item.object_key
        LOG.debug("%s can be fetched from %s", uri, key)
        response = await self.s3_client.get_object(
            Bucket=self.environment.bucket, Key=key
        )
        LOG.debug("S3 response: %s", response)

        content_type: str = response["ResponseMetadata"]["HTTPHeaders"][
            "content-type"
        ]
        content: bytes = await response["Body"].read()

        if uri.endswith(".gz") and content_type in (
            "binary/octet-stream",
            "application/octet-stream",
            "application/x-gzip",
        ):
            content = gzip.decompress(content)

        return content.decode("utf-8")


async def add_autoindex(publish: Publish, env: str, settings: Settings):
    LOG.debug("Starting autoindex for publish %s", publish.id)

    db: Session = inspect(publish).session
    item_query = db.query(Item).filter(Item.publish_id == publish.id)

    repomd_xml_items: list[Item] = item_query.filter(
        Item.web_uri.like("%/repodata/repomd.xml")
    ).all()

    # items are only eligible if there's not already an index in the publish.
    eligible_repo_uris = []
    for item in repomd_xml_items:
        base_repo_uri = item.web_uri[: -len("/repodata/repomd.xml")]
        index_uri = f"{base_repo_uri}/{INDEX_FILENAME}"
        if item_query.filter(Item.web_uri == index_uri).count():
            LOG.debug("Index at %s already exists", index_uri)
        else:
            eligible_repo_uris.append(base_repo_uri)

    LOG.debug(
        "Found %d path(s) eligible for autoindex:\n  %s",
        len(eligible_repo_uris),
        "\n  ".join(eligible_repo_uris),
    )

    # TODO: better handle non-matching case (get_environment)
    environment = [e for e in settings.environments if e.name == env][0]
    async with S3ClientWrapper(profile=environment.aws_profile) as client:
        fetcher = PublishContentFetcher(db, publish, client, environment)
        for uri in eligible_repo_uris:
            await add_autoindex_to_repo(publish, uri, fetcher)


async def add_autoindex_to_repo(publish: Publish, uri: str, fetcher: Fetcher):
    async for idx in autoindex(
        uri,
        fetcher=fetcher,
        # TODO: this is just for testing, make it None later
        index_href_suffix="index.html",
    ):
        LOG.info("Would add autoindex: %s", idx)
