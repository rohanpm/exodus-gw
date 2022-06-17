import asyncio
import gzip
import hashlib
import logging
from typing import AsyncGenerator, Optional

from repo_autoindex import Fetcher, GeneratedIndex, autoindex
from sqlalchemy import inspect
from sqlalchemy.orm import Session

from exodus_gw.aws.client import S3ClientWrapper
from exodus_gw.models import Item, Publish
from exodus_gw.settings import Environment, Settings, get_environment

LOG = logging.getLogger("exodus-gw")
INDEX_FILENAME = "__exodus_autoindex__"


def object_key(content: bytes) -> str:
    hasher = hashlib.sha256()
    hasher.update(content)
    return hasher.hexdigest().lower()


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


class AutoindexEnricher:
    def __init__(self, publish: Publish, env_name: str, settings: Settings):
        self.publish = publish
        self.env_name = env_name
        self.env = get_environment(env_name, settings)
        self.settings = settings
        self.db = inspect(publish).session
        self.item_query = self.db.query(Item).filter(
            Item.publish_id == publish.id
        )

    @property
    def repomd_xml_items(self) -> list[Item]:
        return self.item_query.filter(
            Item.web_uri.like("%/repodata/repomd.xml")
        ).all()

    @property
    def uris_for_autoindex(self) -> list[str]:
        out = []

        for item in self.repomd_xml_items:
            base_repo_uri = item.web_uri[: -len("/repodata/repomd.xml")]
            index_uri = f"{base_repo_uri}/{INDEX_FILENAME}"
            if self.item_query.filter(Item.web_uri == index_uri).count():
                LOG.debug("Index at %s already exists", index_uri)
            else:
                out.append(base_repo_uri)

        return out

    def fetcher_for_client(self, s3_client) -> PublishContentFetcher:
        return PublishContentFetcher(
            db=self.db,
            publish=self.publish,
            s3_client=s3_client,
            environment=self.env,
        )

    async def autoindex_items(
        self, s3_client, fetcher: Fetcher, base_uri: str
    ) -> AsyncGenerator[Item, None]:
        async for idx in autoindex(
            base_uri,
            fetcher=fetcher,
        ):
            index_uri_components = [base_uri]
            if idx.relative_dir:
                index_uri_components.append(idx.relative_dir)
            index_uri_components.append(INDEX_FILENAME)
            web_uri = "/".join(index_uri_components)

            content_bytes = idx.content.encode("utf-8")
            content_key = object_key(content_bytes)

            LOG.info(
                "%s: adding autoindex %s => %s",
                self.publish.id,
                web_uri,
                content_key,
            )

            response = await s3_client.put_object(
                Body=idx.content.encode("utf-8"),
                Bucket=self.env.bucket,
                Key=content_key,
            )
            LOG.debug("Upload response for %s: %s", web_uri, response)

            item = Item(
                web_uri=web_uri,
                object_key=content_key,
                content_type="text/html; charset=UTF-8",
                publish_id=self.publish.id,
            )
            yield item

    async def run(self):
        LOG.debug("Starting autoindex for publish %s", self.publish.id)

        uris = self.uris_for_autoindex
        LOG.debug(
            "Found %d path(s) eligible for autoindex:\n  %s",
            len(uris),
            "\n  ".join(uris),
        )

        async with S3ClientWrapper(profile=self.env.aws_profile) as s3_client:
            fetcher = self.fetcher_for_client(s3_client)
            for base_uri in uris:
                async for item in self.autoindex_items(
                    s3_client, fetcher, base_uri
                ):
                    self.db.add(item)
                    self.db.commit()
