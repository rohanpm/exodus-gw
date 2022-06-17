import asyncio
import logging

from repo_autoindex import Fetcher, GeneratedIndex, autoindex
from sqlalchemy.orm import Session
from sqlalchemy import inspect

from ..models import Item, Publish

LOG = logging.getLogger("exodus-gw")
INDEX_FILENAME = "index.html"


def add_autoindex(db: Session, publish_id: str, env: str):
    LOG.debug("Starting autoindex for publish %s", publish_id)

    repomd_xml_items: list[Item] = (
        db.query(Item)
        .filter(
            Item.publish_id == publish_id,
            Item.web_uri.like("%/repodata/repomd.xml"),
        )
        .all()
    )

    # items are only eligible if there's not already an index in the publish.
    eligible_repo_uris = []
    for item in repomd_xml_items:
        base_repo_uri = item.web_uri[: -len("/repodata/repomd.xml")]
        index_uri = f"{base_repo_uri}/{INDEX_FILENAME}"
        if (
            db.query(Item)
            .filter(Item.publish_id == publish_id, Item.web_uri == index_uri)
            .exists()
        ):
            LOG.debug("Index at %s already exists", index_uri)
        else:
            eligible_repo_uris.append(base_repo_uri)

    LOG.debug(
        "Found %d path(s) eligible for autoindex:\n  %s",
        len(eligible_repo_uris),
        "\n  ".join(eligible_repo_uris),
    )

    # TODO: implement me
