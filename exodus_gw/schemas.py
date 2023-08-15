import re
from datetime import datetime
from enum import Enum
from os.path import join, normpath
from typing import Dict, List, Optional
from uuid import UUID

from fastapi import Path
from pydantic import BaseModel, Field, model_validator

from .settings import Settings

PathPublishId = Path(
    ...,
    title="publish ID",
    description="UUID of an existing publish object.",
)

PathTaskId = Path(
    ..., title="task ID", description="UUID of an existing task object."
)


def normalize_path(path: str):
    if path:
        path = normpath(path)
        path = "/" + path if not path.startswith("/") else path
    return path


SHA256SUM_PATTERN = re.compile(r"[0-9a-f]{64}")

# TYPE/SUBTYPE[+SUFFIX][;PARAMETER=VALUE]
MIMETYPE_PATTERN = re.compile(r"^[-\w]+/[-.\w]+(\+[-\w]*)?(;[-\w]+=[-\w]+)?")

# Note: it would be preferable if we could reuse a settings object loaded by the
# app, however we need this value from within a @classmethod validator.
AUTOINDEX_FILENAME = Settings().autoindex_filename


class ItemBase(BaseModel):
    web_uri: str = Field(
        ...,
        description="URI, relative to CDN root, which shall be used to expose this object.",
    )
    object_key: str = Field(
        "",
        description=(
            "Key of blob to be exposed; should be the SHA256 checksum of a previously uploaded "
            "piece of content, in lowercase hex-digest form. \n\n"
            "Alternatively, the string 'absent' to indicate that no content shall be exposed at the given URI. "
            "Publishing an item with key 'absent' can be used to effectively delete formerly published "
            "content from the point of view of a CDN consumer."
        ),
    )
    content_type: str = Field(
        "",
        description="Content type of the content associated with this object.",
    )
    link_to: str = Field("", description="Path of file targeted by symlink.")

    @model_validator(mode="after")
    def validate_item(self) -> "ItemBase":
        web_uri = self.web_uri
        object_key = self.object_key
        content_type = self.content_type
        link_to = self.link_to
        data = self.__dict__

        if not web_uri:
            raise ValueError("No URI: %s" % data)
        self.web_uri = normalize_path(web_uri)

        if link_to and object_key:
            raise ValueError(
                "Both link target and object key present: %s" % data
            )
        if link_to and content_type:
            raise ValueError("Content type specified for link: %s" % data)

        if link_to:
            self.link_to = normalize_path(link_to)
        elif object_key:
            if object_key == "absent":
                if content_type:
                    raise ValueError(
                        "Cannot set content type when object_key is 'absent': %s"
                        % data
                    )
            elif not re.match(SHA256SUM_PATTERN, object_key):
                raise ValueError(
                    "Invalid object key; must be sha256sum: %s" % data
                )
        else:
            raise ValueError("No object key or link target: %s" % data)

        if content_type:
            # Enforce MIME type structure
            if not re.match(MIMETYPE_PATTERN, content_type):
                raise ValueError("Invalid content type: %s" % data)

        if (
            web_uri
            and AUTOINDEX_FILENAME
            and web_uri.split("/")[-1] == AUTOINDEX_FILENAME
        ):
            raise ValueError(f"Invalid URI {web_uri}: filename is reserved")

        return self


class Item(ItemBase):
    publish_id: UUID = Field(
        ..., description="Unique ID of publish object containing this item."
    )


class PublishStates(str, Enum):
    pending = "PENDING"
    committing = "COMMITTING"
    committed = "COMMITTED"
    failed = "FAILED"

    @classmethod
    def terminal(cls) -> List["PublishStates"]:
        return [cls.committed, cls.failed]


class PublishBase(BaseModel):
    id: str = Field(..., description="Unique ID of publish object.")


class Publish(PublishBase):
    env: str = Field(
        ..., description="""Environment to which this publish belongs."""
    )
    state: PublishStates = Field(
        ..., description="Current state of this publish."
    )
    updated: Optional[datetime] = Field(
        None,
        description="DateTime of last update to this publish. None if never updated.",
    )
    links: Dict[str, str] = Field(
        {}, description="""URL links related to this publish."""
    )
    items: List[Item] = Field(
        [],
        description="""All items (pieces of content) included in this publish.""",
    )

    @model_validator(mode="after")
    def make_links(self) -> "Publish":
        _self = join("/", self.env, "publish", str(self.id))
        self.links = {"self": _self, "commit": join(_self, "commit")}
        return self


class TaskStates(str, Enum):
    not_started = "NOT_STARTED"
    in_progress = "IN_PROGRESS"
    complete = "COMPLETE"
    failed = "FAILED"

    @classmethod
    def terminal(cls) -> List["TaskStates"]:
        return [cls.failed, cls.complete]


class Task(BaseModel):
    id: UUID = Field(..., description="Unique ID of task object.")
    publish_id: Optional[UUID] = Field(
        None, description="Unique ID of publish object handled by this task."
    )
    state: TaskStates = Field(..., description="Current state of this task.")
    updated: Optional[datetime] = Field(
        None,
        description="DateTime of last update to this task. None if never updated.",
    )
    deadline: Optional[datetime] = Field(
        None, description="DateTime at which this task should be abandoned."
    )
    links: Dict[str, str] = Field(
        {}, description="""URL links related to this task."""
    )

    @model_validator(mode="after")
    def make_links(self) -> "Task":
        self.links = {"self": join("/task", str(self.id))}
        return self


class AccessResponse(BaseModel):
    url: str = Field(
        description="Base URL of this CDN environment.",
        examples=["https://abc123.cloudfront.net"],
    )
    expires: str = Field(
        description=(
            "Expiration time of access information included in this "
            "response. ISO8601 UTC timestamp."
        ),
        examples=["2024-04-18T05:30Z"],
    )
    cookie: str = Field(
        description="A cookie granting access to this CDN environment.",
        examples=[
            (
                "CloudFront-Key-Pair-Id=K2266GIXCH; "
                "CloudFront-Policy=eyJTdGF0ZW1lbn...; "
                "CloudFront-Signature=kGkxpnrY9h..."
            )
        ],
    )


class MessageResponse(BaseModel):
    detail: str = Field(
        ..., description="A human-readable message with additional info."
    )


class EmptyResponse(BaseModel):
    """An empty object."""
