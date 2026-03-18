from typing import Optional

import pydantic

from kodiak.events.base import GithubEvent


class Owner(pydantic.BaseModel):
    login: str


class Repository(pydantic.BaseModel):
    name: str
    owner: Owner


class Ref(pydantic.BaseModel):
    ref: str


class HeadRef(pydantic.BaseModel):
    ref: str
    sha: Optional[str] = None


class PullRequest(pydantic.BaseModel):
    base: Ref
    head: Optional[HeadRef] = None


class PullRequestEvent(GithubEvent):
    """
    https://developer.github.com/v3/activity/events/types/#pullrequestevent
    """

    number: int
    pull_request: PullRequest
    repository: Repository
