from __future__ import annotations

from typing import Optional

import pydantic

from kodiak.events.base import GithubEvent


class Ref(pydantic.BaseModel):
    ref: str


class HeadRef(pydantic.BaseModel):
    ref: str
    sha: Optional[str] = None


class PullRequest(pydantic.BaseModel):
    number: int
    base: Ref
    head: Optional[HeadRef] = None


class Owner(pydantic.BaseModel):
    login: str


class Repository(pydantic.BaseModel):
    name: str
    owner: Owner


class ReviewUser(pydantic.BaseModel):
    login: str


class Review(pydantic.BaseModel):
    user: Optional[ReviewUser] = None


class PullRequestReviewEvent(GithubEvent):
    """
    https://developer.github.com/v3/activity/events/types/#pullrequestreviewevent
    """

    pull_request: PullRequest
    repository: Repository
    review: Optional[Review] = None
