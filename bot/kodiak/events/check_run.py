from typing import List, Optional

import pydantic

from kodiak.events.base import GithubEvent


class PullRequestRepository(pydantic.BaseModel):
    id: int


class Ref(pydantic.BaseModel):
    ref: str
    repo: PullRequestRepository


class PullRequest(pydantic.BaseModel):
    number: int
    base: Ref
    draft: bool = False


class CheckRunApp(pydantic.BaseModel):
    id: int


class CheckRun(pydantic.BaseModel):
    name: str
    head_sha: Optional[str] = None
    pull_requests: List[PullRequest]
    app: Optional[CheckRunApp] = None


class Owner(pydantic.BaseModel):
    login: str


class Repository(pydantic.BaseModel):
    id: int
    name: str
    owner: Owner


class CheckRunEvent(GithubEvent):
    """
    https://developer.github.com/v3/activity/events/types/#checkrunevent
    """

    check_run: CheckRun
    repository: Repository
