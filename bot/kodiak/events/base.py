from typing import Optional

import pydantic


class Installation(pydantic.BaseModel):
    id: int


class GithubEvent(pydantic.BaseModel):
    installation: Installation
    action: Optional[str] = None
