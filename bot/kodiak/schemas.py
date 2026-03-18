from __future__ import annotations

from typing import Any, Dict, Optional

import pydantic


class RawWebhookEvent(pydantic.BaseModel):
    event_name: str
    payload: Dict[str, Any]
    delivery_id: Optional[str] = None
