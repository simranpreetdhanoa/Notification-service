"""Domain types shared across the service."""
from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field


class Priority(enum.IntEnum):
    """Lower value = dispatched first (heapq is a min-heap)."""

    CRITICAL = 1       # security / ops: password resets, 2FA codes, outage pages
    TRANSACTIONAL = 2  # order confirmations, shipping updates
    MARKETING = 3      # batch campaigns

    @classmethod
    def parse(cls, value: object) -> "Priority":
        if isinstance(value, str) and not value.isdigit():
            return cls[value.strip().upper()]
        return cls(int(value))  # type: ignore[arg-type]


class Channel(str, enum.Enum):
    SMS = "sms"
    EMAIL = "email"
    PUSH = "push"


class Status(str, enum.Enum):
    PENDING = "PENDING"        # waiting in the queue (new or awaiting retry)
    SENDING = "SENDING"        # claimed by exactly one worker; lease-protected
    DISPATCHED = "DISPATCHED"  # terminal: provider accepted it
    FAILED = "FAILED"          # terminal: permanent error or retries exhausted


TERMINAL = frozenset({Status.DISPATCHED, Status.FAILED})


@dataclass
class Notification:
    channel: Channel
    recipient: str
    body: str
    priority: Priority = Priority.TRANSACTIONAL
    subject: str | None = None
    idempotency_key: str | None = None
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    status: Status = Status.PENDING
    attempts: int = 0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    next_attempt_at: float = 0.0
    dispatched_at: float | None = None
    provider_message_id: str | None = None
    last_error: str | None = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "idempotency_key": self.idempotency_key,
            "channel": self.channel.value,
            "recipient": self.recipient,
            "subject": self.subject,
            "body": self.body,
            "priority": int(self.priority),
            "priority_name": self.priority.name,
            "status": self.status.value,
            "attempts": self.attempts,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "next_attempt_at": self.next_attempt_at,
            "dispatched_at": self.dispatched_at,
            "provider_message_id": self.provider_message_id,
            "last_error": self.last_error,
        }
