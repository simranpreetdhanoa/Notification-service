"""HTTP intake API (stdlib only).

    POST /v1/notifications         submit one   (honours Idempotency-Key header)
    POST /v1/notifications/batch   submit many  {"notifications": [...]}
    GET  /v1/notifications/<id>    status + audit history
    GET  /v1/stats                 counts by status/priority + dispatcher metrics
    GET  /healthz
"""
from __future__ import annotations

import json
import logging
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .dispatcher import Dispatcher
from .models import Channel, Notification, Priority

log = logging.getLogger(__name__)

MAX_BODY_BYTES = 1 << 20
MAX_BATCH = 500
MAX_MESSAGE_CHARS = {Channel.SMS: 1600, Channel.PUSH: 4000, Channel.EMAIL: 100_000}


class ValidationError(ValueError):
    pass


def parse_notification(data: object, idempotency_key: str | None = None) -> Notification:
    if not isinstance(data, dict):
        raise ValidationError("notification must be a JSON object")
    try:
        channel = Channel(str(data.get("channel", "")).lower())
    except ValueError:
        raise ValidationError(f"channel must be one of {[c.value for c in Channel]}") from None
    try:
        priority = Priority.parse(data.get("priority", Priority.TRANSACTIONAL))
    except (KeyError, ValueError, TypeError):
        raise ValidationError("priority must be 1-3 or critical/transactional/marketing") from None
    recipient = data.get("recipient")
    body = data.get("body")
    if not isinstance(recipient, str) or not recipient.strip():
        raise ValidationError("recipient is required")
    if not isinstance(body, str) or not body:
        raise ValidationError("body is required")
    if len(body) > MAX_MESSAGE_CHARS[channel]:
        raise ValidationError(f"body exceeds {MAX_MESSAGE_CHARS[channel]} chars for {channel.value}")
    subject = data.get("subject")
    if channel is Channel.EMAIL and not subject:
        raise ValidationError("subject is required for email")
    key = idempotency_key or data.get("idempotency_key")
    return Notification(channel=channel, recipient=recipient.strip(), body=body,
                        priority=priority, subject=subject,
                        idempotency_key=str(key) if key else None)


def make_handler(dispatcher: Dispatcher) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "notifier/1.0"

        def log_message(self, fmt, *args):  # route access logs through logging
            log.debug("%s - %s", self.address_string(), fmt % args)

        def do_GET(self):
            path = self.path.split("?", 1)[0].rstrip("/")
            if path == "/healthz":
                return self._send(HTTPStatus.OK, {"ok": True})
            if path == "/v1/stats":
                return self._send(HTTPStatus.OK, {
                    **dispatcher.store.stats(),
                    "queue_depth": len(dispatcher.queue),
                    "metrics": dispatcher.snapshot_metrics(),
                })
            if path.startswith("/v1/notifications/"):
                nid = path.rsplit("/", 1)[1]
                n = dispatcher.store.get(nid)
                if n is None:
                    return self._error(HTTPStatus.NOT_FOUND, "notification not found")
                return self._send(HTTPStatus.OK,
                                  {**n.to_dict(), "history": dispatcher.store.history(nid)})
            self._error(HTTPStatus.NOT_FOUND, "no such endpoint")

        def do_POST(self):
            path = self.path.split("?", 1)[0].rstrip("/")
            try:
                payload = self._read_json()
                if path == "/v1/notifications":
                    n = parse_notification(payload, self.headers.get("Idempotency-Key"))
                    stored, created = dispatcher.submit(n)
                    return self._send(HTTPStatus.ACCEPTED if created else HTTPStatus.OK,
                                      _receipt(stored, created))
                if path == "/v1/notifications/batch":
                    items = payload.get("notifications") if isinstance(payload, dict) else None
                    if not isinstance(items, list) or not items:
                        raise ValidationError("'notifications' must be a non-empty list")
                    if len(items) > MAX_BATCH:
                        raise ValidationError(f"batch limited to {MAX_BATCH} notifications")
                    # Validate everything before accepting anything.
                    parsed = [parse_notification(item) for item in items]
                    receipts = [_receipt(*dispatcher.submit(n)) for n in parsed]
                    return self._send(HTTPStatus.ACCEPTED, {"notifications": receipts})
            except ValidationError as e:
                return self._error(HTTPStatus.BAD_REQUEST, str(e))
            self._error(HTTPStatus.NOT_FOUND, "no such endpoint")

        def _read_json(self):
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0:
                raise ValidationError("request body required")
            if length > MAX_BODY_BYTES:
                raise ValidationError("request body too large")
            try:
                return json.loads(self.rfile.read(length))
            except json.JSONDecodeError as e:
                raise ValidationError(f"invalid JSON: {e.msg}") from None

        def _send(self, status: HTTPStatus, body: dict) -> None:
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _error(self, status: HTTPStatus, message: str) -> None:
            self._send(status, {"error": message})

    return Handler


def _receipt(n: Notification, created: bool) -> dict:
    return {"id": n.id, "status": n.status.value, "priority": int(n.priority),
            "duplicate": not created}


def make_server(dispatcher: Dispatcher, host: str = "127.0.0.1", port: int = 8080) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), make_handler(dispatcher))
    server.daemon_threads = True
    return server
