import dataclasses
import functools
import logging
import socketserver
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

log = logging.getLogger(__name__)


@dataclasses.dataclass(kw_only=True)
class Health:
    last_heartbeat: datetime | None = dataclasses.field(default=None)
    heartbeat_max_age: timedelta = dataclasses.field(default=timedelta(minutes=3))

    @property
    def healthy(self) -> bool:
        if self.last_heartbeat is None:
            return False

        return (
            datetime.now(timezone.utc) - self.last_heartbeat
        ) < self.heartbeat_max_age

    def heartbeat(self) -> None:
        self.last_heartbeat = datetime.now(timezone.utc)


class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP request handler for health checks using stdlib."""

    def __init__(
        self,
        health: Health,
        request: Any,
        client_address: Any,
        server: socketserver.BaseServer,
    ) -> None:
        self.health = health
        super().__init__(request, client_address, server)

    def log_message(self, format: str, *args: Any) -> None:
        log.debug(f"Health check: {format % args}")

    def do_GET(self) -> None:
        if self.path != "/health":
            self.send_error(404)
            return
        self.send_response(200 if self.health.healthy else 503)
        self.send_header("Content-Length", "0")
        self.end_headers()

    do_HEAD = do_GET


def create_health_server(health: Health, host: str, port: int) -> HTTPServer:
    """Create an HTTP server for health checks."""

    return HTTPServer((host, port), functools.partial(HealthCheckHandler, health))
