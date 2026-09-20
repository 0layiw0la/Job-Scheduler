"""A webhook receiver double: records deliveries, and can fail, hang or answer slowly."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass(slots=True)
class Delivery:
    method: str
    path: str
    headers: dict[str, str]
    body: str
    received_at: datetime

    @property
    def idempotency_key(self) -> str | None:
        return self.headers.get("idempotency-key")


@dataclass
class Receiver:
    """Serves HTTP/1.1 well enough for a webhook, with scripted responses."""

    statuses: deque[int] = field(default_factory=deque)
    default_status: int = 200
    hang: bool = False
    delay_seconds: float = 0.0
    response_headers: dict[str, str] = field(default_factory=dict)
    deliveries: list[Delivery] = field(default_factory=list)
    _server: asyncio.Server | None = None
    _arrival: asyncio.Event = field(default_factory=asyncio.Event)

    def __post_init__(self) -> None:
        self.statuses = deque(self.statuses)

    @property
    def url(self) -> str:
        assert self._server is not None
        port = self._server.sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{port}/hook"

    @property
    def count(self) -> int:
        return len(self.deliveries)

    def keys(self) -> list[str | None]:
        return [delivery.idempotency_key for delivery in self.deliveries]

    async def wait_for_count(self, count: int) -> None:
        """Block until `count` deliveries have arrived. Callers impose their own timeout."""
        while self.count < count:
            self._arrival.clear()
            await self._arrival.wait()

    async def start(self) -> Receiver:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        return self

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        self._server.abort_clients()
        await self._server.wait_closed()

    async def __aenter__(self) -> Receiver:
        return await self.start()

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request_line = await reader.readline()
            if not request_line:
                return
            method, path, _ = request_line.decode().split(" ", 2)

            headers: dict[str, str] = {}
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break
                name, _, value = line.decode().partition(":")
                headers[name.strip().lower()] = value.strip()

            length = int(headers.get("content-length", 0))
            body = (await reader.readexactly(length)).decode() if length else ""
            self.deliveries.append(Delivery(method, path, headers, body, datetime.now(UTC)))
            self._arrival.set()

            if self.hang:
                await asyncio.sleep(3600)
            if self.delay_seconds:
                await asyncio.sleep(self.delay_seconds)

            status = self.statuses.popleft() if self.statuses else self.default_status
            extra = "".join(f"{k}: {v}\r\n" for k, v in self.response_headers.items())
            payload = f"status {status}".encode()
            writer.write(
                f"HTTP/1.1 {status} X\r\nContent-Length: {len(payload)}\r\n{extra}\r\n".encode()
                + payload
            )
            await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError, asyncio.CancelledError):
            return
        finally:
            writer.close()
