"""Tiny asyncio HTTP/1.1 keep-alive client used by the load generator and experiments.

One `Client` = one TCP connection (one virtual user), like a browser tab.
Kept dependency-free so the measurements include as little client overhead
as possible.
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass


@dataclass
class Resp:
    status: int
    headers: dict
    body: bytes
    ms: float

    def json(self):
        return json.loads(self.body) if self.body else None


class Client:
    def __init__(self, host: str = "127.0.0.1", port: int = 8080, timeout: float = 15.0):
        self.host, self.port, self.timeout = host, port, timeout
        self.r = self.w = None
        self.token: str | None = None

    async def _connect(self):
        self.r, self.w = await asyncio.open_connection(self.host, self.port)

    async def close(self):
        if self.w:
            self.w.close()
            self.r = self.w = None

    async def request(self, method: str, path: str, body=None, headers: dict | None = None) -> Resp:
        data = json.dumps(body).encode() if body is not None else b""
        h = {"Host": self.host, "Content-Length": str(len(data))}
        if body is not None:
            h["Content-Type"] = "application/json"
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        h.update(headers or {})
        raw = (f"{method} {path} HTTP/1.1\r\n" + "".join(f"{k}: {v}\r\n" for k, v in h.items())
               + "\r\n").encode() + data
        t0 = time.perf_counter()
        for attempt in (1, 2):  # one silent reconnect if the keep-alive socket was closed
            try:
                if self.w is None:
                    await self._connect()
                self.w.write(raw)
                await self.w.drain()
                return await asyncio.wait_for(self._read(t0), self.timeout)
            except (ConnectionError, asyncio.IncompleteReadError, OSError):
                await self.close()
                if attempt == 2:
                    raise
        raise RuntimeError("unreachable")

    async def _read(self, t0: float) -> Resp:
        head = await self.r.readuntil(b"\r\n\r\n")
        lines = head.decode("latin-1").split("\r\n")
        status = int(lines[0].split(" ", 2)[1])
        headers = {}
        for line in lines[1:]:
            if line:
                k, _, v = line.partition(":")
                headers[k.strip().lower()] = v.strip()
        n = int(headers.get("content-length", "0"))
        body = await self.r.readexactly(n) if n else b""
        if headers.get("connection", "").lower() == "close":
            await self.close()
        return Resp(status, headers, body, (time.perf_counter() - t0) * 1000)


async def new_user(prefix: str, i: int, host="127.0.0.1", port=8080) -> Client:
    c = Client(host, port)
    name = f"{prefix}{i}"
    r = await c.request("POST", "/api/v1/auth/register", {"username": name, "password": "password-123"})
    if r.status == 409:
        r = await c.request("POST", "/api/v1/auth/login", {"username": name, "password": "password-123"})
    c.token = r.json()["token"]
    c.user_id = r.json()["id"]
    return c


async def login(username: str, password: str, host="127.0.0.1", port=8080) -> Client:
    c = Client(host, port)
    r = await c.request("POST", "/api/v1/auth/login", {"username": username, "password": password})
    c.token = r.json()["token"]
    c.user_id = r.json()["id"]
    return c


def pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = min(len(s) - 1, max(0, int(round(p / 100 * (len(s) - 1)))))
    return s[k]
