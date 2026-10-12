"""Small asyncio Redis client (RESP2) with a connection pool and pub/sub.

It is intentionally dependency-free so the service image stays tiny and every
byte that goes to Redis is visible and explainable. It supports exactly what
the service needs: plain commands, pipelined commands, EVALSHA with automatic
script loading, and a SUBSCRIBE loop for cache-invalidation messages.
"""
from __future__ import annotations

import asyncio
import hashlib
from typing import Any, Awaitable, Callable


class RedisError(Exception):
    pass


def _encode(*args: Any) -> bytes:
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        if isinstance(a, bytes):
            b = a
        elif isinstance(a, str):
            b = a.encode()
        else:
            b = str(a).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(b), b))
    return b"".join(out)


async def _read_reply(reader: asyncio.StreamReader) -> Any:
    line = await reader.readline()
    if not line:
        raise ConnectionError("redis connection closed")
    kind, rest = line[:1], line[1:-2]
    if kind == b"+":
        return rest.decode()
    if kind == b"-":
        return RedisError(rest.decode())
    if kind == b":":
        return int(rest)
    if kind == b"$":
        n = int(rest)
        if n == -1:
            return None
        data = await reader.readexactly(n + 2)
        return data[:-2].decode()
    if kind == b"*":
        n = int(rest)
        if n == -1:
            return None
        return [await _read_reply(reader) for _ in range(n)]
    raise RedisError(f"unknown reply type {line!r}")


class _Conn:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer

    async def call(self, *args: Any) -> Any:
        self.writer.write(_encode(*args))
        await self.writer.drain()
        return await _read_reply(self.reader)

    async def pipeline(self, cmds: list[tuple]) -> list[Any]:
        self.writer.write(b"".join(_encode(*c) for c in cmds))
        await self.writer.drain()
        return [await _read_reply(self.reader) for _ in cmds]

    def close(self) -> None:
        try:
            self.writer.close()
        except Exception:
            pass


class Redis:
    def __init__(self, host: str = "127.0.0.1", port: int = 6379,
                 password: str | None = None, pool_size: int = 32):
        self.host, self.port, self.password = host, port, password
        self._pool: asyncio.Queue[_Conn] = asyncio.Queue()
        self._created = 0
        self._max = pool_size
        self._scripts: dict[str, str] = {}

    async def _connect(self) -> _Conn:
        r, w = await asyncio.wait_for(asyncio.open_connection(self.host, self.port), 3)
        c = _Conn(r, w)
        if self.password:
            res = await c.call("AUTH", self.password)
            if isinstance(res, RedisError):
                raise res
        return c

    async def _acquire(self) -> _Conn:
        if self._pool.empty() and self._created < self._max:
            self._created += 1
            try:
                return await self._connect()
            except Exception:
                self._created -= 1
                raise
        return await self._pool.get()

    def _release(self, c: _Conn, broken: bool) -> None:
        if broken:
            c.close()
            self._created -= 1
        else:
            self._pool.put_nowait(c)

    async def execute(self, *args: Any) -> Any:
        c = await self._acquire()
        broken = False
        try:
            res = await c.call(*args)
        except Exception:
            broken = True
            raise
        finally:
            self._release(c, broken)
        if isinstance(res, RedisError):
            raise res
        return res

    async def pipeline(self, cmds: list[tuple]) -> list[Any]:
        c = await self._acquire()
        broken = False
        try:
            return await c.pipeline(cmds)
        except Exception:
            broken = True
            raise
        finally:
            self._release(c, broken)

    # ---- Lua scripts -------------------------------------------------
    def register_script(self, name: str, source: str) -> None:
        self._scripts[name] = source

    async def run_script(self, name: str, keys: list[str], args: list[Any]) -> Any:
        src = self._scripts[name]
        sha = hashlib.sha1(src.encode()).hexdigest()
        try:
            return await self.execute("EVALSHA", sha, len(keys), *keys, *args)
        except RedisError as e:
            if not str(e).startswith("NOSCRIPT"):
                raise
            return await self.execute("EVAL", src, len(keys), *keys, *args)

    async def ping(self) -> bool:
        return (await self.execute("PING")) == "PONG"

    # ---- pub/sub -----------------------------------------------------
    async def subscribe_forever(self, channel: str,
                                handler: Callable[[str], Awaitable[None] | None],
                                on_reconnect: Callable[[], None] | None = None) -> None:
        """Dedicated connection; reconnects with backoff if Redis restarts."""
        delay = 0.2
        while True:
            try:
                c = await self._connect()
                await c.call("SUBSCRIBE", channel)
                if on_reconnect:
                    on_reconnect()  # we may have missed messages -> caller flushes cache
                delay = 0.2
                while True:
                    msg = await _read_reply(c.reader)
                    if isinstance(msg, list) and msg and msg[0] == "message":
                        r = handler(msg[2])
                        if asyncio.iscoroutine(r):
                            await r
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 5)

    async def close(self) -> None:
        while not self._pool.empty():
            self._pool.get_nowait().close()
