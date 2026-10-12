"""Minimal HTTP/1.1 round-robin load balancer for running the cluster without Docker.

It reproduces the nginx behaviour configured in deploy/nginx/nginx.conf:
  * round-robin over the upstream instances
  * passive health checks: an instance that refuses/drops a connection is
    marked down for FAIL_TIMEOUT seconds (nginx: max_fails=1 fail_timeout=3s)
  * the failed request is retried on the next instance
    (nginx: proxy_next_upstream error timeout http_502 http_503 non_idempotent)

usage: python3 deploy/local/lb.py 8080 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003
"""
from __future__ import annotations

import asyncio
import itertools
import json
import sys
import os
import time
import zlib

FAIL_TIMEOUT = 3.0
POLICY = os.environ.get("LB_POLICY", "round-robin")  # or "user-hash"
UPSTREAM_TIMEOUT = 10.0


class Upstreams:
    def __init__(self, addrs: list[str]):
        self.addrs = [(a.split(":")[0], int(a.split(":")[1])) for a in addrs]
        self.down_until = {a: 0.0 for a in self.addrs}
        self.rr = itertools.cycle(range(len(self.addrs)))

    def order(self, affinity_key: str | None = None) -> list[tuple[str, int]]:
        if POLICY == "user-hash" and affinity_key:
            # same user -> same instance (better L1 cache hit ratio); on failure the
            # ring continues to the next instance, like nginx `hash ... consistent`
            start = zlib.crc32(affinity_key.encode()) % len(self.addrs)
        else:
            start = next(self.rr)
        ring = self.addrs[start:] + self.addrs[:start]
        now = time.time()
        up = [a for a in ring if self.down_until[a] <= now]
        return up or ring  # if all are down, still try (like nginx)

    def mark_down(self, a) -> None:
        self.down_until[a] = time.time() + FAIL_TIMEOUT
        log("upstream_marked_down", upstream=f"{a[0]}:{a[1]}", seconds=FAIL_TIMEOUT)


def log(event: str, **kw) -> None:
    print(json.dumps({"ts": time.strftime("%H:%M:%S"), "component": "lb", "event": event, **kw}),
          flush=True)


async def read_request(reader: asyncio.StreamReader) -> tuple[bytes, list[tuple[str, str]], bytes] | None:
    try:
        head = await reader.readuntil(b"\r\n\r\n")
    except (asyncio.IncompleteReadError, ConnectionError):
        return None
    lines = head.decode("latin-1").split("\r\n")
    start = lines[0].encode()
    headers = []
    for line in lines[1:]:
        if line:
            k, _, v = line.partition(":")
            headers.append((k.strip(), v.strip()))
    n = int(next((v for k, v in headers if k.lower() == "content-length"), "0"))
    body = await reader.readexactly(n) if n else b""
    return start, headers, body


async def forward(addr, start: bytes, headers, body: bytes) -> bytes:
    r, w = await asyncio.wait_for(asyncio.open_connection(*addr), 2)
    try:
        hdrs = [(k, v) for k, v in headers if k.lower() not in ("connection", "keep-alive")]
        hdrs.append(("Connection", "close"))
        w.write(start + b"\r\n" + "".join(f"{k}: {v}\r\n" for k, v in hdrs).encode("latin-1")
                + b"\r\n" + body)
        await w.drain()
        data = await asyncio.wait_for(r.read(), UPSTREAM_TIMEOUT)
        if not data:
            raise ConnectionError("upstream closed without response")
        return data
    finally:
        w.close()


def rewrite_response(raw: bytes, tries: int) -> bytes:
    head, _, body = raw.partition(b"\r\n\r\n")
    lines = head.split(b"\r\n")
    keep = [l for l in lines[1:] if not l.lower().startswith((b"connection:", b"content-length:",
                                                               b"transfer-encoding:"))]
    keep += [b"Content-Length: %d" % len(body), b"Connection: keep-alive",
             b"X-Upstream-Tries: %d" % tries]
    return b"\r\n".join([lines[0]] + keep) + b"\r\n\r\n" + body


def status_of(raw: bytes) -> int:
    try:
        return int(raw.split(b" ", 2)[1])
    except Exception:
        return 0


async def handle(ups: Upstreams, reader, writer) -> None:
    try:
        while True:
            req = await read_request(reader)
            if req is None:
                break
            start, headers, body = req
            resp, tries = None, 0
            auth = next((v for k, v in headers if k.lower() == "authorization"), None)
            for addr in ups.order(auth):
                tries += 1
                try:
                    raw = await forward(addr, start, headers, body)
                    if status_of(raw) in (502, 503) and tries < len(ups.addrs):
                        ups.mark_down(addr)
                        continue
                    resp = raw
                    break
                except (OSError, ConnectionError, asyncio.TimeoutError, asyncio.IncompleteReadError) as e:
                    ups.mark_down(addr)
                    log("upstream_failed_retrying", upstream=f"{addr[0]}:{addr[1]}",
                        request=start.decode(errors="replace"), error=type(e).__name__)
            if resp is None:
                msg = b'{"error":"NO_UPSTREAM","message":"all instances unavailable"}'
                resp = (b"HTTP/1.1 502 Bad Gateway\r\nContent-Type: application/json\r\n"
                        b"Content-Length: %d\r\n\r\n%s" % (len(msg), msg))
                writer.write(resp)
            else:
                writer.write(rewrite_response(resp, tries))
            await writer.drain()
    except (ConnectionError, asyncio.IncompleteReadError):
        pass
    finally:
        writer.close()


async def main() -> None:
    port = int(sys.argv[1])
    ups = Upstreams(sys.argv[2:])
    server = await asyncio.start_server(lambda r, w: handle(ups, r, w), "0.0.0.0", port, backlog=1024)
    log("lb_started", port=port, upstreams=sys.argv[2:], policy=POLICY)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
