"""TCP proxy that adds a fixed one-way delay, to emulate a managed cloud Redis
that lives a network hop away (e.g. another availability zone).

usage: python3 experiments/latency_proxy.py <listen_port> <target_port> <one_way_ms>
"""
import asyncio
import sys
import time

LISTEN, TARGET, DELAY = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3]) / 1000


async def pipe(reader, writer):
    q: asyncio.Queue = asyncio.Queue()

    async def pump_in():
        while True:
            data = await reader.read(65536)
            await q.put((time.monotonic() + DELAY, data))
            if not data:
                return

    async def pump_out():
        while True:
            due, data = await q.get()
            wait = due - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            if not data:
                writer.close()
                return
            writer.write(data)
            await writer.drain()

    try:
        await asyncio.gather(pump_in(), pump_out())
    except Exception:
        writer.close()


async def handle(cr, cw):
    try:
        ur, uw = await asyncio.open_connection("127.0.0.1", TARGET)
    except OSError:
        cw.close()
        return
    await asyncio.gather(pipe(cr, uw), pipe(ur, cw))


async def main():
    srv = await asyncio.start_server(handle, "127.0.0.1", LISTEN)
    async with srv:
        await srv.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
