from logging import DEBUG
import asyncio
import sys

OUTPUT = None
MIN_LEVEL = DEBUG

async def init(min_level = DEBUG, stream=sys.stdout):
    global MIN_LEVEL
    global OUTPUT
    MIN_LEVEL = min_level

    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    w_transport, w_protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, stream)
    OUTPUT = asyncio.StreamWriter(w_transport, w_protocol, reader, loop)

async def log(level, message):
    if level >= MIN_LEVEL:
        OUTPUT.write(f"{asyncio.get_event_loop().time()} {message}\n".encode())
        await OUTPUT.drain()

def log_no_wait(level, message):
    if level >= MIN_LEVEL:
        asyncio.create_task(log(level, message))

