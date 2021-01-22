import asyncio
import time
from typing import Optional, Iterable

import click
import loguru

logger = loguru.logger
logger.disable(__name__)
TIMEOUT = 15


@click.group()
def cli():
    pass


@cli.command()
@click.argument("ip_list", type=click.Path("r"))
@click.argument("ascii_art_path", type=click.File("r"))
@click.argument("workers", type=int)
def run(ip_list, ascii_art_path, workers):
    request_template = conv_art_to_requests_template(ascii_art_path.read())

    hosts_count = count_lines(ip_list)
    with open(ip_list) as file:
        with click.progressbar(file, length=hosts_count, show_pos=True) as bar:
            asyncio.run(run_spammer(bar, request_template, workers))


@cli.command()
@click.argument("ascii_art", type=click.File("r"))
@click.argument("addr")
@click.argument("port", type=int)
def send_test(ascii_art, addr, port):
    request_template = conv_art_to_requests_template(ascii_art.read())
    asyncio.run(send_request(addr, port, request_template))


async def run_spammer(
    addr_iter: Iterable, request_template: bytes, workers_count: int
):
    queue = asyncio.Queue(workers_count)

    for i in range(1, workers_count + 1):
        logger.info(f"Starting worker {i}")
        asyncio.create_task(run_spam_worker(queue, request_template))

    logger.info("Loading tasks to queue..")
    for addr in addr_iter:
        addr = addr.strip()
        if addr:
            try:
                addr, port = addr.split(":")
            except ValueError:
                continue
            await queue.put((addr, port))

    logger.info("Loading stop-stubs to queue")
    for _ in range(workers_count):
        await queue.put(None)

    logger.info("Waiting last requests")
    await queue.join()


async def run_spam_worker(queue: asyncio.Queue, request_template: bytes):
    while True:
        task = await queue.get()
        if task is None:
            queue.task_done()
            return
        addr, port = task

        logger.debug(f"Sending request to {addr}:{port}")
        await send_request(addr, port, request_template)
        queue.task_done()


@logger.catch(reraise=False)
async def send_request(addr: str, port: int, request_template: bytes):
    request_data = request_template.replace(b"%IP%", addr.encode())
    requests_count = request_data.count(b"HTTP/1.1\r\n")

    writer: Optional[asyncio.StreamWriter] = None
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(addr, port),
            timeout=TIMEOUT,
        )
        writer.write(request_data)
        await asyncio.wait_for(writer.drain(), timeout=TIMEOUT)

        await process_resp(reader, requests_count)
    except (OSError, asyncio.exceptions.TimeoutError):
        logger.debug(f"Failed to connect: {addr}:{port}")
    finally:
        if writer and not writer.is_closing():
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=TIMEOUT)
            except asyncio.exceptions.TimeoutError:
                raise


@logger.catch(reraise=True)
async def process_resp(reader, requests_count):
    packed_resp_count = 0
    spent_time_reading = 0

    while True:
        readline_time_start = time.time()
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=TIMEOUT)
        except asyncio.exceptions.TimeoutError:
            break
        if not line:
            break

        spent_time_reading += time.time() - readline_time_start
        if spent_time_reading >= TIMEOUT:
            # Too long. We should stop and disconnect.
            break
        if line.startswith(b"HTTP/1"):
            packed_resp_count += 1
            if packed_resp_count == requests_count:
                # All requests has processed by web server. We can disconnect
                # without waiting
                break


def conv_art_to_requests_template(art) -> bytes:
    message = ""
    lines = art.split("\n")
    max_len = max([len(line.rstrip()) for line in lines])
    for line in lines:
        line = line.replace("\r", "").replace("\n", "")
        line = line.ljust(max_len)
        line = f"...{line}..."
        line = line.replace(" ", ".")
        message += f"HEAD /?q{line} HTTP/1.1\r\n" f"Host: %IP%\r\n\r\n"

    return message.encode()


def count_lines(filename):
    counter = 0
    with open(filename) as file:
        for line in file:
            line = line.strip()
            if line:
                counter += 1

    return counter


if __name__ == "__main__":
    cli()
