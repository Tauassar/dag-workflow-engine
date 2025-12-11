"""Entrypoints for the consumers."""
import asyncio
import logging

import click

from dag_service.ioc import container
from .config import settings
from .server import start


async def start_worker():
    await container.init_worker()
    await container.worker.run()


@click.group()
def main() -> None:
    logging.basicConfig(
        level=settings.LOG_LEVEL,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )


@main.command(name="worker")
def run_worker() -> None:
    asyncio.run(start_worker())


@main.command(name="web")
def run_web() -> None:
    asyncio.run(start())


if __name__ == "__main__":
    main()
