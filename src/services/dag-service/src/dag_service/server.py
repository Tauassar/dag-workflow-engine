import asyncio

import uvicorn

from dag_service.app import app
from dag_service.config import settings


def main():
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start())


async def start():
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host=settings.API_HOST,
            port=settings.API_PORT,
            workers=1,
            log_level="info",
        )
    )
    await server.serve()
