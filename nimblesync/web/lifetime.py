from typing import Awaitable, Callable

import psycopg_pool
from fastapi import FastAPI
# from fastapi_utils.session import FastAPISessionMaker
from fastapi_utils.tasks import repeat_every


from nimblesync.db.dao.contact_dao import ContactDAO
from nimblesync.db.seed import seed_contacts
from nimblesync.services.contact_sync import ContactSync

from nimblesync.settings import settings


async def _setup_db(app: FastAPI) -> None:
    """
    Creates connection pool for timescaledb.

    :param app: current FastAPI app.
    """
    app.state.db_pool = psycopg_pool.AsyncConnectionPool(conninfo=str(settings.db_url))
    await app.state.db_pool.wait()

    contact_dao = ContactDAO(app.state.db_pool)
    contact_table_exists = await contact_dao.contact_table_exists()
    if not contact_table_exists:
        print("Initializing database...")
        await contact_dao.create_contact_table()
        print("Seed...")
        seed_contacts()
        print("Done.")


def register_startup_event(
    app: FastAPI,
) -> Callable[[], Awaitable[None]]:  # pragma: no cover
    """
    Actions to run on application startup.

    This function uses fastAPI app to store data
    in the state, such as db_engine.

    :param app: the fastAPI application.
    :return: function that actually performs actions.
    """

    @app.on_event("startup")
    async def _startup() -> None:  # noqa: WPS430
        app.middleware_stack = None
        await _setup_db(app)
        app.middleware_stack = app.build_middleware_stack()
        pass  # noqa: WPS420

    return _startup


def register_shutdown_event(
    app: FastAPI,
) -> Callable[[], Awaitable[None]]:  # pragma: no cover
    """
    Actions to run on application's shutdown.

    :param app: fastAPI application.
    :return: function that actually performs actions.
    """

    @app.on_event("shutdown")
    async def _shutdown() -> None:  # noqa: WPS430
        await app.state.db_pool.close()
        pass  # noqa: WPS420

    return _shutdown


def register_routine_event(
    app: FastAPI,
) -> Callable[[], Awaitable[None]]:  # pragma: no cover
    """
    Actions to run periodically.

    :param app: the fastAPI application.
    :return: function that actually performs actions.
    """

    @app.on_event("startup")
    @repeat_every(seconds=60 * 60 * 24)  # 1 day
    def _data_sync() -> None:  # noqa: WPS430
        print("Start importing data from Nimble API...")
        ContactSync.sync()
        print("Import done.")
        pass  # noqa: WPS420

    return _data_sync

