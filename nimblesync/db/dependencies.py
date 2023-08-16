from typing import Callable
from fastapi import Depends
from psycopg_pool import AsyncConnectionPool
from starlette.requests import Request

from nimblesync.db.dao.contact_dao import ContactDAO


async def get_db_pool(request: Request) -> AsyncConnectionPool:
    """
    Return database connections pool.

    :param request: current request.
    :returns: database connections pool.
    """
    return request.app.state.db_pool


def get_contact_dao(conn_pool: AsyncConnectionPool = Depends(get_db_pool)) -> Callable[[AsyncConnectionPool], ContactDAO]:
    return ContactDAO(conn_pool)

