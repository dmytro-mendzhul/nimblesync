from psycopg_pool import AsyncConnectionPool


class BaseDao:
    def __init__(
        self,
        db_pool: AsyncConnectionPool
    ) -> None:
        self.db_pool = db_pool
    
    @property
    def connection_pool(self) -> AsyncConnectionPool:
        return self.db_pool