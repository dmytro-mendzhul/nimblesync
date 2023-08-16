from psycopg.rows import class_row
from psycopg_pool import AsyncConnectionPool
from nimblesync.db.dao.base_dao import BaseDao

from nimblesync.db.models.contact_model import ContactModel


class ContactDAO(BaseDao):
    """Class for accessing contact table."""

    def __init__(
        self,
        db_pool: AsyncConnectionPool,
    ) -> None:
        super().__init__(db_pool)

    
    async def contact_table_exists(self) -> bool:
        async with self.connection_pool.connection() as connection:
            async with connection.cursor(binary=True) as cur:
                res = await cur.execute(
                    """
                    SELECT EXISTS (
                    SELECT FROM 
                        pg_tables
                    WHERE 
                        schemaname = 'public' AND 
                        tablename  = 'contact'
                    );""",
                )
                return (await res.fetchone())[0]


    async def create_contact_table(self) -> None:
        async with self.connection_pool.connection() as connection:
            async with connection.cursor(binary=True) as cur:
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS contact (
                        id SERIAL PRIMARY KEY,
                        external_id VARCHAR ( 50 ) UNIQUE,
                        first_name VARCHAR ( 255 ),
                        last_name VARCHAR ( 255 ),
                        email VARCHAR ( 255 ),
                        removed BOOLEAN DEFAULT FALSE NOT NULL,
                        textsearchable_index_col tsvector
                            GENERATED ALWAYS AS (to_tsvector('english',
                                coalesce(first_name, '') || ' ' || 
                                coalesce(last_name, '') || ' ' || 
                                coalesce(email, '')
                            )) STORED
                    );
                    """,
                )


    async def create_contact_model(self, first_name: str, last_name: str, email: str) -> None:
        """Create new contact in a database.

        Args:
            first_name (str): First name (max 255 characters)
            last_name (str): Last name (max 255 characters)
            email (str): Email (max 255 characters)
        """
        async with self.connection_pool.connection() as connection:
            async with connection.cursor(binary=True) as cur:
                await cur.execute(
                    "INSERT INTO contact (first_name, last_name, email) VALUES (%(first_name)s, %(last_name)s, %(email)s);",
                    params={
                        "first_name": first_name,
                        "last_name": last_name,
                        "email": email,
                    },
                )

    async def get_all_contacts(self, limit: int, offset: int, includeRemoved: bool = False) -> list[ContactModel]:
        """Get all contact models with limit/offset pagination.

        Args:
            limit (int): Take
            offset (int): Skip
            includeRemoved (bool): If True, removed contacts will be included

        Returns:
            List[ContactModel]: Contact models
        """
        async with self.connection_pool.connection() as connection:
            async with connection.cursor(
                binary=True,
                row_factory=class_row(ContactModel),
            ) as cur:
                sql = "SELECT id, external_id, first_name, last_name, email FROM contact " +\
                    ("" if includeRemoved else "WHERE removed = FALSE ") +\
                    "LIMIT %(limit)s OFFSET %(offset)s;"
                res = await cur.execute(
                    sql,
                    params={
                        "limit": limit,
                        "offset": offset,
                    },
                )
                return await res.fetchall()
            

    async def search_contacts(self, search: str, limit: int, offset: int, includeRemoved: bool = False) -> list[ContactModel]:
        """Get all contact models with limit/offset pagination.

        Args:
            search (str): Search string (use & for AND and | for OR)
            limit (int): Take
            offset (int): Skip
            includeRemoved (bool): If True, removed contacts will be included

        Returns:
            List[ContactModel]: Contact models
        """
        async with self.connection_pool.connection() as connection:
            async with connection.cursor(
                binary=True,
                row_factory=class_row(ContactModel),
            ) as cur:
                sql = "SELECT id, external_id, first_name, last_name, email FROM contact WHERE " +\
                    ("" if includeRemoved else "removed = FALSE AND ") +\
                    "textsearchable_index_col @@ to_tsquery(%(search)s) " +\
                    "LIMIT %(limit)s OFFSET %(offset)s;"
                res = await cur.execute(
                    sql,
                    params={
                        "search": search,
                        "limit": limit,
                        "offset": offset,
                    },
                )
                return await res.fetchall()
