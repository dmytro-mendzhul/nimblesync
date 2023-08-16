from typing import Iterator
import requests
import psycopg
from nimblesync.settings import settings


NIMBLE_CONTACTS_IDS_ENDPOINT = settings.nimble_contacts_ids_endpoint
NIMBLE_CONTACTS_ENDPOINT = settings.nimble_contacts_endpoint
NIMBLE_TOKEN = settings.nimble_token
NIMBLE_HEADERS = { 'Authorization': 'Bearer ' + NIMBLE_TOKEN }
SUCCESS_STATUS_CODE = 200
BATCH_SIZE = 10 # can be increased, but for test purpose let it be small
DEFAULT_BATCH_SIZE = 30


class ContactSync:

    @staticmethod
    def sync():
        chunks = ContactSync._load_paginated_contacts()
        ContactSync._update_contacts_with_external_data(chunks)
        print('Updated successfully!')
    

    @staticmethod
    def _update_contacts_with_external_data(chunks: Iterator[list[tuple[str, ...]]]) -> None:
        """Loads contacts page-by-page to temp table, then upserts into 'contact' table and marks missing records as removed

        Args:
            chunks (Iterator[list[tuple[str, ...]]]): Paginated contact records from Nimble API
        """
        with psycopg.connect(conninfo=str(settings.db_url)) as conn, \
        conn.cursor() as cursor:
            try:
                # Create temp table
                cursor.execute("""
                    CREATE TEMP TABLE tmp_contact(
                        external_id VARCHAR ( 50 ),
                        first_name VARCHAR ( 255 ),
                        last_name VARCHAR ( 255 ),
                        email VARCHAR ( 255 ))
                        ON COMMIT DROP;""")
                
                # Bulk copy to temp table
                for chunk in chunks:
                    with cursor.copy("""
                        COPY tmp_contact(
                            external_id,
                            first_name,
                            last_name,
                            email)
                        FROM stdin;""") as copy:
                        for row in chunk:
                            copy.write_row(row)
            
            # Add new records and update existing ones if there are changes
            # Also, sets removed=FALSE if a previously deleted record reappears
                cursor.execute("""
                INSERT INTO contact (
                    external_id,
                    first_name,
                    last_name,
                    email)
                SELECT
                    external_id,
                    first_name,
                    last_name,
                    email
                FROM tmp_contact
                ON CONFLICT (external_id)  DO UPDATE  SET
                    first_name=EXCLUDED.first_name,
                    last_name=EXCLUDED.last_name,
                    email=EXCLUDED.email,
                    removed=FALSE
                WHERE
                    contact.first_name != EXCLUDED.first_name OR
                    contact.last_name != EXCLUDED.last_name OR
                    contact.email != EXCLUDED.email OR
                    contact.removed = TRUE;""")
            
            # Mark as removed contacts that are no longer returned by the API
            # (additional index can improve this update, as shown in Appendix 1)
                cursor.execute("""
                CREATE UNIQUE INDEX tmp_contact_external_id_key ON tmp_contact (external_id);
                UPDATE contact c
                SET removed = TRUE
                WHERE
                    c.external_id IS NOT NULL AND
                    NOT EXISTS (
                        SELECT FROM tmp_contact t
                        WHERE t.external_id = c.external_id
                    );""")

                conn.commit()
            except (Exception, psycopg.DatabaseError) as error:
                print("Error: %s" % error)
                conn.rollback()
                cursor.close()
            finally:
                conn.close()
    

    @staticmethod
    def _get_params(page: int, fields: [str] = None) -> dict[str, any]:
        """Generate parameters for a query.

        Args:
            page (int): The page number.
            fields ([str]): Fields to return.

        Returns:
            dict[str, any]:A dictionary of parameters.
        """
        
        params = {'page': page}
        
        if BATCH_SIZE != DEFAULT_BATCH_SIZE:
            params['per_page'] = BATCH_SIZE

        if (fields is not None):
            params['fields'] = ','.join(fields)

        return params


    @staticmethod
    def _get_field(resource: dict[str, any], field: str) -> str:
        """Gets the field value of contact resource

        Args:
            resource (dict[str, any]): The contact from Nimble API.
            field (str): The field name.

        Returns:
            str: The value of the field.
        """
        return next(iter(resource['fields'].get(field, [])), {}).get('value')


    @staticmethod
    def _load_paginated_contacts(verbose: bool = False) -> Iterator[list[tuple[str, ...]]]:
        """Loads all contacts from the Nimble API by pages.

        Args:
            verbose (bool, optional): Whether to print status messages. Defaults to False.

        Returns:
            dict[str, any]: A dictioraty of contacts.
        """
        
        page = 0
        total = 0

        while True:
            page += 1
        
            params = ContactSync._get_params(page, ['first name','last name','email'])
            response_API = requests.get(NIMBLE_CONTACTS_ENDPOINT, headers=NIMBLE_HEADERS, params=params)

            if verbose:
                print(f'GET contacts (page={page}): status={response_API.status_code}')

            if (response_API.status_code != SUCCESS_STATUS_CODE):
                break

            page_json = response_API.json()
            
            chunk = [(
                str(x['id']),
                ContactSync._get_field(x, 'first name'),
                ContactSync._get_field(x, 'last name'),
                ContactSync._get_field(x, 'email')
            ) for x in page_json['resources']]

            yield chunk

            total += len(chunk)
            pages = page_json['meta']['pages']
        
            if page >= pages:
                break
        
        if verbose:
            print(f'loaded {len(total)} contacts')





