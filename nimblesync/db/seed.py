import os
import psycopg
from nimblesync.settings import settings

def seed_contacts(check_if_empty: bool = True, verbose: bool = False) -> None:
    print(os.getcwd())
    csv_path = settings.seed_contact_csv_path
    with open(csv_path, "r") as file, \
        psycopg.connect(conninfo=str(settings.db_url)) as conn, \
        conn.cursor() as cursor:
        try:
            run_import = True

            if check_if_empty:
                cursor.execute('SELECT COUNT(*) FROM contact')
                count = cursor.fetchone()[0]
                if count > 0:
                    run_import = False
                    if verbose:
                        print(f'Found {count} records in \'contact\' table. Skipping seed.')
            
            if run_import:
                with cursor.copy("""
                            COPY contact(
                                first_name,
                                last_name,
                                email)
                            FROM stdin (format csv, delimiter ',', quote '\"')""") as copy:
                    copy.write(file.read())

                conn.commit()
                if verbose:
                    print(f'Successfully imported seed data into \'contact\' table.')
        finally:
            conn.close()

