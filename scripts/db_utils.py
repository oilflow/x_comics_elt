import logging
import psycopg2
import re
from db_config import (POSTGRES_RAW_DB, POSTGRES_RAW_USER, POSTGRES_RAW_PASSWORD, POSTGRES_RAW_HOST,
                       POSTGRES_DWH_DB, POSTGRES_DWH_USER, POSTGRES_DWH_PASSWORD, POSTGRES_DWH_HOST)
from generate_authors import generate_random_author


logger = logging.getLogger(__name__)


def connect_to_db(user, pwd, db, host):
    """
    Connection to PostgreSQL db
    """
    try:
        conn = psycopg2.connect(
            user=user,
            password=pwd,
            database=db,
            host=host,
            port=5432
        )
        print(conn)
        return conn
    except Exception as msg:
        logger.error(f"Error connecting to {db} database: {msg}")
        raise


def create_raw_table():
    """
    Create the comics raw table
    """
    try:
        conn = connect_to_db(POSTGRES_RAW_USER, POSTGRES_RAW_PASSWORD, POSTGRES_RAW_DB, POSTGRES_RAW_HOST)
        if conn:
            with conn.cursor() as cur:
                cur.execute("""
                        CREATE TABLE IF NOT EXISTS raw_comics (
                            num INTEGER PRIMARY KEY,
                            month INTEGER,
                            year INTEGER,
                            day INTEGER,
                            link TEXT,
                            news TEXT,
                            safe_title TEXT,
                            transcript TEXT,
                            alt TEXT,
                            img TEXT,
                            title TEXT,
                            write_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                conn.commit()  # Commit within the 'with' block after executing the statement
        else:
            logger.error("Connection to database failed. Table creation aborted.")
    except Exception as msg:
        logger.error(f"Error creating table: {msg}")
        if conn:
            conn.rollback()
        raise


# def get_latest_comic_id_from_db():
#     """
#     Get the latest comic ID (num)
#     """
#     try:
#         sql = "SELECT MAX(num) FROM raw_comics"
#         conn = connect_to_db(POSTGRES_RAW_USER, POSTGRES_RAW_PASSWORD, POSTGRES_RAW_DB, POSTGRES_RAW_HOST)
#         with conn.cursor() as cur:
#             cur.execute(sql)
#             result = cur.fetchone()
#             return result[0] if result[0] else None
#     except Exception as msg:
#         logger.error(f"Error retrieving latest comic ID: {msg}")


def get_latest_comic_id_from_db(db_id):
    """
    Get the latest comic ID (num) based on the database ID (RAW or DWH).
    """
    try:
        if db_id == "RAW":
            sql = "SELECT MAX(num) FROM raw_comics"
            conn = connect_to_db(POSTGRES_RAW_USER, POSTGRES_RAW_PASSWORD, POSTGRES_RAW_DB, POSTGRES_RAW_HOST)
        elif db_id == "DWH":
            sql = "SELECT MAX(num) FROM dim_comics"
            conn = connect_to_db(POSTGRES_DWH_USER, POSTGRES_DWH_PASSWORD, POSTGRES_DWH_DB, POSTGRES_DWH_HOST)
        else:
            raise ValueError(f"Unsupported db_id: {db_id}")

        with conn.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchone()
            return result[0] if result[0] else None
    except Exception as msg:
        logger.error(f"Error retrieving latest comic ID from {db_id}: {msg}")
        return None


def is_raw_table_exists():
    """
    Check if table exists in the db
    """
    sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'raw_comics'"
    try:
        conn = connect_to_db(POSTGRES_RAW_USER, POSTGRES_RAW_PASSWORD, POSTGRES_RAW_DB, POSTGRES_RAW_HOST)
        with conn.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchone()
            if result[0] == 1:
                return 'check_latest_comic'
            else:
                return 'import_all_comics'
    except Exception as msg:
        logger.error(f"Error checking if raw_comics table exists: {msg}")
        raise
    finally:
        if conn:
            conn.close()


def insert_in_raw_comic_data(comic_data):
    """
    Insert single comic in raw db
    """
    sql = """
        INSERT INTO raw_comics (num, month, year, day, link, news, safe_title, transcript, alt, img, title, write_ts)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (num) DO NOTHING
    """
    try:
        conn = connect_to_db(POSTGRES_RAW_USER, POSTGRES_RAW_PASSWORD, POSTGRES_RAW_DB, POSTGRES_RAW_HOST)
        with conn.cursor() as cur:
            cur.execute(sql, (
                comic_data['num'],
                comic_data['month'],
                comic_data['year'],
                comic_data['day'],
                comic_data['link'],
                comic_data['news'],
                comic_data['safe_title'],
                comic_data['transcript'],
                comic_data['alt'],
                comic_data['img'],
                comic_data['title']
            ))
            conn.commit()
    except Exception as msg:
        logger.error(f"Error inserting comic data: {msg}")
        conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

# def insert_in_raw_comics_data_batch(comics_data):
#     """
#     Insert batch of comics in raw db
#     """
#     sql = """
#         INSERT INTO raw_comics (num, month, year, day, link, news, safe_title, transcript, alt, img, title, write_ts)
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
#         ON CONFLICT (num) DO NOTHING
#     """
#     try:
#         conn = connect_to_db(POSTGRES_RAW_USER, POSTGRES_RAW_PASSWORD, POSTGRES_RAW_DB, POSTGRES_RAW_HOST)
#         with conn.cursor() as cur:
#             cur.executemany(sql, comics_data)
#             conn.commit()
#     except Exception as msg:
#         logger.error(f"Error inserting comics batch: {msg}")
#         conn.rollback()


def insert_in_raw_comics_data_batch(comics_data, db_id):
    """
    Insert batch of comics in raw db
    """
    try:
        if db_id == "RAW":
            sql = """
                    INSERT INTO raw_comics (num, month, year, day, link, news, safe_title, transcript, alt, img, title, write_ts)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (num) DO NOTHING
                """
            conn = connect_to_db(POSTGRES_RAW_USER, POSTGRES_RAW_PASSWORD, POSTGRES_RAW_DB, POSTGRES_RAW_HOST)
        elif db_id == "DWH":
            sql = """
                    INSERT INTO temp_missing_comics (num, month, year, day, link, news, safe_title, transcript, alt, img, title, write_ts)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (num) DO NOTHING
                """
            conn = connect_to_db(POSTGRES_DWH_USER, POSTGRES_DWH_PASSWORD, POSTGRES_DWH_DB, POSTGRES_DWH_HOST)
        else:
            raise ValueError(f"Unsupported db_id: {db_id}")

        with conn.cursor() as cur:
            cur.executemany(sql, comics_data)
            conn.commit()
    except Exception as msg:
        logger.error(f"Error inserting comics batch: {msg}")
        conn.rollback()
        raise


def create_dim_comic_author_table():
    """
    Create the dim_comic_author table in DWH.
    """
    sql = """
        CREATE TABLE IF NOT EXISTS dim_comic_author (
            author_id SERIAL PRIMARY KEY,
            author_name VARCHAR(100) NOT NULL,
            email VARCHAR(150) NOT NULL UNIQUE,
            write_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """
    try:
        conn = connect_to_db(POSTGRES_DWH_USER, POSTGRES_DWH_PASSWORD, POSTGRES_DWH_DB, POSTGRES_DWH_HOST)
        if conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
        else:
            logger.error("Connection to DWH failed. Table creation aborted.")
    except Exception as msg:
        logger.error(f"Error creating dim_comic_author table: {msg}")
        conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def create_dim_date_table():
    """
    Create the dim_date table in DWH.
    """
    sql = """
        CREATE TABLE IF NOT EXISTS dim_date (
            date DATE NOT NULL PRIMARY KEY,
            day INT NOT NULL,
            month INT NOT NULL,
            year INT NOT NULL,
            day_of_week INT NOT NULL,
            quarter INT NOT NULL
        );
    """
    try:
        conn = connect_to_db(POSTGRES_DWH_USER, POSTGRES_DWH_PASSWORD, POSTGRES_DWH_DB, POSTGRES_DWH_HOST)
        if conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
        else:
            logger.error("Connection to DWH failed. Table creation aborted.")
    except Exception as msg:
        logger.error(f"Error creating dim_date table: {msg}")
        conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def create_dim_comics_table():
    """
    Create the dim_comics table in DWH.
    """
    sql = """
        CREATE TABLE IF NOT EXISTS dim_comics (
            num INT NOT NULL PRIMARY KEY,
            safe_title VARCHAR(200) NOT NULL,
            title VARCHAR(200) NOT NULL,
            img_url VARCHAR(255) NOT NULL, 
            transcript TEXT,
            alt TEXT,
            link TEXT,
            news TEXT,
            creation_cost DECIMAL(10, 2) NOT NULL,
            publish_date DATE NOT NULL,
            author_id INT NOT NULL,
            write_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (publish_date) REFERENCES dim_date(date),
            FOREIGN KEY (author_id) REFERENCES dim_comic_author(author_id)
        );
    """
    try:
        conn = connect_to_db(POSTGRES_DWH_USER, POSTGRES_DWH_PASSWORD, POSTGRES_DWH_DB, POSTGRES_DWH_HOST)
        if conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
        else:
            logger.error("Connection to DWH failed. Table creation aborted.")
    except Exception as msg:
        logger.error(f"Error creating dim_comics table: {msg}")
        conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def create_fact_comic_metrics_table():
    """
    Create the fact_comic_metrics table in DWH.
    """
    sql = """
        CREATE TABLE IF NOT EXISTS fact_comic_metrics (
            id SERIAL PRIMARY KEY,
            comic_num INT NOT NULL,
            date DATE NOT NULL,
            views INT NOT NULL,
            customer_rating DECIMAL(3, 1) NOT NULL,
            write_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (comic_num) REFERENCES dim_comics(num),
            FOREIGN KEY (date) REFERENCES dim_date(date),
            CONSTRAINT unique_comic_date UNIQUE (comic_num, date) 
        );
    """
    try:
        conn = connect_to_db(POSTGRES_DWH_USER, POSTGRES_DWH_PASSWORD, POSTGRES_DWH_DB, POSTGRES_DWH_HOST)
        if conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
        else:
            logger.error("Connection to DWH failed. Table creation aborted.")
    except Exception as msg:
        logger.error(f"Error creating fact_comic_metrics table: {msg}")
        conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def get_missing_comics_from_raw(latest_dwh_id):
    """
    Get missing comics from the RAW
    """
    try:
        # Connect to the RAW database
        conn = connect_to_db(POSTGRES_RAW_USER, POSTGRES_RAW_PASSWORD, POSTGRES_RAW_DB, POSTGRES_RAW_HOST)

        sql_fetch_missing = """
                            SELECT num, month, year, day, link, news, safe_title, transcript, alt, img, title 
                                FROM raw_comics WHERE num > %s
                            """
        with conn.cursor() as cur:
            cur.execute(sql_fetch_missing, (latest_dwh_id,))
            missing_comics = cur.fetchall()
            logger.info(f"Fetched {len(missing_comics)} missing comics from RAW database.")
            return missing_comics

    except Exception as msg:
        logger.error(f"Error get_missing_comics_from_raw: {msg}")
        return []
    finally:
        if conn:
            conn.close()


def create_temp_table_in_dwh():
    """
    Create a temp table in the DWH  with te same structure as raw_comics from the RAW
    """
    # same structure as the RAW layer
    sql = """
            CREATE TABLE IF NOT EXISTS temp_missing_comics (
                num INTEGER PRIMARY KEY,
                month INTEGER,
                year INTEGER,
                day INTEGER,
                link TEXT,
                news TEXT,
                safe_title TEXT,
                transcript TEXT,
                alt TEXT,
                img TEXT,
                title TEXT,
                write_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
    try:
        conn = connect_to_db(POSTGRES_DWH_USER, POSTGRES_DWH_PASSWORD, POSTGRES_DWH_DB, POSTGRES_DWH_HOST)
        if conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
        else:
            logger.error("Connection to DWH failed. Table creation aborted.")
    except Exception as msg:
        logger.error(f"Error creating fact_comic_metrics table: {msg}")
        conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def create_invalid_comics_table_in_dwh():
    """
    Create a temp table in the DWH  with te same structure as raw_comics from the RAW
    """
    # same structure as the RAW layer
    sql = """
            CREATE TABLE IF NOT EXISTS invalid_comics (
                num INTEGER PRIMARY KEY,
                month INTEGER,
                year INTEGER,
                day INTEGER,
                link TEXT,
                news TEXT,
                safe_title TEXT,
                transcript TEXT,
                alt TEXT,
                img TEXT,
                title TEXT,
                write_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
    try:
        conn = connect_to_db(POSTGRES_DWH_USER, POSTGRES_DWH_PASSWORD, POSTGRES_DWH_DB, POSTGRES_DWH_HOST)
        if conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
        else:
            logger.error("Connection to DWH failed. Table creation aborted.")
    except Exception as msg:
        logger.error(f"Error creating fact_comic_metrics table: {msg}")
        conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def log_invalid_comic_to_dwh(comics):
    """
    Logging invalid records to invalid_comics table
    """
    sql = """
            INSERT INTO invalid_comics (num, month, year, day, link, news, safe_title, transcript, alt, img, title)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (num) DO NOTHING
        """
    try:
        conn = connect_to_db(POSTGRES_DWH_USER, POSTGRES_DWH_PASSWORD, POSTGRES_DWH_DB, POSTGRES_DWH_HOST)

        with conn.cursor() as cur:
            for comic in comics:
                cur.execute(sql, comic)
            conn.commit()
            logger.info(f"Logged {len(comics)} invalid records to DWH.")

    except Exception as msg:
        logger.error(f"Error logging invalid records to DWH: {msg}")
        raise
    finally:
        if conn:
            conn.close()


def check_authors_count():
    """
    Check count of authors in the dim_comic_author
    """
    sql_check = "SELECT COUNT(*) FROM dim_comic_author"
    try:
        conn = connect_to_db(POSTGRES_DWH_USER, POSTGRES_DWH_PASSWORD, POSTGRES_DWH_DB, POSTGRES_DWH_HOST)
        with conn.cursor() as cur:
            cur.execute(sql_check)
            result = cur.fetchone()
            return result[0]
    except Exception as msg:
        logger.error(f"Error check_authors_count: {msg}")
        return 0
    finally:
        if conn:
            conn.close()


def insert_in_dim_comic_author(authors):
    """
    Insert batch of authors into dim_comic_author table.
    """
    sql = """
        INSERT INTO dim_comic_author (author_name, email)
        VALUES (%s, %s)
    """
    try:
        conn = connect_to_db(POSTGRES_DWH_USER, POSTGRES_DWH_PASSWORD, POSTGRES_DWH_DB, POSTGRES_DWH_HOST)
        with conn.cursor() as cur:
            cur.executemany(sql, authors)
            conn.commit()
    except Exception as msg:
        logger.error(f"Error inserting authors: {msg}")
        conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def get_max_date_from_dim_date():
    """
    Get max date from  dim_date.
    """
    sql = "SELECT MAX(date) FROM dim_date"
    try:
        conn = connect_to_db(POSTGRES_DWH_USER, POSTGRES_DWH_PASSWORD, POSTGRES_DWH_DB, POSTGRES_DWH_HOST)

        with conn.cursor() as cur:
            cur.execute(sql)
            max_date = cur.fetchone()[0]

        return max_date

    except Exception as msg:
        logger.error(f"Error getting max date from dim_date: {msg}")
        raise
    finally:
        if conn:
            conn.close()


def insert_dim_date(authors):
    """
    Insert batch of authors into dim_comic_author table.
    """
    sql = """
            INSERT INTO dim_date (date, day, month, year, day_of_week, quarter)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
    try:
        conn = connect_to_db(POSTGRES_DWH_USER, POSTGRES_DWH_PASSWORD, POSTGRES_DWH_DB, POSTGRES_DWH_HOST)
        with conn.cursor() as cur:
            cur.executemany(sql, authors)
            conn.commit()
    except Exception as msg:
        logger.error(f"Error inserting records into dim_date: {msg}")
        conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def get_date_id_from_dim_date(year, month, day):
    """
    Get date_id from dim_date based on year, month and day
    """
    sql = """
        SELECT date FROM dim_date 
        WHERE year = %s AND month = %s AND day = %s
    """
    try:
        conn = connect_to_db(POSTGRES_DWH_USER, POSTGRES_DWH_PASSWORD, POSTGRES_DWH_DB, POSTGRES_DWH_HOST)
        with conn.cursor() as cur:
            cur.execute(sql, (year, month, day))
            result = cur.fetchone()
            return result[0] if result else None
    except Exception as msg:
        logger.error(f"Error getting date_id from dim_date: {msg}")
        raise
    finally:
        if conn:
            conn.close()


def insert_dim_comics(comics_data):
    """
    Insert data into dim_comics
    """
    sql = """
        INSERT INTO dim_comics 
        (num, safe_title, title, img_url, transcript, alt, link, news, creation_cost, publish_date, author_id, write_ts)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (num) DO NOTHING
    """
    try:
        conn = connect_to_db(POSTGRES_DWH_USER, POSTGRES_DWH_PASSWORD, POSTGRES_DWH_DB, POSTGRES_DWH_HOST)
        with conn.cursor() as cur:
            cur.executemany(sql, comics_data)
            conn.commit()
    except Exception as msg:
        logger.error(f"Error inserting records into dim_comics: {msg}")
        conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def get_temp_missing_comics():
    """
    Get data from temp_missing_comics
    """
    sql = """
            SELECT num, safe_title, title, img, transcript, alt, link, news, year, month, day 
            FROM temp_missing_comics ORDER BY num ASC
        """
    try:
        conn = connect_to_db(POSTGRES_DWH_USER, POSTGRES_DWH_PASSWORD, POSTGRES_DWH_DB, POSTGRES_DWH_HOST)
        with conn.cursor() as cur:
            cur.execute(sql)
            comics = cur.fetchall()
            return comics
    except Exception as msg:
        logger.error(f"Error getting data from temp_missing_comics: {msg}")
        raise
    finally:
        if conn:
            conn.close()


def get_comics_from_temp_for_metrics():
    """
    Extract comic nums and date from temp_missing_comics
    """
    sql = """
        SELECT num,  MAKE_DATE(year, month, day) + INTERVAL '1 day' AS date
        FROM temp_missing_comics
        ORDER BY num ASC;
    """
    try:
        conn = connect_to_db(POSTGRES_DWH_USER, POSTGRES_DWH_PASSWORD, POSTGRES_DWH_DB, POSTGRES_DWH_HOST)
        with conn.cursor() as cur:
            cur.execute(sql)
            comics = cur.fetchall()
        return comics
    except Exception as msg:
        logger.error(f"Error extracting comics from temp_missing_comics: {msg}")
        raise
    finally:
        if conn:
            conn.close()


def insert_into_fact_comic_metrics(comic_num, date, views, customer_rating):
    """
    Insert data into fact_comic_metrics
    """
    sql = """
        INSERT INTO fact_comic_metrics (comic_num, date, views, customer_rating)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (comic_num, date) DO NOTHING;
    """
    try:
        conn = connect_to_db(POSTGRES_DWH_USER, POSTGRES_DWH_PASSWORD, POSTGRES_DWH_DB, POSTGRES_DWH_HOST)
        with conn.cursor() as cur:
            cur.execute(sql, (comic_num, date, views, customer_rating))
            conn.commit()
    except Exception as msg:
        logger.error(f"Error inserting into fact_comic_metrics: {msg}")
        conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def delete_temp_table():
    try:
        conn = connect_to_db(POSTGRES_DWH_USER, POSTGRES_DWH_PASSWORD, POSTGRES_DWH_DB, POSTGRES_DWH_HOST)
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS temp_missing_comics;")
            conn.commit()
    except Exception as msg:
        logger.error(f"Error deleting temp_missing_comics: {msg}")
        raise
    finally:
        if conn:
            conn.close()