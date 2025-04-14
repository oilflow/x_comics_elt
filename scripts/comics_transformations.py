import datetime
import logging
import random
import re
from generate_authors import generate_random_author
from db_utils import (create_dim_date_table, create_dim_comics_table, create_dim_comic_author_table,
                      create_fact_comic_metrics_table, get_latest_comic_id_from_db, get_missing_comics_from_raw,
                      create_temp_table_in_dwh, insert_in_raw_comics_data_batch, create_invalid_comics_table_in_dwh,
                      log_invalid_comic_to_dwh, check_authors_count, insert_in_dim_comic_author, insert_dim_date,
                      get_max_date_from_dim_date, get_temp_missing_comics, get_date_id_from_dim_date, insert_dim_comics,
                      get_comics_from_temp_for_metrics, insert_into_fact_comic_metrics)


logger = logging.getLogger(__name__)


def create_model_if_not_exist():
    """"
    DWH Model creation
    """
    create_dim_comic_author_table()
    create_dim_date_table()
    create_dim_comics_table()
    create_fact_comic_metrics_table()


def validation_comic_data(comic):
    """
    Checking comic data for correctness
    """
    num, month, year, day, link, news, safe_title, transcript, alt, img, title = comic

    current_year = datetime.datetime.now().year
    if not (2005 <= year <= current_year):
        return False

    if not (1 <= month <= 12):
        return False

    if not (1 <= day <= 31):
        return False

    try:
        datetime.datetime(year, month, day)
    except ValueError:
        return False

    return True


def check_last_comic_in_dwh_and_raw_save_in_temp_table():
    """
    Compare the latest comic in RAW and DWH and save missing in a temp table in DWH
    """
    try:
        # Step 1 Get the latest comic nums
        latest_raw_id = get_latest_comic_id_from_db("RAW")
        latest_dwh_id = get_latest_comic_id_from_db("DWH")

        if latest_dwh_id is None:
            logger.warning("DWH comics table is empty yet.")
            latest_dwh_id = 0

        if latest_raw_id > latest_dwh_id:
            # Step 2: Get missing comics from RAW
            missing_comics = get_missing_comics_from_raw(latest_dwh_id)

            # Step 3: Data validation (date only can expand)
            invalid_comics = []
            valid_comics = []
            for comic in missing_comics:
                if not validation_comic_data(comic):
                    invalid_comics.append(comic)
                else:
                    valid_comics.append(comic)

            # Step 4: Create valid comic (temp_missing_comics) table in DWH and create invalid comic (invalid_comics) table
            create_temp_table_in_dwh()
            create_invalid_comics_table_in_dwh()

            # Step 5: Save missing comics into the temp table
            insert_in_raw_comics_data_batch(valid_comics, "DWH")
            log_invalid_comic_to_dwh(invalid_comics)
        else:
            logger.info("No updates required.")

    except Exception as msg:
        logger.error(f"Error while comparing and saving comics: {msg}")
        raise


def fill_dim_comic_author():
    """
    Insert unique authors in dim_comic_author if less than 30 authors.
    """
    try:
        authors_count = check_authors_count()

        if authors_count < 30:
            authors = set()
            while len(authors) < (30 - authors_count):
                author_name, email = generate_random_author()
                authors.add((author_name, email))

            insert_in_dim_comic_author(list(authors))
    except Exception as msg:
        logger.error(f"Error filling dim_comic_author: {msg}")
        raise


def fill_dim_date():
    """
    Fills the dim_date with dates
    If the table is empty -> from 2006-01-01 to the end of the current year
    If the table contains data, add only the new year dates
    """
    try:
        current_date = datetime.datetime.now().date()
        current_year = current_date.year
        current_date_end_of_year = datetime.datetime(current_year, 12, 31).date()

        max_date_in_dim_date = get_max_date_from_dim_date()

        if not max_date_in_dim_date:
            start_date = datetime.datetime(2006, 1, 1).date()
            end_date = current_date_end_of_year
        elif max_date_in_dim_date < current_date_end_of_year:
            start_date = max_date_in_dim_date + datetime.timedelta(days=1)
            end_date = current_date_end_of_year
        else:
            return

        current_date = start_date
        dates_data = []

        while current_date <= end_date:
            day_of_week = current_date.weekday() + 1
            quarter = (current_date.month - 1) // 3 + 1
            date_value = (current_date, current_date.day, current_date.month,
                          current_date.year, day_of_week, quarter)
            dates_data.append(date_value)
            current_date += datetime.timedelta(days=1)

        insert_dim_date(dates_data)

    except Exception as msg:
        logger.error(f"Error filling dim_date: {msg}")
        raise


def fill_dim_comics():
    """
    Fill dim_comics with data from temp_missing_comics
    """
    try:
        comics = get_temp_missing_comics()

        comics_data = []
        for comic in comics:
            num, safe_title, title, img, transcript, alt, link, news, year, month, day = comic
            publish_date_id = get_date_id_from_dim_date(year, month, day)
            if not publish_date_id:
                logger.warning(f"No matching date_id for comic {num} with date {year}-{month}-{day}. Skipping...")
                continue

            author_id = random.randint(1, 30)
            creation_cost = len(re.sub(r'[^A-Za-zА-Яа-я]', '', title)) * 5
            comics_data.append((
                num, safe_title, title, img, transcript, alt, link, news, creation_cost, publish_date_id, author_id
            ))

        if comics_data:
            insert_dim_comics(comics_data)
            logger.info(f"Inserted {len(comics_data)} comics into dim_comics.")
        else:
            logger.info("No data to insert into dim_comics.")
    except Exception as msg:
        logger.error(f"Error filling dim_comics: {msg}")
        raise


def fill_fact_comic_metrics():
    """
    Fill the fact_comic_metrics table by generating random data.
    """
    try:
        comics = get_comics_from_temp_for_metrics()

        for comic in comics:
            print(comic)
            comic_num = comic[0]
            date = comic[1]
            views = int(random.random() * 10000)
            customer_rating = round(random.uniform(1.0, 10.0), 1)

            insert_into_fact_comic_metrics(comic_num, date, views, customer_rating)

    except Exception as msg:
        logger.error(f"Error filling fact_comic_metrics: {msg}")
        raise
