import logging
import requests
import time
from html import unescape
from db_utils import (create_raw_table, insert_in_raw_comics_data_batch, get_latest_comic_id_from_db,
                      insert_in_raw_comic_data)


logger = logging.getLogger(__name__)


def prepare_comic_data(comic_data):
    """
    Prepare data for insertion (can add cleaning). !!!Contain only most frequent keys
    """
    return {
        'month': int(comic_data.get('month', 0)),
        'num': comic_data['num'],
        'link': comic_data.get('link', '').strip(),
        'year': int(comic_data.get('year', 0)),
        'news': unescape(comic_data.get('news', '').strip()),
        'safe_title': comic_data.get('safe_title', '').strip(),
        'transcript': comic_data.get('transcript', '').strip(),
        'alt': comic_data.get('alt', '').strip(),
        'img': comic_data.get('img', '').strip(),
        'title': comic_data.get('title', '').strip(),
        'day': int(comic_data.get('day', 0)),
    }


def get_comic_data(comic_id=None):
    """
    Ingest comic data from the XKCD API.
    By default, ingest the latest comic. For specific comics, provide a comic_id.
    """
    if comic_id:
        url = f"https://xkcd.com/{comic_id}/info.0.json"
    else:
        url = "https://xkcd.com/info.0.json"

    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as msg:
        logger.error(f"Error getting XKCD data: {msg}")
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return None


def check_latest_comic_in_db_and_api(ti):
    """
    Check the latest comic num in db and API
    Pushes results to XCom
    """
    try:
        latest_comic_id_db = get_latest_comic_id_from_db("RAW")
        latest_comic_api = get_comic_data()
        latest_comic_id_api = latest_comic_api['num']

        ti.xcom_push(key="latest_comic_id_db", value=latest_comic_id_db)
        ti.xcom_push(key="latest_comic_id_api", value=latest_comic_id_api)
        ti.xcom_push(key="number_of_comics_to_add", value=latest_comic_id_api-latest_comic_id_db)

    except Exception as msg:
        logger.error(f"Error in check_latest_comic_in_db_and_api: {msg}")


def get_all_comics_data():
    """
    Get all comics, batch writing in db
    """
    latest_comic = get_comic_data()
    if not latest_comic:
        return False

    total_comics = latest_comic['num']
    try:
        create_raw_table()
        comics_batch = []
        for comic_id in range(1, total_comics + 1):
            if comic_id == 404:
                continue
            comic_data = get_comic_data(comic_id)
            if comic_data:
                prepared_data = prepare_comic_data(comic_data)
                comics_batch.append((
                    prepared_data['num'],
                    prepared_data['month'],
                    prepared_data['year'],
                    prepared_data['day'],
                    prepared_data['link'],
                    prepared_data['news'],
                    prepared_data['safe_title'],
                    prepared_data['transcript'],
                    prepared_data['alt'],
                    prepared_data['img'],
                    prepared_data['title']
                ))

            time.sleep(0.3)

            if len(comics_batch) == 100:
                insert_in_raw_comics_data_batch(comics_batch, "RAW")
                comics_batch = []

        if comics_batch:
            insert_in_raw_comics_data_batch(comics_batch, "RAW")
    except Exception as msg:
        logger.error(f"Error in get_all_comics_data: {msg}")


def get_missing_comics(ti):
    """
    Get missing comics based on IDs from XCom
    """
    try:
        latest_comic_id_db = ti.xcom_pull(key="latest_comic_id_db", task_ids="check_latest_comic")
        latest_comic_id_api = ti.xcom_pull(key="latest_comic_id_api", task_ids="check_latest_comic")

        if latest_comic_id_db < latest_comic_id_api:

            comics_batch = []
            for comic_id in range(latest_comic_id_db + 1, latest_comic_id_api + 1):
                comic_data = get_comic_data(comic_id)
                if comic_data:
                    prepared_data = prepare_comic_data(comic_data)
                    comics_batch.append((
                        prepared_data['num'],
                        prepared_data['month'],
                        prepared_data['year'],
                        prepared_data['day'],
                        prepared_data['link'],
                        prepared_data['news'],
                        prepared_data['safe_title'],
                        prepared_data['transcript'],
                        prepared_data['alt'],
                        prepared_data['img'],
                        prepared_data['title']
                    ))

                if len(comics_batch) == 100:
                    insert_in_raw_comics_data_batch(comics_batch, "RAW")
                    comics_batch = []

            if comics_batch:
                insert_in_raw_comics_data_batch(comics_batch, "RAW")

    except Exception as msg:
        logger.error(f"Error in get_missing_comics: {msg}")


def import_new_comic():
    """
    Get a single new comic from API
    """
    try:
        comic_data = get_comic_data()

        if comic_data:
            prepared_data = prepare_comic_data(comic_data)
            insert_in_raw_comic_data(prepared_data)

    except Exception as msg:
        logger.error(f"Error in import_new_comic: {msg}")


def wait_for_new_data(ti):
    latest_comic_id_db = ti.xcom_pull(key="latest_comic_id_db", task_ids="check_latest_comic")
    comic_data = get_comic_data()
    latest_comic_id_api = comic_data['num']

    if latest_comic_id_api - latest_comic_id_db == 1:
        prepared_data = prepare_comic_data(comic_data)
        insert_in_raw_comic_data(prepared_data)
    else:
        raise Exception("Waiting for new data, retrying...")


def branch_comic_import_based_on_ids(ti):
    """
    Based on the value of numbers of comics to add (check_latest_comic_in_db_and_api), decide which task to execute
    """
    comics_to_import = ti.xcom_pull(key="number_of_comics_to_add")

    if comics_to_import > 1:
        return 'import_missing_comics'
    elif comics_to_import == 1:
        return 'import_new_comic'
    else:
        return 'wait_for_new_comic'
