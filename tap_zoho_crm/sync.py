# pylint: disable=too-many-lines
from datetime import date, datetime, timedelta
import time
from urllib.parse import urlparse
from dateutil.parser import parse

import singer
from singer import metrics, utils
from singer.utils import strptime_to_utc

LOGGER = singer.get_logger()
DEFAULT_START_DATE = '2010-01-01T00:00:00'

STREAMS = {
    'leads': {
        'module_name': 'Leads',
        'params': {
            'per_page': 200,
            'sort_by': "Modified_Time",
            'sort_order': "asc"
        },
        'bookmark_key': 'Modified_Time',
    },
    'contacts': {
        'module_name': 'Contacts',
        'params': {
            'per_page': 200,
            'sort_by': "Modified_Time",
            'sort_order': "asc"
        },
        'bookmark_key': 'Modified_Time',
    },
    'deals': {
        'module_name': 'Deals',
        'params': {
            'per_page': 200,
            'sort_by': "Modified_Time",
            'sort_order': "asc"
        },
        'bookmark_key': 'Modified_Time',
    },
    'accounts': {
        'module_name': 'Accounts',
        'params': {
            'per_page': 200,
            'sort_by': "Modified_Time",
            'sort_order': "asc"
        },
        'bookmark_key': 'Modified_Time',
    },
    'events': {
        'module_name': 'Events',
        'params': {
            'per_page': 200,
            'sort_by': "Modified_Time",
            'sort_order': "asc"
        },
        'bookmark_key': 'Modified_Time',
    },
    'activities': {
        'module_name': 'Activities',
        'params': {
            'per_page': 200,
            'sort_by': "Modified_Time",
            'sort_order': "asc"
        },
        'bookmark_key': 'Modified_Time',
    }
}


# not used yet
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ("currently_syncing" in state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def write_record(stream_name, record, time_extracted):
    try:
        singer.messages.write_record(
            stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.error(
            "Stream: {} - OS Error writing record".format(stream_name))
        LOGGER.error("record: {}".format(record))
        raise err


def get_bookmark(state, stream, default):
    # default only populated on initial sync
    if (state is None) or ("bookmarks" not in state):
        return default
    return state.get("bookmarks", {}).get(stream, default)


def write_bookmark(state, stream, value):
    if "bookmarks" not in state:
        state["bookmarks"] = {}
    state["bookmarks"][stream] = value
    LOGGER.info(
        "Stream: {} - Write state, bookmark value: {}".format(stream, value))
    singer.write_state(state)


def sync(client, config, state):
    start_date = config.get("start_date") or DEFAULT_START_DATE
    bookmark_value = None

    for stream_name, stream_metadata in STREAMS.items():
        with metrics.record_counter(stream_name) as counter:
            try:
                initial_bookmark_value = get_bookmark(
                    state, stream_name, start_date)
                last_bookmark_value_dt = strptime_to_utc(
                    initial_bookmark_value)
                bookmark_key = stream_metadata['bookmark_key']
                params = stream_metadata['params']
                params['modified_since'] = initial_bookmark_value
                for record in client.paginate_generator(stream_metadata['module_name'], **params):
                    bookmark_value = record[bookmark_key]
                    bookmark_value_dt = strptime_to_utc(bookmark_value)
                    if bookmark_value_dt < last_bookmark_value_dt:
                        raise RuntimeError(
                            f"out of order data seen!, last_bookmark_value: '{last_bookmark_value_dt}', new_book_mark_value: '{bookmark_value_dt}', full record: {record}")
                    write_record(
                        stream_name, record, time_extracted=utils.now()
                    )
                    write_bookmark(state, stream_name, bookmark_value)
                    counter.increment()
                    last_bookmark_value_dt = bookmark_value_dt

            except:
                LOGGER.exception(f"Error during sync of {stream_name}")
                raise
            finally:
                if bookmark_value is not None:
                    write_bookmark(state, stream_name, bookmark_value)
