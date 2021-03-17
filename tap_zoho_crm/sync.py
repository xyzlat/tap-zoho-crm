# pylint: disable=too-many-lines
from datetime import date, datetime, timedelta
import time
from urllib.parse import urlparse
from dateutil.parser import parse
from tap_zoho_crm.client import ZohoFeatureNotEnabled

import singer
from singer import metrics, utils
from singer.utils import strptime_to_utc

LOGGER = singer.get_logger()
DEFAULT_START_DATE = "2010-01-01T00:00:00"

NON_PAGINATE_MODULES = [
    {"module_name": "org"},
    {
        "module_name": "settings/stages",
        "stream_name": "settings_stages",
        "params": {"module": "Deals"},
    },
]

KNOWN_SUBMODULES = {
    "Deals": [
        {
            "module_name": "Stage_History",
            "stream_name": "deals_stage_history",
        }
    ]
}


def update_currently_syncing(state, stream_name=None):

    if (stream_name is None) and ("currently_syncing" in state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def write_record(stream_name, record, time_extracted):
    try:
        singer.messages.write_record(stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.error("Stream: {} - OS Error writing record".format(stream_name))
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
    LOGGER.info("Stream: {} - Write state, bookmark value: {}".format(stream, value))
    singer.write_state(state)


def sync(client, config, state):
    start_date = config.get("start_date") or DEFAULT_START_DATE
    bookmark_value = None

    modules = client.fetch_list_of_modules()
    api_accessible_modules = [
        {
            "module_name": m["api_name"],
            "params": {
                "per_page": 200,
                "sort_by": "Modified_Time",
                "sort_order": "asc",
            },
            "bookmark_key": "Modified_Time",
        }
        for m in modules
        if m["api_supported"] and m["profiles"]
    ]

    LOGGER.warning(
        f"skipping modules because they are either api_disabled or does not have associated profiles: {[{k:m[k] for k in ['api_name', 'api_supported', 'profiles']}for m in modules if  not(m['api_supported'] and m['profiles'])]}"
    )

    for stream_metadata in api_accessible_modules + NON_PAGINATE_MODULES:

        stream_name = stream_metadata.get("stream_name", stream_metadata["module_name"])

        sub_modules = KNOWN_SUBMODULES.get(stream_metadata["module_name"]) or []

        update_currently_syncing(state, stream_name)

        with metrics.record_counter(stream_name) as counter:
            bookmark_key = bookmark_value_dt = None
            try:
                initial_bookmark_value = get_bookmark(state, stream_name, start_date)
                last_bookmark_value_dt = strptime_to_utc(initial_bookmark_value)

                bookmark_key = stream_metadata.get("bookmark_key")

                params = stream_metadata.get("params") or {}
                per_page = params.get("per_page")

                if per_page:
                    # paginating
                    params["modified_since"] = last_bookmark_value_dt.isoformat()
                    records_generator = client.paginate_generator(
                        stream_metadata["module_name"], **params
                    )
                else:
                    records_generator = client.paginate_one_page_results(
                        stream_metadata["module_name"], **params
                    )

                for record in records_generator:
                    for sub_module in sub_modules:
                        for sub_record in client.paginate_generator(
                            f"{stream_metadata['module_name']}/{record['id']}/{sub_module['module_name']}",
                            **sub_module.get("params", {}),
                        ):
                            write_record(
                                sub_module["stream_name"],
                                sub_record,
                                time_extracted=utils.now(),
                            )

                    if bookmark_key:
                        bookmark_value = record[bookmark_key]
                        bookmark_value_dt = strptime_to_utc(bookmark_value)
                        if bookmark_value_dt < last_bookmark_value_dt:
                            raise RuntimeError(
                                f"out of order data seen!, last_bookmark_value: '{last_bookmark_value_dt}', new_book_mark_value: '{bookmark_value_dt}', full record: {record}"
                            )
                        write_record(stream_name, record, time_extracted=utils.now())
                        write_bookmark(
                            state, stream_name, bookmark_value_dt.isoformat()
                        )
                        last_bookmark_value_dt = bookmark_value_dt
                    else:
                        # no bookmark
                        write_record(stream_name, record, time_extracted=utils.now())
                    counter.increment()
            except ZohoFeatureNotEnabled:
                LOGGER.warning(
                    f"Skipping stream_name: {stream_name} as its not enabled for customer"
                )
            except:
                LOGGER.exception(f"Error during sync of {stream_name}")
                raise
            finally:
                update_currently_syncing(state)

                if bookmark_key and bookmark_value_dt is not None:
                    write_bookmark(state, stream_name, bookmark_value_dt.isoformat())
