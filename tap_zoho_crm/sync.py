# pylint: disable=too-many-lines
from datetime import date, datetime, timedelta
import time
from urllib.parse import urlparse
from dateutil.parser import parse
from tap_zoho_crm.client import ZohoFeatureNotEnabled
from tap_zoho_crm.modules import (
    NON_PAGINATE_MODULES,
    PAGINATE_MODULES,
    KNOWN_SUBMODULES,
)
import json

import singer
from singer import metrics, utils
from singer.utils import strptime_to_utc

LOGGER = singer.get_logger()
DEFAULT_START_DATE = "2010-01-01T00:00:00"


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


def update_bookmark(state, stream, value):
    if "bookmarks" not in state:
        state["bookmarks"] = {}
    state["bookmarks"][stream] = value


def sync(client, config, state):
    start_date = config.get("start_date") or DEFAULT_START_DATE
    bookmark_value = None

    modules = client.fetch_list_of_modules()

    skipped_modules = []
    selected_paginate_modules = {}
    for m in modules:
        module_config = PAGINATE_MODULES.get(m["api_name"])
        if module_config:
            selected_paginate_modules[m["api_name"]] = module_config
        else:
            skipped_modules.append(m)

    skipped_modules = [m for m in modules if not PAGINATE_MODULES.get(m["api_name"])]

    LOGGER.warning(
        f"skipping modules not in modules list due to not being needed, being api_disabled etc, skipped modules: {json.dumps(skipped_modules)}"
    )

    for module_name, stream_metadata in [
        *selected_paginate_modules.items(),
        *NON_PAGINATE_MODULES.items(),
    ]:

        stream_name = stream_metadata.get("stream_name", module_name)

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
                            sub_record["parent_id"] = record["id"]
                            write_record(
                                sub_module["stream_name"],
                                sub_record,
                                time_extracted=utils.now(),
                            )
                            sub_bookmark_key = sub_module.get("bookmark_key")
                            if sub_bookmark_key:
                                sub_bookmark_value = sub_record[sub_bookmark_key]
                            else:
                                sub_bookmark_value = utils.now().isoformat()

                            update_bookmark(
                                state, sub_module["stream_name"], sub_bookmark_value
                            )

                    if bookmark_key:
                        try:
                            bookmark_value = record[bookmark_key]
                        except:
                            LOGGER.exception(
                                "/crm/v2/"
                                + stream_metadata["module_name"]
                                + " "
                                + json.dumps(record)
                            )
                            raise
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
