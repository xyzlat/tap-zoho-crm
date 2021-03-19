PAGINATE_MODULES = {
    "Approvals": {
        "stream_name": "approvals",
        "module_name": "Approvals",
        "params": {"per_page": 200},
    },
    "Leads": {
        "stream_name": "leads",
        "module_name": "Leads",
        "params": {"per_page": 200, "sort_by": "Modified_Time", "sort_order": "asc"},
        "bookmark_key": "Modified_Time",
    },
    "Deals": {
        "stream_name": "deals",
        "module_name": "Deals",
        "params": {"per_page": 200, "sort_by": "Modified_Time", "sort_order": "asc"},
        "bookmark_key": "Modified_Time",
    },
    "Contacts": {
        "stream_name": "contacts",
        "module_name": "Contacts",
        "params": {"per_page": 200, "sort_by": "Modified_Time", "sort_order": "asc"},
        "bookmark_key": "Modified_Time",
    },
    "Accounts": {
        "stream_name": "accounts",
        "module_name": "Accounts",
        "params": {"per_page": 200, "sort_by": "Modified_Time", "sort_order": "asc"},
        "bookmark_key": "Modified_Time",
    },
    "Tasks": {
        "stream_name": "tasks",
        "module_name": "Tasks",
        "params": {"per_page": 200, "sort_by": "Modified_Time", "sort_order": "asc"},
        "bookmark_key": "Modified_Time",
    },
    "Events": {
        "stream_name": "events",
        "module_name": "Events",
        "params": {"per_page": 200, "sort_by": "Modified_Time", "sort_order": "asc"},
        "bookmark_key": "Modified_Time",
    },
    "Calls": {
        "stream_name": "calls",
        "module_name": "Calls",
        "params": {"per_page": 200, "sort_by": "Modified_Time", "sort_order": "asc"},
        "bookmark_key": "Modified_Time",
    },
    "Activities": {
        "stream_name": "activities",
        "module_name": "Activities",
        "params": {"per_page": 200, "sort_by": "Modified_Time", "sort_order": "asc"},
        "bookmark_key": "Modified_Time",
    },
    "Visits": {
        "stream_name": "visits",
        "module_name": "Visits",
        "params": {"per_page": 200, "sort_by": "Modified_Time", "sort_order": "asc"},
        "bookmark_key": "Modified_Time",
    },
    "Invoices": {
        "stream_name": "invoices",
        "module_name": "Invoices",
        "params": {"per_page": 200, "sort_by": "Modified_Time", "sort_order": "asc"},
        "bookmark_key": "Modified_Time",
    },
    "Notes": {
        "stream_name": "notes",
        "module_name": "Notes",
        "params": {"per_page": 200, "sort_by": "Modified_Time", "sort_order": "asc"},
        "bookmark_key": "Modified_Time",
    },
    "Attachments": {
        "stream_name": "attachments",
        "module_name": "Attachments",
        "params": {"per_page": 200, "sort_by": "Modified_Time", "sort_order": "asc"},
        "bookmark_key": "Modified_Time",
    },
    "Lead_Status_History": {
        "stream_name": "lead_status_history",
        "module_name": "Lead_Status_History",
        "params": {"per_page": 200, "sort_by": "Modified_Time", "sort_order": "asc"},
        "bookmark_key": "Modified_Time",
    },
}

NON_PAGINATE_MODULES = {
    "org": {
        "module_name": "org",
        "stream_name": "org_settings",
    },
    "settings/stages": {
        "module_name": "settings/stages",
        "stream_name": "settings_stages",
        "params": {"module": "Deals"},
    },
}

KNOWN_SUBMODULES = {
    "Deals": [
        {
            "module_name": "Stage_History",
            "stream_name": "deals_stage_history",
            "bookmark_key": "Last_Modified_Time",
        }
    ]
}
