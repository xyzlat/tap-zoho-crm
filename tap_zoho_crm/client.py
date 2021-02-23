import os
import requests
from requests.exceptions import ConnectionError, Timeout, HTTPError
import datetime
import backoff

try:
    import singer
    logger = singer.get_logger()
except:
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()


AUTH_URL = "https://accounts.zoho.eu/oauth/v2/token"
REDIRECT_URI = "https://app.dreamdata.io/oauth2/pipedrive/callback"
API_PATH = "/crm/v2/"
DEFAULT_PER_PAGE = 200


class WaitAndRetry(Exception):
    pass


class ZohoClient:
    client_id: str = None
    client_secret: str = None
    access_token: str = None
    refresh_token: str = None
    expires_in: datetime.datetime = None
    api_domain: str = None

    def __init__(self, **creds):
        self._session = requests.session()
        self._set_creds(creds)
        if self.refresh_token:
            self.request_refresh_token()

    def _set_creds(self, creds):
        self.client_id = creds.get("client_id", self.client_id)
        self.client_secret = creds.get("client_secret", self.client_secret)
        self.access_token = creds.get("access_token", self.access_token)
        self.refresh_token = creds.get("refresh_token", self.refresh_token)
        self.expires_in = creds.get("expires_in", self.expires_in)
        self.api_domain = creds.get("api_domain", self.api_domain)

    def request_creds_from_code(self, code):
        creds = self._session.post(
            AUTH_URL,
            params={
                "grant_type": "authorization_code",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "redirect_uri": REDIRECT_URI,
                "code": code,
                "access_type": "offline",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        creds.raise_for_status()
        self._set_creds(creds.json())

    def request_refresh_token(self):
        creds = self._session.post(
            AUTH_URL,
            params={
                "grant_type": "refresh_token",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        creds.raise_for_status()
        self._set_creds(creds.json())

    @backoff.on_exception(
        backoff.expo,
        (Timeout, ConnectionError, WaitAndRetry, HTTPError),
        max_tries=5,
        factor=2,
    )
    def make_request(self, zoho_module, **params):
        modified_since = params.pop("modified_since", None)
        url = f"{self.api_domain}{API_PATH}{zoho_module}"
        headers = {"Authorization": f"Zoho-oauthtoken {self.access_token}"}
        if modified_since:
            headers["If-Modified-Since"] = modified_since
        response = self._session.get(url, params=params, headers=headers)

        if response.status_code == 304:
            logger.warning(
                f"{zoho_module} has no new material after {modified_since}")
            return None
        if response.status_code == 429:
            logger.warning("got rate limited, waiting a bit")
        elif response.status_code == 500:
            logger.warning(
                "got internal server error from zoho, waiting a bit")
        elif response.status_code in [400, 401, 403]:
            logger.warning(
                "got possible bad auth, refreshing tokens and trying again")
            self.request_refresh_token()
        else:
            response.raise_for_status()
            return response.json()

        raise WaitAndRetry()

    def paginate_generator(self, zoho_module, **params):
        more_records = True
        per_page = params.pop("per_page", DEFAULT_PER_PAGE)
        page = params.pop("page", 1)
        while more_records:
            logger.info(f"Paginating through {zoho_module}, page={page}")
            response = self.make_request(
                zoho_module, per_page=per_page, page=page, **params
            )
            if response is None:
                return
            for record in response["data"]:
                yield record
            pagination_info = response["info"]
            more_records = pagination_info["more_records"]
            per_page = pagination_info.get("per_page", per_page)
            page = pagination_info.get("page", page) + 1
