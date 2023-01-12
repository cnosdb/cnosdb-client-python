from base64 import b64encode

import json
import logging
from urllib.request import urlopen, Request
from urllib.error import HTTPError as UrlLibHTTPError

from .error import HTTPError, DatabaseError

error_msgs = {
    400: "parameter error",
    401: "authorization error",
    404: "api not found",
    500: "internal error",
    503: "system resources is not sufficient. It is may be caused by a huge query."
}


class Client:
    def __init__(self, url: str = "http://localhost:31007/",
                 database: str = "public",
                 user: str = "root",
                 password: str = ""):
        self._url = url
        self._database = database
        self._user = user
        self._password = password

    def sql(self, q: str) -> dict:
        _url = self._url + f"api/v1/sql?db={self._database}&pretty=true"
        request = self.build_request(url=_url, data=q)
        try:
            response = urlopen(request)
        except UrlLibHTTPError as e:
            logging.error(f"Invalid syntax or Object not exists, get ERROR from database : {e}")
            return {}
        else:
            self._check_status(response)
            resp = response.read().decode('utf-8')
            if resp == "":
                return {}
            resp = json.loads(resp)
            return resp

    def line_protocol(self, lines: [str]):
        _url = self._url + f"api/v1/write?db={self._database}&pretty=true"
        _data = ""
        for line in lines:
            _data += line + "\n"
        request = self.build_request(url=_url, data=_data)
        response = urlopen(request)
        self._check_status(response)

    def set_database(self, database_name):
        self._database = database_name

    def set_user(self, user, password):
        self._user = user
        self._password = password

    def set_url(self, url):
        self._url = url

    def _check_status(self, response):
        status = response.status
        if status != 200:
            msg = error_msgs.get(status)
            raise HTTPError(status, msg)

    def build_request(self, url, data):
        _basic = basic_auth(self._user, self._password)
        request = Request(url=url, data=data.encode("UTF-8"))
        request.add_header("Authorization", "Basic " + _basic)
        request.add_header("Accept", "application/json")
        return request


def basic_auth(name, pwd):
    return b64encode((name + ":" + pwd).encode(encoding="UTF-8")).decode(encoding="UTF-8")
