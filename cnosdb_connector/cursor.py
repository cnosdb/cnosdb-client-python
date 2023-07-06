import datetime
import decimal
from collections.abc import Iterable
from typing import Dict


from .error import *
from .client import Client


# PEP249 cursor
class Cursor:

    def __init__(self, client: Client):
        self._c = client
        self.arraysize = 1
        self._rowcount = -1
        self._response = None
        self._index = -1
        self._description = None
        self.escaper = ParamEscaper()

    @property
    def rowcount(self):
        return self._rowcount

    @property
    def description(self):
        """
        :return: each item is a list contain two str item
                which is column name and column type
        """
        if self._response is None:
            return None
        return self._description

    def callproc(self, procname, parameters=None):
        raise NotSupportedError()

    def close(self):
        pass

    def execute(self, operation, parameters=None):
        if parameters is not None:
            operation = inject_parameters(
                operation, self.escaper.escape_args(parameters)
            )

        self._response = []
        self._index = -1

        resp = self._c.sql(operation)
        if type(resp).__name__ == "dict":
            self._response.append(resp)
        else:
            self._response.extend(resp)
        self._rowcount = len(self._response)
        description = []
        for row in self._response:
            for column in row:
                description.append([column, type(row[column]).__name__])
            break
        self._description = description

    def executemany(self, operation, parameters=None):
        self.execute(operation, parameters)

    def get_type(self, col):
        if not self._description:
            return None
        return self._description[col][1]

    def fetchone(self):
        if self._response is None:
            raise OperationalError("no result to fetch")
        self._index += 1
        if self._index + 1 > len(self._response):
            return None
        return list(self._response[self._index].values())

    def fetchmany(self):
        return self.fetchone()

    def fetchall(self):
        if self._response is None:
            raise OperationalError("no result to fetch")
        start_index = self._index + 1
        self._index = len(self._response)
        return [list(row.values()) for row in self._response[start_index:]]

    def nextset(self):
        raise NotSupportedError()

    def setinputsizes(self):
        raise NotSupportedError()

    def setoutputsize(self, size, column=None):
        raise NotSupportedError()


# Taken from PyHive
class ParamEscaper:
    _DATE_FORMAT = "%Y-%m-%d"
    _TIME_FORMAT = "%H:%M:%S.%f"
    _DATETIME_FORMAT = "{} {}".format(_DATE_FORMAT, _TIME_FORMAT)

    def escape_args(self, parameters):
        if isinstance(parameters, dict):
            return {k: self.escape_item(v) for k, v in parameters.items()}
        elif isinstance(parameters, (list, tuple)):
            return tuple(self.escape_item(x) for x in parameters)
        else:
            raise ProgrammingError(
                "Unsupported param format: {}".format(parameters)
            )

    def escape_number(self, item):
        return item

    def escape_string(self, item):
        # Need to decode UTF-8 because of old sqlalchemy.
        # Newer SQLAlchemy checks dialect.supports_unicode_binds before encoding Unicode strings
        # as byte strings. The old version always encodes Unicode as byte strings, which breaks
        # string formatting here.
        if isinstance(item, bytes):
            item = item.decode("utf-8")
        # This is good enough when backslashes are literal, newlines are just followed, and the way
        # to escape a single quote is to put two single quotes.
        # (i.e. only special character is single quote)
        return "'{}'".format(item.replace("\\", "\\\\").replace("'", "\\'"))

    def escape_sequence(self, item):
        l = map(str, map(self.escape_item, item))
        return "(" + ",".join(l) + ")"

    def escape_datetime(self, item, format, cutoff=0):
        dt_str = item.strftime(format)
        formatted = dt_str[:-cutoff] if cutoff and format.endswith(".%f") else dt_str
        return "'{}'".format(formatted)

    def escape_decimal(self, item):
        return str(item)

    def escape_item(self, item):
        if item is None:
            return "NULL"
        elif isinstance(item, (int, float)):
            return self.escape_number(item)
        elif isinstance(item, str):
            return self.escape_string(item)
        elif isinstance(item, Iterable):
            return self.escape_sequence(item)
        elif isinstance(item, datetime.datetime):
            return self.escape_datetime(item, self._DATETIME_FORMAT)
        elif isinstance(item, datetime.date):
            return self.escape_datetime(item, self._DATE_FORMAT)
        elif isinstance(item, decimal.Decimal):
            return self.escape_decimal(item)
        else:
            raise ProgrammingError("Unsupported object {}".format(item))


def inject_parameters(operation: str, parameters: Dict[str, str]):
    return operation % parameters
