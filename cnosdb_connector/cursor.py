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
        self._response = None
        self._index = -1
        self._response = self._c.sql(operation)
        self._rowcount = len(self._response)
        description = []
        for row in self._response:
            for column in row:
                description.append([column, type(row[column]).__name__])
            break
        self._description = description

    def executemany(self, operation, parameters=None):
        self.execute(operation)

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
        return self._response[self._index]

    def fetchmany(self):
        return self.fetchone()

    def fetchall(self):
        if self._response is None:
            raise OperationalError("no result to fetch")
        start_index = self._index + 1
        self._index = len(self._response)
        return self._response[start_index:]

    def nextset(self):
        raise NotSupportedError()

    def setinputsizes(self):
        raise NotSupportedError()

    def setoutputsize(self, size, column=None):
        raise NotSupportedError()
