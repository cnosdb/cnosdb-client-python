"""This module's layout loosely follows example of SQLAlchemy's postgres dialects
"""
from sqlalchemy import types, event
from sqlalchemy.engine import default, Engine
from sqlalchemy.exc import DatabaseError
from sqlalchemy.engine import reflection

from cnosdb_connector.sqlalchemy.dialects.base import (
    CnosDBDDLCompiler,
    CnosDBIdentifierPreparer,
)
import cnosdb_connector
from cnosdb_connector.sqlalchemy.dialects.compiler import CnosDBTypeCompiler

try:
    import alembic
except ImportError:
    pass
else:
    from alembic.ddl import DefaultImpl


    class CnosDBImpl(DefaultImpl):
        __dialect__ = "cnosdb"


class CnosDBDialect(default.DefaultDialect):
    """This dialects implements only those methods required to pass our e2e tests"""

    # Possible attributes are defined here: https://docs.sqlalchemy.org/en/14/core/internals.html#sqlalchemy.engine.Dialect
    name: str = "cnosdb"
    driver: str = "cnosdb-connector"
    default_schema_name: str = "default"

    preparer = CnosDBIdentifierPreparer
    type_compiler = CnosDBTypeCompiler
    ddl_compiler = CnosDBDDLCompiler
    supports_statement_cache: bool = True
    supports_multivalues_insert: bool = True
    supports_native_decimal: bool = True
    supports_sane_rowcount: bool = False

    @classmethod
    def dbapi(cls):
        return cnosdb_connector

    def create_connect_args(self, url):
        kwargs = {
            "url": "http://" + url.host + ":" + str(url.port) + "/",
            "user": url.username,
            "password": url.password,
            "tenant": url.query.get("tenant"),
            "database": url.query.get("db"),
        }

        self.schema = kwargs["database"]
        self.catalog = kwargs["tenant"]

        return [], kwargs

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        """Return information about columns in `table_name`.

        Given a :class:`_engine.Connection`, a string
        `table_name`, and an optional string `schema`, return column
        information as a list of dictionaries with these keys:

        name
          the column's name

        type
          [sqlalchemy.types#TypeEngine]

        nullable
          boolean

        default
          the column's default value

        autoincrement
          boolean

        sequence
          a dictionary of the form
              {'name' : str, 'start' :int, 'increment': int, 'minvalue': int,
               'maxvalue': int, 'nominvalue': bool, 'nomaxvalue': bool,
               'cycle': bool, 'cache': int, 'order': bool}

        Additional column attributes may be present.
        """

        _type_map = {
            "BIGINT": types.BigInteger,
            "BIGINT UNSIGNED": types.BigInteger,
            "DOUBLE": types.Float,
            "STRING": types.String,
            "BOOLEAN": types.BOOLEAN,
            "TIMESTAMP(NANOSECOND)": types.TIMESTAMP,
            "TIMESTAMP(MICROSECOND)": types.TIMESTAMP,
            "TIMESTAMP(MILLISECOND)": types.TIMESTAMP,
        }

        cur = self.get_driver_connection(
            connection
        )._dbapi_connection.dbapi_connection.cursor()

        cur.execute(f"DESCRIBE TABLE {table_name}")
        resp = cur.fetchall()
        columns = []

        for col in resp:
            this_column = {
                "name": col[0],
                "col_type": col[1],
                "compression_codec": col[2],
                "type": _type_map[col[3]],
            }
            columns.append(this_column)
        columns = sorted(columns, key=lambda x: x["name"])

        return columns

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """Return information about the primary key constraint on
        table_name`.

        Given a :class:`_engine.Connection`, a string
        `table_name`, and an optional string `schema`, return primary
        key information as a dictionary with these keys:

        constrained_columns
          a list of column names that make up the primary key

        name
          optional name of the primary key constraint.

        """
        # TODO: implement this behaviour
        return {"constrained_columns": []}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """Return information about foreign_keys in `table_name`.

        Given a :class:`_engine.Connection`, a string
        `table_name`, and an optional string `schema`, return foreign
        key information as a list of dicts with these keys:

        name
          the constraint's name

        constrained_columns
          a list of column names that make up the foreign key

        referred_schema
          the name of the referred schema

        referred_table
          the name of the referred table

        referred_columns
          a list of column names in the referred table that correspond to
          constrained_columns
        """
        # TODO: Implement this behaviour
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        """Return information about indexes in `table_name`.

        Given a :class:`_engine.Connection`, a string
        `table_name` and an optional string `schema`, return index
        information as a list of dictionaries with these keys:

        name
          the index's name

        column_names
          list of column names in order

        unique
          boolean
        """
        # TODO: Implement this behaviour
        return []

    def get_table_names(self, connection, schema=None, **kwargs):
        cur = self.get_driver_connection(
            connection
        )._dbapi_connection.dbapi_connection.cursor()
        sql_str = "SHOW TABLES"
        cur.execute(sql_str)
        data = cur.fetchall()
        _tables = []
        for dict in data:
            _tables += dict

        return _tables

    def get_view_names(self, connection, schema=None, **kwargs):

        return []

    def do_rollback(self, dbapi_connection):
        # CnosDB SQL Does not support transactions
        pass

    def has_table(self, connection, table_name, schema=None, **kwargs) -> bool:
        """SQLAlchemy docstrings say dialects providers must implement this method"""

        schema = schema or "default"

        # DBR >12.x uses underscores in error messages
        DBR_LTE_12_NOT_FOUND_STRING = "Table or not found"
        DBR_GT_12_NOT_FOUND_STRING = "TABLE_NOT_FOUND"

        try:
            res = connection.execute(f"DESCRIBE TABLE {table_name}")
            return True
        except DatabaseError as e:
            if DBR_GT_12_NOT_FOUND_STRING in str(
                    e
            ) or DBR_LTE_12_NOT_FOUND_STRING in str(e):
                return False
            else:
                raise e

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        # Equivalent to SHOW DATABASES

        # TODO: replace with call to cursor.schemas() once its performance matches raw SQL
        return [row[0] for row in connection.execute("SHOW DATABASES")]


@event.listens_for(Engine, "do_connect")
def receive_do_connect(dialect, conn_rec, cargs, cparams):
    """Helpful for DS on traffic from clients using SQLAlchemy in particular"""

    # Ignore connect invocations that don't use our dialects
    if not dialect.name == "cnosdb":
        return

    if "_user_agent_entry" in cparams:
        new_user_agent = f"sqlalchemy + {cparams['_user_agent_entry']}"
    else:
        new_user_agent = "sqlalchemy"

    cparams["_user_agent_entry"] = new_user_agent
