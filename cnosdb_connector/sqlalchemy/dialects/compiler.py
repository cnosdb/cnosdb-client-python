from sqlalchemy.sql import compiler


class CnosDBTypeCompiler(compiler.GenericTypeCompiler):
    """Originally forked from pyhive"""

    def visit_INTEGER(self, type_):
        return "BIGINT"

    def visit_NUMERIC(self, type_):
        return "DOUBLE"

    def visit_CHAR(self, type_):
        return "STRING"

    def visit_VARCHAR(self, type_):
        return "STRING"

    def visit_NCHAR(self, type_):
        return "STRING"

    def visit_TEXT(self, type_):
        return "STRING"

    def visit_CLOB(self, type_):
        return "STRING"

    def visit_BLOB(self, type_):
        return "STRING"

    def visit_TIME(self, type_):
        return "TIMESTAMP"

    def visit_DATE(self, type_):
        return "TIMESTAMP"

    def visit_DATETIME(self, type_):
        return "TIMESTAMP"

