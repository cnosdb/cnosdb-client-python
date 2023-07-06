import re
from sqlalchemy.sql import compiler


class CnosDBIdentifierPreparer(compiler.IdentifierPreparer):
    legal_characters = re.compile(r"^[A-Z0-9_]+$", re.I)

    def __init__(self, dialect):
        super().__init__(dialect, initial_quote="`")


class CnosDBDDLCompiler(compiler.DDLCompiler):
    def post_create_table(self, table):
        return ""
