#!/usr/bin/python3
# -----------------------------------------------
"""
    DESCRIPTION:
        Constants for use in the package

    ASSUMPTIONS:
        TODO

    LIMITATIONS:
        TODO.
"""
# -----------------------------------------------
#  Parameters

COLUMNS = 'columns'
CONSOLE = 'console'
CONSTRAINTS = 'constraints'
ENCODING = 'encoding'
ETL_DATA = 'etl_data'
ETL_FILES = 'etl_files'
ETL_MIGRATE = 'etl_migrate'
EXCLUDE = 'exclude'
INDEXES = 'indexes'
INSERT_ROWS = 'insert_rows'
ORACLE_CONN = 'oracle_conn'
ORACLE_INSTANT_CLIENT = 'oracle_instant_client'
POSTGRES_CONN = 'postgres_conn'
POSTGRES_SCHEMA = 'postgres_schema'
QUOTE_COLUMN_NAMES = 'quote_column_names'
SEQUENCES = 'sequences'
TABLES = 'tables'
TABLE_NAMES = 'table_names'  # System only.
TABLESPACE_MAP = 'tablespace_map'
TARGET_PATH = 'target_path'
TRIGGERS = 'triggers'

# ---

PRE = 'pre'
ETL = 'etl'
POST = 'post'

# ---
#  plsql to psql replacements

DEFAULT_PARAMETERS = {
    CONSOLE: False,
    CONSTRAINTS: True,
    ENCODING: 'utf-8-sig',
    ETL_DATA: True,
    ETL_FILES: True,
    ETL_MIGRATE: True,
    INDEXES: True,
    INSERT_ROWS: 2500,
    QUOTE_COLUMN_NAMES: ['LIMIT', 'NATURAL'],
    POSTGRES_SCHEMA: 'O2P',
    TABLES: True,
    TRIGGERS: False
}

# ---
#  plsql to psql replacements

DEFAULT_PLS2PGS = {
    "\t": "  ",
    ":OLD.": "OLD.",
    ":NEW.": "NEW.",
    "INSERTING": "TG_OP = 'INSERT'",
    "UPDATING": "TG_OP = 'UPDATE'",
    "DELETING": "TG_OP = 'DELETE'",
    " NUMBER": " NUMERIC",
    " PLS_INTEGER": " INTEGER",
    "SQL%ROWCOUNT": "sql_rowcount()"
}

# ---
#  object types

OBJECT_TYPES = {
    CONSTRAINTS: 'CONSTRAINT',
    INDEXES: 'INDEX',
    TRIGGERS: 'TRIGGER'
}

# -----------------------------------------------
# End.
