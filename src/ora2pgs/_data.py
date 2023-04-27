#!/usr/bin/python3
# -----------------------------------------------
"""
    DESCRIPTION:
        Insert statement module

    ASSUMPTIONS:
        No assumptions to note

    ACCURACY:
        No accuracy issues to note
"""
# -----------------------------------------------

import datetime
import cx_Oracle

from ora2pgs import o2p

# -----------------------------------------------


def _insert_rows(table_name, columns, file, rows, base):
    """ Build the insert statement when the number of rows has been reached """

    rs = ',\n'.join(rows)
    cmd = f"INSERT INTO {base.parameters[o2p.POSTGRES_SCHEMA]}.{table_name} ({columns}) VALUES \n{rs}; \n"
    cmd = cmd.replace('\x00', '')
    base.execute_command(file=file, cmd=cmd)


# -----------------------------------------------


def _process_data(table_name, table_columns, base, file):
    """ Processes export for a specified table """

    insert_rows = base.parameters[o2p.INSERT_ROWS]

    columns = ','.join(table_columns)
    query = 'SELECT ' + columns.replace('"', '') + ' FROM ' + table_name
    cursor = base.get_oracle_connection().cursor()
    cursor.execute(query)

    while True:
        rows = cursor.fetchmany(insert_rows)
        if not rows:
            break
        rows_to_insert = []

        for row in rows:
            cols = []
            for col in row:
                if col is None:
                    cols.append('NULL')
                elif isinstance(col, (str, cx_Oracle.LOB)):
                    cols.append("'" + str(col).replace("'", "''") + "'")
                elif isinstance(col, datetime.datetime):
                    cols.append(f"TO_TIMESTAMP('{col.strftime('%Y%m%d%H%M%S')}','YYYYMMDDHH24MISS')")
                else:
                    cols.append(str(col))
            rows_to_insert.append('(' + ','.join(cols) + ')')

        _insert_rows(table_name, columns, file, rows_to_insert, base)


# -----------------------------------------------


def etl(table_name, table_columns, base):
    """ Main data export function """

    base.log(f'Running... {table_name} Data')

    if base.parameters[o2p.ETL_FILES]:
        file_name = f'{base.parameters[o2p.TARGET_PATH]}/etl/{table_name}.2.sql'
        file = open(file_name, 'w', encoding=base.parameters[o2p.ENCODING])
        file.write(f'\\echo Running... {table_name} Data\n\n')
    else:
        file = None

    _process_data(table_name, table_columns, base, file)

    if file:
        file.close()


# -----------------------------------------------
# End.
