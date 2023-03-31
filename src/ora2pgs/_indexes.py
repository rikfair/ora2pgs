#!/usr/bin/python3
# -----------------------------------------------
"""
    DESCRIPTION:
        Indexes module

    ASSUMPTIONS:
        No assumptions to note

    ACCURACY:
        No accuracy issues to note
"""
# -----------------------------------------------

import ora2pgs as o2p

# -----------------------------------------------


def _process_create_indexes(table_name: str, base, file):
    """
    Builds the indexes
    """

    schema = base.parameters[o2p.POSTGRES_SCHEMA]

    # ---

    def add_index(idx, cols, tbs):
        """ Executes the create index statement """
        if idx and base.include_object(o2p.INDEXES, idx):
            tablespace_name = base.tablespace_map('INDEX', tbs)
            base.execute_command(
                file=file,
                cmd=f"CREATE INDEX {idx} ON {schema}.{table_name} ({','.join(cols)}) TABLESPACE {tablespace_name};"
            )
        return

    # ---

    query = ' '.join([
        "SELECT ui.index_name, uic.column_name, ui.tablespace_name",
        "  FROM user_indexes ui, user_ind_columns uic",
        " WHERE ui.uniqueness = 'NONUNIQUE'"
        "   AND ui.index_type = 'NORMAL'"
        f"  AND ui.table_name = '{table_name}'"
        "   AND ui.index_name = uic.index_name"       
        " ORDER BY ui.index_name, uic.column_position"
    ])

    dd, cd = base.execute_query(query)

    columns = list()
    index_name = ''
    source_tbs = ''

    for d in dd:
        if index_name != d[cd['index_name']]:
            add_index(index_name, columns, source_tbs)
            index_name = d[cd['index_name']]
            source_tbs = d[cd['tablespace_name']]
            columns = []
        columns.append(d[cd['column_name']])

    add_index(index_name, columns, source_tbs)


# -----------------------------------------------


def etl(table_name: str, base):
    """
    Creates a set of sql files for creating tables in postgresql
    """

    base.log(f'Running... {table_name} Indexes')

    if base.parameters[o2p.ETL_FILES]:
        file_name = f'{base.parameters[o2p.TARGET_PATH]}/etl/{table_name}.3.sql'
        file = open(file_name, 'a', encoding=base.parameters[o2p.ENCODING])
        file.write(f'\n\\echo Running... {table_name} Indexes \n\n')
    else:
        file = None

    _process_create_indexes(table_name, base, file)

    if file:
        file.close()


# -----------------------------------------------
# End.
