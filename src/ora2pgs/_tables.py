#!/usr/bin/python3
# -----------------------------------------------
"""
    DESCRIPTION:
        Tables module

    ASSUMPTIONS:
        No assumptions to note

    ACCURACY:
        No accuracy issues to note
"""
# -----------------------------------------------

from ora2pgs import o2p

# -----------------------------------------------


def _process_create_comments_cols(table_name, base, file):
    """
    Builds the create column comment statements
    """

    schema = base.parameters[o2p.POSTGRES_SCHEMA]

    query = ' '.join([
        "SELECT column_name, REPLACE(comments, '''', '''''') comments",
        "  FROM user_col_comments",
        f"WHERE table_name = '{table_name}'"
    ])

    dd, cd = base.execute_query(query)

    # ---

    exc = base.parameters[o2p.EXCLUDE]
    qcn = base.parameters[o2p.QUOTE_COLUMN_NAMES]

    for d in dd:
        column_name = d[cd['column_name']]
        if f"COLUMN {table_name}.{column_name}" in exc:
            continue
        if column_name in qcn:
            column_name = '"' + column_name + '"'

        comments = d[cd['comments']]
        if comments:
            comments = comments.replace('\n', ' ')
            base.execute_command(
                file=file,
                cmd=f"COMMENT ON COLUMN {schema}.{table_name}.{column_name} IS '{comments}';"
            )


# -----------------------------------------------


def _process_create_comments_tab(table_name, base, file):
    """
    Builds the create table comment statement
    """

    query = ' '.join([
        "SELECT REPLACE(comments, '''', '''''') comments",
        "  FROM user_tab_comments",
        f"WHERE table_name = '{table_name}'"
    ])

    dd, cd = base.execute_query(query)

    # ---

    schema = base.parameters[o2p.POSTGRES_SCHEMA]

    for d in dd:
        comments = d[cd['comments']]
        if comments:
            comments = comments.replace('\n', ' ')
            base.execute_command(
                file=file,
                cmd=f"\nCOMMENT ON TABLE {schema}.{table_name} IS '{comments}';\n"
            )


# -----------------------------------------------


def _process_create_puks(table_name: str, base, file):
    """
    Builds the primary and unique key constraint statements
    """

    def add_puk_constraint(cn, ct, cols, tbs):
        """ Executes the statement to create the primary key """
        if cn and base.include_object(o2p.CONSTRAINTS, cn):
            base.execute_command(
                file=file,
                cmd=(
                    f"ALTER TABLE {base.parameters[o2p.POSTGRES_SCHEMA]}.{table_name} ADD CONSTRAINT {cn} "
                    + f"{'PRIMARY KEY' if ct == 'P' else 'UNIQUE'} ("
                    + ','.join(cols) + f") USING INDEX TABLESPACE {tbs};"
                )
            )

    # ---

    query = ' '.join([
        "SELECT uc.constraint_name, uc.constraint_type, ucc.column_name, ui.tablespace_name",
        "  FROM user_constraints uc, user_cons_columns ucc, user_indexes ui",
        " WHERE uc.constraint_type IN ('P','U')"
        f"  AND uc.table_name = '{table_name}'"
        "   AND uc.constraint_name = ucc.constraint_name"
        "   AND uc.index_name = ui.index_name"
        " ORDER BY uc.constraint_name, ucc.position"
    ])

    dd, cd = base.execute_query(query)

    columns = list()
    constraint_name = ''
    constraint_type = ''
    tablespace_name = ''

    for con in dd:
        if constraint_name != con[cd['constraint_name']]:
            add_puk_constraint(constraint_name, constraint_type, columns, tablespace_name)
            constraint_name = con[cd['constraint_name']]
            constraint_type = con[cd['constraint_type']]
            tablespace_name = base.tablespace_map('INDEX', con[cd['tablespace_name']])
            columns = list()
        columns.append(con[cd['column_name']])

    add_puk_constraint(constraint_name, constraint_type, columns, tablespace_name)


# -----------------------------------------------


def _process_create_table(table_name, sequence, table_columns, base, file):
    """
    Builds the create table statement
    """

    lines = list()

    # ---

    query = f"SELECT tablespace_name FROM user_tables WHERE table_name = '{table_name}'"
    data = base.execute_query_list(query)
    if not data:
        raise Exception(f'Table not found: "{table_name}"')
    tablespace_name = base.tablespace_map('TABLE', data[0])

    # ---

    query = ' '.join([
        f"SELECT column_name, data_type, data_length, data_precision, data_scale, nullable",
        "   FROM user_tab_columns",
        f" WHERE table_name = '{table_name}'",
        "  ORDER BY column_id"
    ])

    dd, cd = base.execute_query(query)

    # ---

    exc = base.parameters[o2p.EXCLUDE]
    qcn = base.parameters[o2p.QUOTE_COLUMN_NAMES]

    for column in dd:

        column_name = column[cd['column_name']]
        if f'COLUMN {table_name}.{column_name}' in exc:
            continue

        if column_name in qcn:
            column_name = '"' + column_name + '"'

        table_columns.append(column_name)
        line = f", {column_name}".ljust(35)

        if column[cd['data_type']] == 'NUMBER' and column_name.endswith('_ID'):
            line += "INTEGER"
        elif column[cd['data_type']] in ['CHAR', 'VARCHAR2']:
            line += f"VARCHAR({column[cd['data_length']]})"
        elif column[cd['data_type']] == 'NUMBER' and column[cd['data_precision']] and column[cd['data_scale']]:
            line += f"NUMERIC({column[cd['data_precision']]},{column[cd['data_scale']]})"
        elif column[cd['data_type']] == 'NUMBER' and column[cd['data_precision']]:
            if column[cd['data_precision']] < 5:
                line += "SMALLINT"
            elif column[cd['data_precision']] > 9:
                line += "BIGINT"
            else:
                line += "INTEGER"
            if not column_name.endswith('_ID') and column[cd['data_precision']] < 9:
                line += f" CHECK ({column_name} < {'1' + ('0' * column[cd['data_precision']])})"
        elif column[cd['data_type']] == 'NUMBER':
            line += f"NUMERIC"
        elif column[cd['data_type']] == 'DATE':
            line += "TIMESTAMP(0)"
        elif column[cd['data_type']] == 'CLOB':
            line += "TEXT"
        elif column[cd['data_type']] == 'BLOB':
            line += "BYTEA"
        else:
            base.log(f"Unknown column: {table_name}.{column_name}")
            line += f"UNKNOWN"

        if column[cd['nullable']] == 'N':
            line += ' NOT NULL'

        if sequence and column_name == sequence['column_name']:
            line += f" DEFAULT NEXTVAL('{sequence['sequence_name']}')"

        lines.append(line)

    # ---

    base.execute_command(
        file=file,
        cmd=(
            f"CREATE TABLE {base.parameters[o2p.POSTGRES_SCHEMA]}.{table_name.lower()} \n("
            + ('\n'.join(lines)).lstrip(',')
            + f"\n) TABLESPACE {tablespace_name}\n;\n"
        )
    )


# -----------------------------------------------


def etl(table_name, sequence, table_columns, base):
    """
    Creates a set of sql files for creating tables in postgresql
    """

    base.log(f'Running... {table_name} Tables')

    if base.parameters[o2p.ETL_FILES]:
        file_name = f'{base.parameters[o2p.TARGET_PATH]}/etl/{table_name}.1.sql'
        file = open(file_name, 'w', encoding=base.parameters[o2p.ENCODING])
        file.write(f'\\echo Running... {table_name} Tables \n\n')
    else:
        file = None

    _process_create_table(table_name, sequence, table_columns, base, file)
    _process_create_puks(table_name, base, file)
    _process_create_comments_tab(table_name, base, file)
    _process_create_comments_cols(table_name, base, file)

    if file:
        file.close()


# -----------------------------------------------
# End.
