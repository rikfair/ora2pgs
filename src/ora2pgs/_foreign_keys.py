#!/usr/bin/python3
# -----------------------------------------------
"""
    DESCRIPTION:
        Foreign key module

    ASSUMPTIONS:
        No assumptions to note

    ACCURACY:
        No accuracy issues to note
"""
# -----------------------------------------------

from ora2pgs import o2p

# -----------------------------------------------


def _process_create_fks(table_name: str, base, file):
    """
    Builds the primary and unique key constraint statements
    """

    def add_fk_constraint(cn, tcols, rtab, rcols):
        """ Adds a foreign key constraint """

        if cn and base.include_object(o2p.CONSTRAINTS, cn) and rtab in base.parameters[o2p.TABLE_NAMES]:
            base.execute_command(
                file=file,
                cmd=(
                    f"ALTER TABLE {schema}.{table_name} ADD CONSTRAINT {cn} FOREIGN KEY "
                    + f"({','.join(tcols)}) REFERENCES {rtab} "
                    + f"({','.join(rcols)});"
                )
            )

    # ---

    schema = base.parameters[o2p.POSTGRES_SCHEMA]

    query = ' '.join([
        'SELECT uc.constraint_name, ruc.table_name r_table_name, ucc.column_name, rucc.column_name r_column_name',
        '  FROM user_constraints uc, user_cons_columns ucc, user_constraints ruc, user_cons_columns rucc',
        ' WHERE uc.constraint_name = ucc.constraint_name',
        '   AND uc.r_constraint_name = ruc.constraint_name',
        '   AND ruc.constraint_name = rucc.constraint_name',
        '   AND ucc.position = rucc.position',
        f"  AND uc.table_name = '{table_name}'",
        ' ORDER BY uc.constraint_name, ucc.position'
    ])

    dd, cd = base.execute_query(query)

    tcolumns = list()
    rcolumns = list()
    constraint_name = ''
    rtable_name = ''

    for con in dd:
        if constraint_name != con[cd['constraint_name']]:
            add_fk_constraint(constraint_name, tcolumns, rtable_name, rcolumns)
            constraint_name = con[cd['constraint_name']]
            rtable_name = con[cd['r_table_name']]
            tcolumns = list()
            rcolumns = list()
        tcolumns.append(con[cd['column_name']])
        rcolumns.append(con[cd['r_column_name']])

    add_fk_constraint(constraint_name, tcolumns, rtable_name, rcolumns)


# -----------------------------------------------


def etl(table_name: str, base):
    """
    Creates a set of sql files for creating tables in postgresql
    """

    base.log(f'Running... {table_name} Foreign Keys')

    if base.parameters[o2p.ETL_FILES]:
        file_name = f'{base.parameters[o2p.TARGET_PATH]}/etl/{table_name}.3.sql'
        file = open(file_name, 'w', encoding=base.parameters[o2p.ENCODING])  # Assuming first "3" entry
        file.write(f'\n\\echo Running... {table_name} Foreign Keys\n\n')
    else:
        file = None

    _process_create_fks(table_name, base, file)

    if file:
        file.close()


# -----------------------------------------------
# End.
