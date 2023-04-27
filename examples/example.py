import datetime
import re

import ora2pgs as o2p

# -----------------------------------------------

MY_PATH = 'C:/Temp/o2p/'

# -----------------------------------------------


def _get_sequence_column(trigger_name, x):
    """
    Gets the column to attach the sequence to
    """

    query = ' '.join([
        "SELECT us.text",
        "  FROM user_source us",
        f"WHERE us.name = '{trigger_name}'",
    ])

    data = x.execute_query_list(query)

    for d in data:
        columns = re.findall(r'NEW\.(.*):=', d.replace(' ', '').upper())
        if len(columns) == 1:
            return columns[0]

    return None


# -----------------------------------------------


def _get_sequences(x):
    """
    Gets a dict of sequence names to export
    """

    query = ' '.join([
        "SELECT ut.table_name, us.sequence_name, ut.trigger_name",
        "  FROM user_triggers ut,",
        "       user_dependencies ud,",
        "       user_sequences us",
        " WHERE ut.trigger_name LIKE '%SEQTRG'",
        "   AND ut.trigger_name = ud.name",
        "   AND ud.referenced_type = 'SEQUENCE'",
        "   AND ud.referenced_name = us.sequence_name"
    ])

    data = x.execute_query_list(query, None)

    for i, d in enumerate(data):
        data[i] = [*d, _get_sequence_column(d[2], x)]

    table_names = x.parameters[o2p.TABLES]

    table_sequences = {
        d[0]: {'sequence_name': d[1], 'column_name': d[3]}
        for d in data if d[0] in table_names and d[3]
    }

    return table_sequences


# -----------------------------------------------


def _process_mod_trigger(table_name, trigger_name, x):
    """ Builds a default mod trigger """

    if table_name in x.get_parameter(o2p.TABLE_NAMES):

        cmd = '\n'.join([
            f'CREATE TRIGGER {trigger_name}',
            f'BEFORE INSERT OR UPDATE ON {table_name}',
            'FOR EACH ROW',
            'EXECUTE PROCEDURE mod_trigger_fnc(); \n\n'
        ])

        file = x.open_file(x.get_filename(f'trigger_name.sql'), 'w')
        x.execute_command(file=file, cmd=cmd)
        x.close_file(file)


# -----------------------------------------------


def go():
    """ Go go go """

    now = datetime.datetime.now().strftime('%Y-%m-%d#%H-%M-%S')

    parameters = {
        o2p.ETL_DATA: True,
        o2p.ETL_MIGRATE: True,
        o2p.ETL_FILES: True,
        o2p.ORACLE_INSTANT_CLIENT: "C:/oracle/instantclient_12_1",
        o2p.ORACLE_CONN: "samaris/beast#42@194.129.151.10:1521/neo_pdb.sameacock.com",
        o2p.POSTGRES_CONN: "dbname='db_samaris' user='samaris_admin' host='localhost' password='sam88admin' port='5432'",
        o2p.POSTGRES_SCHEMA: 'samaris',
        o2p.TABLESPACE_MAP: {'INDEX': 'ts_samaris_indexes', 'TABLE': 'ts_samaris_tables'},
        o2p.TARGET_PATH: f'{MY_PATH}/exports/{now}'
    }

    x = o2p.O2P(**parameters)
    x.set_pls2pgs(x.file_read_json_file(f'{MY_PATH}/pls2pgs.json'))

    tables_sql = x.file_read_to_string(f'{MY_PATH}/tables.sql')
    tables = x.execute_query_list(tables_sql)
    x.set_parameter(o2p.TABLES, tables)

    sequences = _get_sequences(x)
    x.set_parameter(o2p.SEQUENCES, sequences)

    trigger_names_sql = x.file_read_to_string(f'{MY_PATH}/trigger_names.sql')
    trigger_names = x.execute_query_list(trigger_names_sql)
    x.set_parameter(o2p.TRIGGERS, trigger_names)

    exclude = x.file_read_to_string(f'{MY_PATH}/exclude.txt').split('\n')
    exclude_columns_sql = x.file_read_to_string(f'{MY_PATH}/exclude_columns.sql')
    exclude.extend(x.execute_query_list(exclude_columns_sql))
    x.set_parameter(o2p.EXCLUDE, exclude)

    # ---

    x.initialise_postgres_schema()
    x.execute_command_files(f'{MY_PATH}/prebuild')
    x.do_etl()
    x.execute_command_files(f'{MY_PATH}/postbuild')

    # ---

    mod_triggers_sql = x.file_read_to_string(f'{MY_PATH}/mod_triggers.sql')
    mod_triggers = x.execute_query_list(mod_triggers_sql)
    for i in mod_triggers:
        _process_mod_trigger(*i.split('.'), x)

    # ---

    x.execute_command_files(f'{MY_PATH}/postdev')

    print('here')


go()
