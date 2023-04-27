"""
The extract transform load manager module for migrating an oracle schema to postgresql
"""
# -----------------------------------------------

import datetime
import json
import logging
import os

import cx_Oracle
import psycopg2

from ora2pgs import o2p
import ora2pgs._data as data
import ora2pgs._foreign_keys as foreign_keys
import ora2pgs._indexes as indexes
import ora2pgs._sequences as sequences
import ora2pgs._tables as tables
import ora2pgs._triggers as triggers

# -----------------------------------------------


class BASE:
    """ Manage connections and parameters """

    def __init__(self, **kwargs):
        """
        Initialise the o2p class
        """

        self.conn_ora = None
        self.conn_pgs = None
        self.parameters = o2p.DEFAULT_PARAMETERS.copy()
        self.set_parameters(**kwargs)
        self.pls2pgs = o2p.DEFAULT_PLS2PGS.copy()
        self.stage = o2p.PRE
        self.file_number = 0

        self._validate_target_path()

        log_file = os.path.join(self.parameters[o2p.TARGET_PATH], 'o2p.log')
        handler = logging.FileHandler(log_file)
        self.logger = logging.getLogger('o2p')
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(handler)

    # -------------------------------------------

    def get_parameter(self, name: str):
        """
        Get a named parameter value

        :param name: the name of the parameter
        :return: the parameter value if it exists or None
        """

        return self.parameters.get(name)

    # ---

    def set_parameter(self, name: str, value):
        """
        Sets a single parameter with the specified value

        :param name: the name of the parameter
        :param value: the value of the parameter
        """

        self.parameters[name] = value

    # ---

    def set_parameters(self, **kwargs):
        """
        Sets multiple parameters with the specified values

        :param kwargs: the names and values of the parameters
        """

        if kwargs:
            self.parameters.update(**kwargs)

    # -------------------------------------------

    def set_pls2pgs(self, pls2pgs: dict):
        """
        Sets multiple pls2pgs mappings with the specified values

        :param pls2pgs: the names and values of the pls2pgs mappings
        """

        self.pls2pgs.update(pls2pgs)

    # -------------------------------------------

    def get_oracle_connection(self):
        """
        Gets the cx_oracle connection object

        :return: oracle connection object
        """

        if not self.conn_ora:
            if o2p.ORACLE_INSTANT_CLIENT not in self.parameters:
                raise Exception(f'"{o2p.ORACLE_INSTANT_CLIENT}" not set')
            if o2p.ORACLE_CONN not in self.parameters:
                raise Exception(f'"{o2p.ORACLE_CONN}" not set')

            cx_Oracle.init_oracle_client(lib_dir=self.parameters[o2p.ORACLE_INSTANT_CLIENT])
            self.conn_ora = cx_Oracle.connect(self.parameters[o2p.ORACLE_CONN], encoding='UTF-8')
            self.conn_ora.outputtypehandler = output_type_handler

        return self.conn_ora

    # -------------------------------------------

    def get_postgres_connection(self):
        """
        Gets the psycopg2 connection object

        :return: psycopg2 connection object
        """

        if not self.conn_pgs:
            if o2p.POSTGRES_CONN not in self.parameters:
                raise Exception(f'"{o2p.POSTGRES_CONN}" not set')

            self.conn_pgs = psycopg2.connect(self.parameters[o2p.POSTGRES_CONN])
            self.conn_pgs.set_session(autocommit=True)

        return self.conn_pgs

    # ---

    def initialise_postgres_schema(self):
        """
        Creates postgres schema, dropping it first, if it exists.
        """

        schema = self.parameters.get(o2p.POSTGRES_SCHEMA)
        if not schema:
            raise Exception('No postgres schema set')
        pconn = self.get_postgres_connection()
        pconn.cursor().execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        pconn.cursor().execute(f"CREATE SCHEMA {schema}")
        pconn.cursor().execute(f"SET search_path TO {schema}")

    # -------------------------------------------

    def execute_command(self, file, cmd: str):
        """
        Execute a command on postgres and write to a file

        :param file: the file name or file object
        :param cmd: the command string
        """

        cmd = cmd.replace('%%schema%%', self.parameters[o2p.POSTGRES_SCHEMA])

        if self.parameters[o2p.ETL_FILES] and file:
            file.write(cmd + '\n')
        if self.parameters[o2p.ETL_MIGRATE]:
            conn = self.get_postgres_connection()
            conn.cursor().execute(cmd)

    # -------------------------------------------

    def execute_command_file(self, file_path):
        """
        Executes the command file

        :param file_path: the path and file name
        """

        self._validate_target_path()
        cmd = self.file_read_to_string(file_path)
        if self.parameters[o2p.ETL_FILES]:
            self.file_number += 1
            cmd_target = f"{self.parameters[o2p.TARGET_PATH]}/{self.stage}/{str(self.file_number).zfill(6)}."
            cmd_target += os.path.basename(file_path)
            with open(cmd_target, 'w', encoding=self.parameters[o2p.ENCODING]) as file:
                self.execute_command(file, cmd)
        else:
            self.execute_command(None, cmd)

    # -------------------------------------------

    def execute_command_files(self, path: str):
        """
        Executes all files in a directory

        :param path: the directory path
        """

        for file in os.listdir(path):
            self.execute_command_file(f'{path}/{file}')

    # -------------------------------------------

    def execute_query(self, query: str):
        """
        Executes a query on the oracle database

        :param query: the query string
        :return: tuple of dd, dictionary data, and cd, cursor description
        """

        conn = self.get_oracle_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        cd = {c[0].lower(): i for i, c in enumerate(list(cursor.description))}
        dd = list(cursor.fetchall())
        cursor.close()

        return dd, cd

    # -------------------------------------------

    def execute_query_list(self, query: str, sep='\t'):
        """
        Executes a query on the oracle database

        :param query: the query string
        :param sep: character to seperate values
        :return: list of rows with seperated values
        """

        result = self.execute_query(query)[0]
        if sep:
            result = [sep.join([str(j) for j in i]) for i in result]

        return result

    # -------------------------------------------

    def file_read_json_file(self, filename: str):
        """
        Reads a json file to a dict

        :param filename: the filename including path
        :return: the json file as dict
        """

        with open(filename, encoding=self.parameters[o2p.ENCODING]) as jf:
            parameters = json.load(jf)
        return parameters

    # -------------------------------------------

    def file_read_to_string(self, filename: str, parameter_map=None):
        """
        Reads a file and return the file text as a string

        :param filename: the filename including path
        :param parameter_map: list of strings, encased by %%, to be replaced by parameters
        :return: the file content as a string
        """

        with open(filename, 'r', encoding=self.parameters[o2p.ENCODING]) as r:
            text = r.read()

        if parameter_map:
            for i in parameter_map:
                text = text.replace(f'%%{i}%%', self.parameters.get(i))

        return text

    # -------------------------------------------

    def file_write_from_string(self, filename: str, content: str, mode='a'):
        """
        Function to read a file and return the file text as a string

        :param filename: the filename including path
        :param content: the string to writeString to write
        :param mode: 'a'ppend, or 'w'rite
        """

        with open(filename, mode, encoding=self.parameters[o2p.ENCODING]) as text_file:
            text_file.write(content)  # Use write instead of print to avoid trailing new line

    # -------------------------------------------

    def get_filename(self, filename: str):
        """
        Gets the fully qualified filename

        :param filename:
        :return: filename with path
        """

        return f'{self.parameters[o2p.TARGET_PATH]}/{self.stage}/{self.file_number}.{filename}'

    # ---

    def open_file(self, filename: str, mode: str):
        """
        Creates a file, or not, depending on ETL_FILE parameter

        :param filename: the filename with path
        :param mode: the open file mode
        :return: the file object or None
        """

        if self.parameters[o2p.ETL_FILES]:
            return open(filename, mode, encoding=self.parameters[o2p.ENCODING])
        return None

    # ---

    @ staticmethod
    def close_file(file):
        """
        Closes the file, if it is a file

        :param file: the file object or None
        """

        if file:
            file.close()

    # -------------------------------------------

    def add_last_number(self):
        """
        Creates a set of sql files for creating sequences in postgresql
        """

        self.log('Add Last Number to Sequences')

        idx = {v['sequence_name']: k for k, v in self.parameters[o2p.SEQUENCES].items()}
        sequence_names = "'" + "','".join(list(idx)) + "'"

        query = f"SELECT sequence_name, last_number FROM user_sequences WHERE sequence_name IN ({sequence_names})"
        result = self.execute_query_list(query, None)
        for i in result:
            self.parameters[o2p.SEQUENCES][idx[i[0]]]['last_number'] = i[1]

    # -------------------------------------------

    def include_object(self, object_type: str, object_name: str):
        """
        Should the specified trigger be included in the export

        :param object_type: the object type
        :param object_name: the name of the trigger
        :return: boolean
        """

        if object_type not in o2p.OBJECT_TYPES:
            return False

        objs = self.parameters[object_type]

        return (
            False if f'{o2p.OBJECT_TYPES[object_type]} {object_name}' in self.parameters[o2p.EXCLUDE]
            else (objs if isinstance(objs, bool) else bool(object_name in objs))
        )

    # -------------------------------------------

    def log(self, line):
        """ todo """

        self.logger.info(line)
        if self.parameters.get(o2p.CONSOLE):
            print(line)

    # -------------------------------------------

    def tablespace_map(self, object_type: str, tablespace_name: str):
        """
        Maps existing tablespaces to target tablespace

        :param object_type: either TABLE or INDEX
        :param tablespace_name: the tablespace name to map
        :return: the target tablesapce name
        """

        tbs_map = self.parameters[o2p.TABLESPACE_MAP]
        if not tbs_map:
            return tablespace_name

        for i in [f'{object_type}.{tablespace_name}', tablespace_name, object_type]:
            if i in tbs_map:
                return tbs_map[i]

        return tablespace_name

    # -------------------------------------------

    def _validate_target_path(self):
        """
        Validates the target path and creates it if it does not exist
        """

        if self.parameters[o2p.TARGET_PATH]:
            if not self.parameters.get('_target_path_validated'):
                if self.parameters[o2p.ETL_FILES] or self.parameters[o2p.ETL_DATA]:
                    if os.path.exists(self.parameters[o2p.TARGET_PATH]):
                        raise Exception(f'Target path already exists: "{self.parameters[o2p.TARGET_PATH]}"')
                    os.makedirs(f'{self.parameters[o2p.TARGET_PATH]}/{o2p.PRE}')
                    os.makedirs(f'{self.parameters[o2p.TARGET_PATH]}/{o2p.ETL}')
                    os.makedirs(f'{self.parameters[o2p.TARGET_PATH]}/{o2p.POST}')
                else:
                    os.makedirs(f'{self.parameters[o2p.TARGET_PATH]}')

        elif self.parameters[o2p.ETL_FILES] or self.parameters[o2p.ETL_DATA]:
            raise Exception('Target path not set')

        self.parameters['_target_path_validated'] = True


# -----------------------------------------------


class MANAGER(BASE):
    """ The manager class, including all base modules and the run option, do_etl """

    def do_etl(self):
        """ todo """
        do_etl(self)


# -----------------------------------------------


def do_etl(base: BASE):
    """
    Runs the ETL functions for tables and the object database objects

    :param base: the base class containing connections and parameters
    """

    base.stage = o2p.ETL

    for i in [o2p.ETL_DATA, o2p.ETL_FILES, o2p.ETL_MIGRATE]:
        if i not in base.parameters:
            raise Exception(f'"{i}" not in parameters')

    if o2p.EXCLUDE not in base.parameters:
        base.parameters[o2p.EXCLUDE] = []

    # ---
    #  Connections

    base.get_oracle_connection()

    if base.parameters[o2p.ETL_MIGRATE]:
        pconn = base.get_postgres_connection()
        pconn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {base.parameters[o2p.POSTGRES_SCHEMA]}")
        pconn.cursor().execute(f"SET search_path TO {base.parameters[o2p.POSTGRES_SCHEMA]}")

    # ---
    #  Tables

    if not base.parameters[o2p.TABLES]:
        return

    if base.parameters[o2p.TABLES] is True:
        query = ' '.join([
            "SELECT ut.table_name",
            "  FROM user_tables ut",
            " WHERE ut.status = 'VALID'",
            "   AND tablespace_name IS NOT NULL",
            "   AND SUBSTR(table_name,1,4) != 'SYS_'"
        ])
        base.parameters[o2p.TABLE_NAMES] = base.execute_query_list(query)

    else:
        base.parameters[o2p.TABLE_NAMES] = base.parameters[o2p.TABLES]

    # ---
    #  Sequences

    seqs = base.parameters.get(o2p.SEQUENCES, {})
    if seqs:
        base.add_last_number()
        sequences.etl(base)

    # ---

    table_columns = {}  # Column details are populated as the table in built, for reference later

    for table_name in base.parameters[o2p.TABLE_NAMES]:
        base.log(f'Starting {table_name}  (1/2) {"." * (35 - len(table_name))} {datetime.datetime.now()}')
        table_columns[table_name] = []
        tables.etl(table_name, seqs.get(table_name), table_columns[table_name], base)
        if base.parameters[o2p.ETL_DATA]:
            data.etl(table_name, table_columns[table_name], base)

    for table_name in base.parameters[o2p.TABLE_NAMES]:
        base.log(f'Starting {table_name}  (2/2) {"." * (35 - len(table_name))} {datetime.datetime.now()}')
        if not base.parameters[o2p.CONSTRAINTS] is False:
            foreign_keys.etl(table_name, base)
        if not base.parameters[o2p.INDEXES] is False:
            indexes.etl(table_name, base)
        if not base.parameters[o2p.TRIGGERS] is False:
            triggers.etl(table_name, base)

    # ---

    base.stage = o2p.POST


# -----------------------------------------------


def output_type_handler(cursor, name, defaultType, size, precision, scale):  # noqa
    """ Converts Clob to Long to improve performance. Parameter names and order are specified by cx_Oracle """

    if defaultType == cx_Oracle.CLOB:
        return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize)


# -----------------------------------------------
# End.
