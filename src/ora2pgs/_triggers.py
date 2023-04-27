#!/usr/bin/python3
# -----------------------------------------------
"""
    DESCRIPTION:
        Trigger module

    ASSUMPTIONS:
        No assumptions to note

    ACCURACY:
        No accuracy issues to note
"""
# -----------------------------------------------

import re

from ora2pgs import o2p

# -----------------------------------------------


def _get_pgsised_code(text: str, base):
    """ Replace Oracle code strings """

    for k, v in base.pls2pgs.items():
        text = text.replace(k, v).replace(k.lower(), v)

    return text


# -----------------------------------------------


def _process_create_triggers(table_name: str, base, file):
    """
    Builds the triggers
    """

    query = ' '.join([
        "SELECT ut.trigger_name, ut.trigger_type, ut.triggering_event, ut.when_clause",
        "  FROM user_triggers ut",
        f" WHERE ut.table_name = '{table_name}'"
    ])

    triggers, _ = base.execute_query(query)

    for trigger in triggers:
        if base.include_object(o2p.TRIGGERS, trigger[0]):
            _process_trg_function(trigger, base, file)
            _process_trg_trigger(trigger, table_name, base, file)


# -----------------------------------------------


def _process_trg_function(trigger: list, base, file):
    """ Builds a trigger function from the Oracle code """

    query = ' '.join([
        "SELECT us.text",
        "  FROM user_source us",
        f"WHERE us.name = '{trigger[0]}'",
        " ORDER BY us.line"
    ])

    dd = [d[0] for d in base.execute_query(query)[0]]
    body = False
    text = ''

    # ---

    for usl in dd:
        if not body and usl.strip().upper() in ['DECLARE', 'BEGIN']:
            body = True
        if body:
            if usl.strip().upper() == 'END;':
                text += '  RETURN NEW; \n'
            text += _get_pgsised_code(usl, base)
        elif usl.strip().upper().endswith(' DECLARE'):
            text += 'DECLARE \n'
            body = True
        elif usl.strip().upper().endswith(' BEGIN'):
            text += 'BEGIN \n'
            body = True

    # ---

    loop_vars = re.findall(r'\n\s*FOR\s+(\w+)\s+IN', text)

    if loop_vars:
        declare_loop_vars = '\n'.join(f'  {i} RECORD;' for i in loop_vars) + '\nBEGIN'
        if text.upper().startswith('BEGIN'):
            declare_loop_vars = '\nDECLARE' + declare_loop_vars
        text = re.sub('BEGIN', declare_loop_vars, text, count=1, flags=(re.I + re.M))

    # ---

    cmd = '\n'.join([
        f"CREATE FUNCTION {trigger[0]}_TF()",
        "RETURNS TRIGGER",
        "LANGUAGE PLPGSQL",
        "AS",
        "$$",
        text,
        "$$; \n\n"
    ])

    base.execute_command(file=file, cmd=cmd)


# -----------------------------------------------


def _process_trg_trigger(trigger: list, table_name: str, base, file):
    """ Creates the trigger stub """

    ba = trigger[1].split(' ', 1)[0]
    er = 'FOR EACH ROW' if trigger[1].endswith('EACH ROW') else ''
    wc = 'WHEN (' + trigger[3] + ')' if trigger[3] else ''

    cmd = '\n'.join([
        f'CREATE TRIGGER {trigger[0]}',
        f'{ba} {trigger[2]} ON {table_name} {er} {wc}',
        f'EXECUTE PROCEDURE {trigger[0]}_TF(); \n\n'
    ])

    base.execute_command(file=file, cmd=cmd)


# -----------------------------------------------


def etl(table_name, base):
    """
    Creates a set of sql files for creating tables in postgresql
    """

    base.log(f'Running... {table_name} Triggers')

    if base.parameters[o2p.ETL_FILES]:
        file_name = f'{base.parameters[o2p.TARGET_PATH]}/etl/{table_name}.3.sql'
        file = open(file_name, 'a', encoding=base.parameters[o2p.ENCODING])
        file.write(f'\n\\echo Running... {table_name} Triggers\n\n')
    else:
        file = None

    _process_create_triggers(table_name, base, file)

    if file:
        file.close()


# -----------------------------------------------
# End.
