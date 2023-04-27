
from ora2pgs import o2p

# -----------------------------------------------


def _process_sequences(sequences, base, file=None):
    """ Process the sequence list """

    schema = base.parameters[o2p.POSTGRES_SCHEMA]

    for tn in sorted(list(sequences)):
        seq = sequences[tn]
        base.execute_command(
            file=file,
            cmd=f"CREATE SEQUENCE {schema}.{seq['sequence_name']} INCREMENT 1 START {seq['last_number'] + 1};"
        )


# -----------------------------------------------


def etl(base):
    """
    Creates a set of sql files for creating sequences in postgresql
    """

    base.log('Creating Sequences')

    seqs = base.parameters[o2p.SEQUENCES]

    if base.parameters[o2p.ETL_FILES]:
        file_name = f'{base.parameters[o2p.TARGET_PATH]}/etl/sequences.sql'
        file = open(file_name, 'w', encoding=base.parameters[o2p.ENCODING])
        file.write('\\echo Creating Sequences\n\n')
    else:
        file = None

    _process_sequences(seqs, base, file)

    if file:
        file.close()


# -----------------------------------------------
# End.
