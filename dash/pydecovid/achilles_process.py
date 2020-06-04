import os, sys
import pandas as pd
import numpy as np
import fire
from warnings import warn

import sqlalchemy
from sqlalchemy import select, cast, Table

# local python files
sys.path.append('./db')
import tabledefs     # achilles table defs
import valuesstmt    # extend SQL Alchemy with VALUES statement


def error_if_file_exists(fname):
    if os.path.isfile(fname):
        raise RuntimeError(f"File {fname:s} already exists. " + \
            "Use --force if you want to overwrite.")


def get_connection_str(**kwargs):
    
    dialect = kwargs.pop('dialect')
    def has(s): return len(kwargs[s]) > 0
    assert not dialect.lower() == 'sqlite', 'SQLite not supported'
    uidpwd = '{user:s}:{password:s}'.format(**kwargs) if not kwargs['trusted'] else ''
    driver = '+' + kwargs['driver'] if has('driver') else ''
    rdbms = f'{dialect:s}{driver:s}'
    url = '@' + kwargs['url'] if not has('dsn') else kwargs['dsn']
    db = '/' + kwargs['db'] if has('db') else ''
    
    # extract connection string
    connstr = f'{rdbms:s}://{uidpwd:s}{url:s}{db:s}'
    return connstr


def process_achilles_results(user='alexbird', password='', dialect='postgresql', 
    url='localhost', driver='', db='synpuf1k', dsn='', trusted=False, 
    dir_out='../data', dir_achilles='../../../Achilles', force=False, verbose=True):
    
    connection_string = get_connection_str(user=user, password=password, dialect=dialect, 
        url=url, driver=driver, db=db, dsn=dsn, trusted=trusted)

    # Before we do anything, check that we are not going to fall at the last hurdle
    path_output = os.path.join(dir_out, 'achilles_results.feather')
    if not force: error_if_file_exists(path_output)

    # Connect to DB
    engine = sqlalchemy.create_engine(connection_string)
    metadata = sqlalchemy.MetaData()


    # SAFETY: keep results tables schemas in `tablerefs` file and load explicitly
    # rather than using schema from existing connection
    #
    # ==> The OMOP schema is not likely to change, but Achilles might, or some
    # mistake might be made during the creation / running of the tool. We prefer
    # the code not to fail silently
    tbl_results, tbl_results_dist = tabledefs.results_tables(metadata);
    assert tbl_results.exists(engine), '(Standard) results table not found in the database.'
    assert tbl_results_dist.exists(engine), '(Distribution) results table not found in the database.'

    # Load CONCEPT metadata from database (see also metadata.reflect(engine))
    tbl_concept = Table('concept', metadata, autoload=True, autoload_with=engine)


    # _______________Annotate non-concept rows in results table_________________________________
    # Read in analysis definitions from Achilles
    verbose and print('Reading Achilles results schema...')
    achilles_schema = pd.read_csv(os.path.join(dir_achilles, 'inst/csv/achilles', 
        'achilles_analysis_details.csv'))
    achilles_schema = achilles_schema[['ANALYSIS_ID', 'STRATUM_1_NAME', 'STRATUM_2_NAME',
                                      'STRATUM_3_NAME', 'STRATUM_4_NAME', 'STRATUM_5_NAME', 
                                      'ANALYSIS_NAME']]
    achilles_schema['STRATUM_5_NAME'] = achilles_schema['STRATUM_5_NAME'].astype(object)

    # Define various non-concept splits from the Achilles definitions
    def ref_age(x):
        return (x == 'year_of_birth') | (x == 'age') | (x == 'age_decile') | (x == 'age decile')
    def ref_datetime(x):
        return (x == 'calendar_month') | (x == 'calendar month') | (x == 'calendar year')
    def ref_periods(x):
        return (x == 'payer plan period length 30d increments') |  \
        (x == 'Observation period length 30d increments') |  \
        (x == 'number of observation periods') |  \
        (x == 'number of payer plan periods')
    def ref_location(x):
        return (x == '3-digit zip') | (x == 'state')
    def ref_table(x):
        return (x == 'table name') | (x == 'table_name')
    def ref_other(x):
        return (x == 'source_value')
    def bad_concept(x):
        x = x.str.strip()
        return ref_age(x) | ref_datetime(x) | ref_location(x) | ref_periods(x) | ref_table(x) |   \
               ref_other(x) | x.isna()

    # Determine which strata / analysis_ids contain non-concept_ids using `bad_concept` above.
    not_concepts = [bad_concept(achilles_schema['STRATUM_{:d}_NAME'.format(i+1)]) for i in range(5)]
    not_concepts = [pd.DataFrame({'analysis_id': achilles_schema.ANALYSIS_ID, 
                                  'bad_{:d}'.format(i+1): x}) for i,x in enumerate(not_concepts)]

    # Read Achiles results table from the Database.
    # analysis_id > 2000000 are Achilles statistics (irrelevant)
    verbose and print('Querying DB for Achilles results...')
    q = tbl_results.select().where(tbl_results.c.analysis_id < 2000000)
    df_result = pd.read_sql(q, engine)

    # identify which of the stratum_{:d} values are not concept_ids
    for i in range(5):
        # determine bad concept based on definition in CSV
        df_result = pd.merge(df_result, not_concepts[i], how='left', on='analysis_id')
        
        # determine bad_concept based on value in results stratum
        s, b = 'stratum_{:d}'.format(i+1), 'bad_{:d}'.format(i+1)
        df_result[b] = df_result[b] | df_result[s].isna() | (df_result[s].str.len() == 0)

    # TEST: Check that all non-numeric strings have been labelled as 'non-concept_id'.
    def isdigit_or_none(x):
        return x.str.isdigit() | x.isna()

    for i in range(1,6):
        assert (~isdigit_or_none(df_result['stratum_{:d}'.format(i)]) &  \
                ~df_result['bad_{:d}'.format(i)]).sum() == 0, \
                'Achilles results table has non-numeric ' + \
                'codes that have *NOT* been removed by pre-processing (stratum_{:d}).'.format(i)


    # _______________Join these analysis_id/strata flags to results table________________________

    # Union all numeric concept_ids from *ALL* strata
    def is_concept_id(x): 
        return x is not None and x.isdigit()

    all_concepts = np.hstack([df_result['stratum_{:d}'.format(i)][
        ~df_result['bad_{:d}'.format(i)]].unique() for i in range(1,6)])
    all_concepts = list(filter(is_concept_id, all_concepts))
    all_concepts = [int(x) for x in all_concepts]
    all_concepts = np.unique(np.sort(all_concepts))

    # Make sure not too large
    len(all_concepts) >= 20000 and warn('all_concepts list is large: may run very slowly.');
    assert len(all_concepts) <= 100000, 'all_concepts list is over 100,000 concept IDs. The query is too large.'


    # Retrieve all the relevant concepts from the Database
    # Use a VALUES statement instead of a WHERE as there are often limits
    # on the size of a WHERE clause.
    val_concepts = valuesstmt.values(
        [
            sqlalchemy.column('concept_id', sqlalchemy.INTEGER),
        ],
        *[(x,) for x in all_concepts],
        alias_name='myconcepts',
    )

    # Join the concept_id values extracted above to the 'Concept' table in DB.
    # (in principle, not committed yet.)
    tbl_conceptjoin = tbl_concept.join(val_concepts, 
                                       tbl_concept.c.concept_id == val_concepts.c.concept_id)

    # Write and execute query to extract the relevant portion of the 'Concept' table.
    verbose and print('Querying relevant subset of concept table...')
    tc = tbl_concept.c  # shorthand
    df_concept = pd.read_sql(select([tc.concept_id,
                                     tc.concept_name,
                                     tc.domain_id])\
                             .select_from(tbl_conceptjoin).\
                             where(
                                    (tc.standard_concept=='S') and
                                    (tc.invalid_reason == None)
                                  ), 
                             engine)
    del tc              # delete shorthand


    assert df_concept.concept_id.nunique() == df_concept.shape[0],  \
        'Retrieved concept table does not have a unique primary key.'

    df_concept.index = df_concept.concept_id
    df_concept = df_concept.drop(columns='concept_id')


    # Merge each stratum in the results table with the concept_ids
    # (where the values correspond to concept_ids, o.w. leave as-is.)
    for i in range(1,6):
        c = 'stratum_{:d}'.format(i)
        is_ok = ~(df_result['bad_{:d}'.format(i)])
        s = df_result[c][is_ok].astype(int)
        df = pd.DataFrame(s)
        decoded = pd.merge(df, df_concept, how='left', left_on=c, right_index=True)
        decoded.loc[decoded['concept_name'].isna(), 'concept_name'] = 'No matching concept'
        df_result.loc[is_ok, c] = decoded['concept_name']
        
    # drop the non-concept flags
    df_result = df_result.drop(columns=list(filter(lambda x: x[:3] == 'bad', df_result.columns)))

    # Write results to disk
    if not force: error_if_file_exists(path_output)  # in case of race.
    verbose and print(f'Writing results to file: {path_output:s}... ', end='')
    df_result.to_feather(path_output)
    verbose and print('Success.')


if __name__ == '__main__':
    fire.Fire(process_achilles_results)
