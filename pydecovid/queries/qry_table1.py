import pandas as pd

# __________________QUERY (TABLE 1)_____________________________________

# --------- ACHILLES GENERIC UTILS -------------------------------------
def _extract_achilles(df, analysis_id, num_strata):
    strata = ['stratum_{:d}'.format(i+1) for i in range(num_strata)]
    return df.loc[df.analysis_id == analysis_id, [*strata, 'count_value']]

def _generate_achilles_simple(analysis_id, num_strata):
    def achilles_simple(df):
        qry = _extract_achilles(df, analysis_id, num_strata)
        for i in range(num_strata):
            qry[f'stratum_{i+1}'] = qry[f'stratum_{i+1}'].str.capitalize()
        return qry.reset_index(drop=True)
    return achilles_simple

# Persons by Age
# ==> Note will sometimes need to fill in zeros (o.w. missing) on-the-fly in Dash.
def achilles_age(df, aggregate_to=1):
    c_year = 2020 # Date at time of study, not date.today().strftime("%Y")
    qry = qry = _extract_achilles(df, 3, 1)
    qry['stratum_1'] = qry['stratum_1'].astype(int) 
    qry['stratum_1'] = 2020 - qry['stratum_1']
    return qry.reset_index(drop=True)


# --------- UTILS ------------------------------------------------------

def aggregate_age(df, age_column, step, label=False, agg_below=None, sort="asc"):
    df[age_column] = (df[age_column] // step) * step
    if agg_below is not None:
        lt_cutoff = df[age_column] < agg_below
        df.loc[lt_cutoff, age_column] = -1  # to allow sorting
    df = df.groupby(age_column, as_index=False).sum()
    df[age_column] = df[age_column].sort_values(ascending = sort[:3]=='asc')\
        .reset_index(drop=True)
    if label:
        if agg_below is not None:
            gt_cutoff = df[age_column] != -1
            df.loc[gt_cutoff, age_column] = df.loc[gt_cutoff, age_column].map(
                lambda x: "{:d}-{:d}".format(x, x+step-1))
            df[age_column] = df[age_column].astype(str)
            df.loc[~gt_cutoff, age_column] = f"<{agg_below}"
        else:
            df[age_column] = df[age_column].map(lambda x: '{:d}-{:d}'.format(x, x+step-1))
    return df

def concat_pct_col(df, val_column='count_value'):
    pct_col = df[val_column] / sum(df[val_column])
    df['%'] = pct_col.map(lambda x: '{:.1f}'.format(100*x))
    return df

def title_row(df, title):
    df = df.astype(str)
    trow = pd.DataFrame([[title, *['' for i in range(df.shape[1]-1)]]], columns=df.columns)
    return pd.concat((trow, df), axis=0, ignore_index=True)
    




# --------- EXECUTE QUERIES AND FORMAT----------------------------------
# ----------------------------------------------------------------------


# --------- ACHILLES 'QUERY TEMPLATES' ---------------------------------
achilles_gender = _generate_achilles_simple(2, 1)
achilles_race = _generate_achilles_simple(4, 1)
achilles_ethnicity = _generate_achilles_simple(5, 1)
achilles_race_ethnicity = _generate_achilles_simple(12, 2)
def achilles_age_agg(df):
    return aggregate_age(achilles_age(df), 'stratum_1', 10, label=True, 
        agg_below=40, sort="asc")


def query(df):
    age = achilles_age_agg(df)
    gender = achilles_gender(df)
    race = achilles_race(df)
    ethnicity = achilles_ethnicity(df)


    return pd.concat((
        title_row(concat_pct_col(age), 'Age'),
        title_row(concat_pct_col(gender), 'Gender'),
        title_row(concat_pct_col(race), 'Race'),
        title_row(concat_pct_col(ethnicity), 'Ethnicity'),
        ), axis=0
    ).reset_index(drop=True)


# ______________________________________________________________________