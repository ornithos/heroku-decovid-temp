import sqlalchemy
from sqlalchemy import Table, Column

# Note that it is much neater to use
# class MyClass(Base):
#     __table__ = Table('mytable', metadata,
#                     autoload=True, autoload_with=conn)
#
# But it is much less safe (i.e. if somehow table defs changes within the DB.)

def results_tables(metadata):
    tbl_results = Table('achilles_results', metadata,
        Column('analysis_id', sqlalchemy.INTEGER, nullable=True),
        Column('stratum_1', sqlalchemy.String(255), nullable=True),
        Column('stratum_2', sqlalchemy.String(255), nullable=True),
        Column('stratum_3', sqlalchemy.String(255), nullable=True),
        Column('stratum_4', sqlalchemy.String(255), nullable=True),
        Column('stratum_5', sqlalchemy.String(255), nullable=True),
        Column('count_value', sqlalchemy.BIGINT, nullable=True),
        schema='results'
    )

    tbl_results_dist = Table('achilles_results_dist', metadata,
        Column('analysis_id', sqlalchemy.INTEGER, nullable=True),
        Column('stratum_1', sqlalchemy.String(255), nullable=True),
        Column('stratum_2', sqlalchemy.String(255), nullable=True),
        Column('stratum_3', sqlalchemy.String(255), nullable=True),
        Column('stratum_4', sqlalchemy.String(255), nullable=True),
        Column('stratum_5', sqlalchemy.String(255), nullable=True),
        Column('count_value', sqlalchemy.BIGINT, nullable=True),
        Column('min_value', sqlalchemy.FLOAT, nullable=True),
        Column('max_value', sqlalchemy.FLOAT, nullable=True),
        Column('avg_value', sqlalchemy.FLOAT, nullable=True),
        Column('stdev_value', sqlalchemy.FLOAT, nullable=True),
        Column('median_value', sqlalchemy.FLOAT, nullable=True),
        Column('p10_value', sqlalchemy.FLOAT, nullable=True),
        Column('p25_value', sqlalchemy.FLOAT, nullable=True),
        Column('p75_value', sqlalchemy.FLOAT, nullable=True),
        Column('p90_value', sqlalchemy.FLOAT, nullable=True),
        schema='results'
    )

    return tbl_results, tbl_results_dist
