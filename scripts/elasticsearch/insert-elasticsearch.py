#!/usr/bin/env python3

from elasticsearch import Elasticsearch, helpers
import sqlalchemy as sqa
import sqlalchemy.sql as ssql
import arrow

SERVER = 'scitassrv16.epfl.ch'
INDEX = 'sbench'
DB = '/home/scitasbench/benchmarks/db/benchmarks.db'
TABLES = [
        {'table': 'HPL',                      'id': ['cluster', 'id']},
        {'table': 'OsuAllreduce',             'id': ['cluster', 'id', 'size']},
        {'table': 'OsuAlltoall',              'id': ['cluster', 'id', 'size']},
        {'table': 'OsuBandwith',              'id': ['cluster', 'id', 'size']},
        {'table': 'OsuBidirectionalBandwith', 'id': ['cluster', 'id', 'size']},
        {'table': 'OsuLatency',               'id': ['cluster', 'id', 'size']},
]

def query(table):
    engine = sqa.create_engine('sqlite:///' + DB)
    with engine.connect() as con:
        meta = sqa.MetaData(engine)
        jobs = sqa.Table('Jobs', meta, autoload=True)
        tbl = sqa.Table(table, meta, autoload=True)

        # filter cluster to avoid to have it twice in the results
        cols = [c for c in jobs.c if c.name != 'cluster']
        cols.extend([c for c in tbl.c if c.name != 'jobid'])
        date_from = arrow.get().shift(days=-2).format('YYYY-MM-DD HH:mm:ss.0000')

        stm = ssql.select(cols).where(
            ssql.and_((jobs.c.id == tbl.c.jobid),
                      (jobs.c.cluster == tbl.c.cluster),
                      (jobs.c.start > date_from)))
        rs = con.execute(stm)
        a = rs.fetchall()
        return a
    return []

def prepare_insert():
    for info in TABLES:
        _table = info['table']
        for results in query(_table):
            _id = '_'.join([str(results[k]) for k in info['id']])
            body = {
                '_id': _id,
                '_index': INDEX,
                '_type': _table,
                '_op_type': 'index',
            }
            body.update(results)
            yield body


#
# MAIN
#
if __name__ == '__main__':
    l = open('/tmp/es_log_jba', 'a+')
    client = Elasticsearch(hosts=SERVER)
    try:
        helpers.bulk(
            client,
            prepare_insert(),
        )
    except Exception as e:
        l.write(str(e) + '\n')
    l.close()

