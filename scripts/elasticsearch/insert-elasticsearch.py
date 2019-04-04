#!/usr/bin/env python3

from elasticsearch import Elasticsearch, helpers
import sqlalchemy as sqa
import sqlalchemy.sql as ssql

SERVER = 'scitassrv16.epfl.ch'
INDEX = 'sbench'
DB = '/home/scitasbench/benchmarks/db/benchmarks.db'
TABLES = [
    'HPL',
    'OsuAllreduce',
    'OsuAlltoall',
    'OsuBandwith',
    'OsuBidirectionalBandwith',
    'OsuLatency',
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

        stm = ssql.select(cols).where(
            ssql.and_((jobs.c.id == tbl.c.jobid),
                      (jobs.c.cluster == tbl.c.cluster)))
        rs = con.execute(stm)
        a = rs.fetchall()
        return a
    return []

def prepare_insert():
    for table in TABLES:
        for results in query(table):
            _id = '{}_{}'.format(results['cluster'], results['id'])
            body = {
                '_id': _id,
                '_index': INDEX,
                '_type': table,
                '_op_type': 'index',
            }
            body.update(results)
            yield body


#
# MAIN
#
if __name__ == '__main__':
    l = open('/tmp/es_log', 'a+')
    client = Elasticsearch(hosts=SERVER)
    try:
        helpers.bulk(
            client,
            prepare_insert(),
        )
    except Exception as e:
        l.write(str(e) + '\n')
    l.close()

