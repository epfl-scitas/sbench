#!/usr/bin/env python3

from elasticsearch import Elasticsearch, helpers
import sqlalchemy as sqa
import sqlalchemy.sql as ssql

SERVER = 'scitassrv16.epfl.ch'
INDEX = 'sbench'
TYPE = 'hpl'
DB = '/home/scitasbench/benchmarks/db/benchmarks.db'

def query_hpl():
    engine = sqa.create_engine('sqlite:///' + DB)
    with engine.connect() as con:
        meta = sqa.MetaData(engine)
        jobs = sqa.Table('Jobs', meta, autoload=True)
        hpl = sqa.Table('HPL', meta, autoload=True)

        # filter cluster to avoid to have it twice in the results
        cols = [c for c in jobs.c if c.name != 'cluster']
        cols.extend([c for c in hpl.c if c.name != 'jobid'])

        stm = ssql.select(cols).where(
            ssql.and_((jobs.c.id == hpl.c.jobid),
                      (jobs.c.cluster == hpl.c.cluster)))
        rs = con.execute(stm)
        a = rs.fetchall()
        return a
    return []

def prepare_insert():
    for results in query_hpl():
        _id = '{}_{}'.format(results['cluster'], results['id'])
        body = {
            '_id': _id,
            '_index': INDEX,
            '_type': TYPE,
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

