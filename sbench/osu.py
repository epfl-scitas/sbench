import re

from sqlalchemy import Column, Integer, Float, ForeignKey, String

from ._sql import Base
from .commands import parser


class OsuLatencyRow(Base):
    """Results of the osu_latency benchmark"""
    __tablename__ = 'OsuLatency'

    # Foreign keys to identify a job
    cluster = Column(String, ForeignKey('Jobs.cluster'), primary_key=True)
    jobid = Column(Integer, ForeignKey('Jobs.id'), primary_key=True)
    # Size of the message in bytes
    size = Column(Integer, primary_key=True, nullable=False)
    # Latency in micro-seconds
    latency = Column(Float)


class OsuBwRow(Base):
    """Results of the osu_bw benchmark"""
    __tablename__ = 'OsuBandwith'

    # Foreign keys to identify a job
    cluster = Column(String, ForeignKey('Jobs.cluster'), primary_key=True)
    jobid = Column(Integer, ForeignKey('Jobs.id'), primary_key=True)
    # Size of the message in bytes
    size = Column(Integer, primary_key=True, nullable=False)
    # Bandwidth in MB/sec.
    bandwidth = Column(Float)


class OsuBiBwRow(Base):
    """Results of the osu_bw benchmark"""
    __tablename__ = 'OsuBidirectionalBandwith'

    # Foreign keys to identify a job
    cluster = Column(String, ForeignKey('Jobs.cluster'), primary_key=True)
    jobid = Column(Integer, ForeignKey('Jobs.id'), primary_key=True)
    # Size of the message in bytes
    size = Column(Integer, primary_key=True, nullable=False)
    # Bandwidth in MB/sec.
    bandwidth = Column(Float)


class OsuAlltoallRow(Base):
    """Results of the osu_latency benchmark"""
    __tablename__ = 'OsuAlltoall'

    # Foreign keys to identify a job
    cluster = Column(String, ForeignKey('Jobs.cluster'), primary_key=True)
    jobid = Column(Integer, ForeignKey('Jobs.id'), primary_key=True)
    # Size of the message in bytes
    size = Column(Integer, primary_key=True, nullable=False)
    # Latency in micro-seconds
    latency = Column(Float)


class OsuAllreduceRow(Base):
    """Results of the osu_latency benchmark"""
    __tablename__ = 'OsuAllreduce'

    # Foreign keys to identify a job
    cluster = Column(String, ForeignKey('Jobs.cluster'), primary_key=True)
    jobid = Column(Integer, ForeignKey('Jobs.id'), primary_key=True)
    # Size of the message in bytes
    size = Column(Integer, primary_key=True, nullable=False)
    # Latency in micro-seconds
    latency = Column(Float)


# : Parse an int at the beginning of the line and a float at the end
int_and_float = re.compile('^([\d]+)[\s]*([\d.]+)$')


class _OsuParser(object):
    """Base class for any parser of an osu benchmark test."""
    def __init__(self, job, context):
        self.job = job
        self.context = context

    def update_sql_db(self, session):
        rows = []
        with open(self.job.output) as f:
            for line in f.readlines():
                r = self.osu_test_regex.match(line)
                if r:
                    item, created = self.query_or_create(session, r)
                    if created:
                        rows.append(item)

        session.add_all(rows)


class _OsuIntAndFloat(_OsuParser):
    #: Parses the size and latency of the osu_latency benchmark
    osu_test_regex = int_and_float

    def query_or_create(self, session, regexp):
        int_value, created = int(regexp.group(1)), False

        item = session.query(self.row_cls).filter_by(
            jobid=self.job.id, size=int_value
        ).first()

        if not item:
            float_value = float(regexp.group(2))
            kwargs = {
                'cluster': self.job.cluster,
                'jobid': self.job.id,
                self.int_tag: int_value,
                self.float_tag: float_value
            }
            item, created = self.row_cls(**kwargs), True

        return item, created


@parser('osu_latency')
class OsuLatency(_OsuIntAndFloat):
    #: Row in the correct DB table
    row_cls = OsuLatencyRow
    #: Size of the benchmark
    int_tag = 'size'
    #: Latency in micro-seconds
    float_tag = 'latency'


@parser('osu_bw')
class OsuBw(_OsuIntAndFloat):
    #: Row in the correct DB table
    row_cls = OsuBwRow
    #: Size of the benchmark
    int_tag = 'size'
    #: Latency in micro-seconds
    float_tag = 'bandwidth'


@parser('osu_bibw')
class OsuBiBw(_OsuIntAndFloat):
    #: Row in the correct DB table
    row_cls = OsuBiBwRow
    #: Size of the benchmark
    int_tag = 'size'
    #: Latency in micro-seconds
    float_tag = 'bandwidth'


@parser('osu_alltoall')
class OsuAlltoall(_OsuIntAndFloat):
    #: Row in the correct DB table
    row_cls = OsuAlltoallRow
    #: Size of the benchmark
    int_tag = 'size'
    #: Latency in micro-seconds
    float_tag = 'latency'


@parser('osu_allreduce')
class OsuAllreduce(_OsuIntAndFloat):
    #: Row in the correct DB table
    row_cls = OsuAllreduceRow
    #: Size of the benchmark
    int_tag = 'size'
    #: Latency in micro-seconds
    float_tag = 'latency'
