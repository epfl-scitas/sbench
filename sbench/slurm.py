import datetime
import glob
import os
import re

from sqlalchemy import Column, DateTime, Integer, String

from ._sql import Base


def read_date_file(filename):
    """Reads a date contained in a text file.

    Args:
        filename (path): file containing the date to be read

    Returns:
        The corresponding ``datetime`` object
    """
    date_fmt = '%a, %d %b %Y %X %z'
    with open(filename) as f:
        date_str = ''.join(f.readlines()).strip()
        return datetime.datetime.strptime(date_str, date_fmt)


class JobRow(Base):
    """Describes a job entry in a table of jobs"""
    __tablename__ = 'Jobs'

    cluster = Column(String, primary_key=True, nullable=False)
    id = Column(Integer, primary_key=True, nullable=False)
    start = Column(DateTime)
    finish = Column(DateTime)
    nnodes = Column(Integer)
    ntasks = Column(Integer)
    target = Column(String)
    compiler = Column(String)
    lapack = Column(String)
    python = Column(String)
    mpi = Column(String)
    nodelist = Column(String, nullable=False)
    root = Column(String, nullable=False)


class SlurmJob(object):
    #: Regex needed to parse information related to the Slurm job
    regexps = {
        'id': re.compile('run.(\d*).start'),
        'nodelist': re.compile('SLURM_NODELIST=([\w\[,\]-]*)$'),
        'cluster': re.compile('SLURM_CLUSTER_NAME=([\w\[,\]]*)$'),
        'nnodes': re.compile('SLURM_NNODES=([\d]*)$'),
        'ntasks': re.compile('SLURM_NTASKS=([\d]*)$'),
        'target': re.compile('SPACK_TARGET_TYPE=([\d\w_]*)$')
    }

    def __init__(self, root, context):
        self.root = root
        self.files = {
            'output': glob.glob(os.path.join(root, '*.out')),
            'error': glob.glob(os.path.join(root, '*.err')),
            'environment': glob.glob(os.path.join(root, '*.env')),
            'start': glob.glob(os.path.join(root, '*.start')),
            'finish': glob.glob(os.path.join(root, '*.finished')),
        }

        for key, value in self.files.items():
            setattr(self, key, value[0])

        self.id = self.regexps['id'].search(self.start).group(1)
        self.context = context
        self.cluster = None

    def update_sql_db(self, session):

        to_be_parsed = {'nodelist', 'cluster', 'nnodes', 'ntasks', 'target'}
        kwargs = {
            'id': self.id,
            'compiler': self.context['compiler'],
            'mpi': self.context['mpi'],
            'root': self.root
        }

        with open(self.environment, 'r') as f:
            for line in f.readlines():
                for item in list(to_be_parsed):
                    t = self.regexps[item].search(line)
                    if t:
                        kwargs[item] = t.group(1)
                        to_be_parsed.remove(item)

                if not to_be_parsed:
                    break

        self.cluster = kwargs['cluster']
        job_row = session.query(JobRow).filter_by(id=self.id).first()

        if not job_row:
            kwargs['start'] = read_date_file(self.start)
            kwargs['finish'] = read_date_file(self.finish)
            job_row = JobRow(**kwargs)
            session.add(job_row)
