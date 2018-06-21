import os
import jinja2
from math import sqrt
from sqlalchemy import Column, Integer, Float, ForeignKey, String

from ._sql import Base
from .commands import parser, preparator, targets_info


class HPLRow(Base):
    """Results of the hpl benchmark"""
    __tablename__ = 'HPL'

    # Foreign keys to identify a job
    cluster = Column(String, ForeignKey('Jobs.cluster'), primary_key=True)
    jobid = Column(Integer, ForeignKey('Jobs.id'), primary_key=True)

    # Size of the message in bytes
    flops = Column(Float)


@parser('hpl')
class HPLParser(object):
    """Output parser for the hpl benchmarck."""
    def __init__(self, job, context):
        self.job = job
        self.context = context


def _get_dimensions(n):
        n = int(n)
        divisors = (i for i in range(1, int(sqrt(n)+1)) if n % i == 0)
        min_divisor = min(divisors, key=lambda d: abs(d - sqrt(n)))

        return min_divisor, n // min_divisor


@preparator('hpl')
class HPLPreparator(object):
    block_size = 256
    memory_percent = 82

    """Input file preparator for hpl benchmarks."""
    def __init__(self, directory, context):
        self.directory = directory
        self.context = context

    def prepare(self):
        input_file = os.path.join(self.directory, 'HPL.dat')

        target_info = targets_info[self.context['target']]

        if not self.context['ntasks']:
            self.context['ntasks'] = target_info['ncores'] * \
                self.context['nnodes']
        mem = self.memory_percent / 100 * min(target_info['mem']) * \
            self.context['nnodes']
        mem = (int(mem * 2**20) // self.block_size) * self.block_size

        if 'intel' in self.context['compiler']:
            self.context['blas'] = 'intel-mkl'
        else:
            self.context['blas'] = 'openblas'

        self.context['P'], self.context['Q'] = \
            _get_dimensions(self.context['ntasks'])
        self.context['NB'] = self.block_size
        self.context['memory'] = mem
        self.context['memory_percent'] = self.memory_percent

        env = jinja2.Environment(
            loader=jinja2.PackageLoader('sbench', 'templates'))

        template = env.get_template('HPL.dat')

        input_content = template.render(**self.context)
        with open(input_file, 'w') as f:
            f.write(input_content)
