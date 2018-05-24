import click
import jinja2

import os
import os.path
import subprocess
import uuid

mpi_stacks = [
    ('gcc/6.4.0', 'mvapich2'),
    ('gcc/6.4.0', 'openmpi'),
    ('gcc/7.3.0', 'mvapich2'),
    ('gcc/6.4.0', 'openmpi'),
    ('intel', 'intel-mpi')
]

targets = [
#    'E5v2',
#    'E5v3',
    'E5v4',
    's6g1',
]

tests_info = {
    'osu_bw': {
        'subdir': 'pt2pt',
        'command': 'osu_bw',
        'configurations': [
            # (nnodes, ntasks)
            (1, 2),
            (2, 2)
        ]
    },
    'osu_bibw': {
        'subdir': 'pt2pt',
        'command': 'osu_bibw',
        'configurations': [
            # (nnodes, ntasks)
            (1, 2),
            (2, 2)
        ]
    }
}


@click.group()
def sosu():
    """SCITAS osu-micro-bechmarks runner"""
    pass


@sosu.command()
@click.option('--tests', default=None, help='Tests to be run (default: all tests)')
@click.argument('directory', type=click.Path(exists=True, file_okay=False, dir_okay=True, writable=True))
def run(tests, directory):
    """Runs the specified benchmark using slurm. Puts all relevant
    files in a tree starting from the directory passed in as a 
    parameter.
    """
    
    context = {}
    tests = tests or tests_info.keys()

    env = jinja2.Environment(loader=jinja2.PackageLoader('sosu', '.'))
    template = env.get_template('slurm_template.sh')
    
    for target in targets:
        context['target'] = target
        click.echo('[{0}]'.format(target))
        for compiler, mpi in mpi_stacks:
            context['compiler'] = compiler
            context['mpi'] = mpi
            click.echo('Benchmark for {0} {1}'.format(compiler, mpi))
            for test in tests:

                context['subdir'] = tests_info[test]['subdir']
                context['command'] = tests_info[test]['command']

                for nnodes, ntasks in tests_info[test]['configurations']:
                    
                    context['nnodes'] = nnodes
                    context['ntasks'] = ntasks

                    uid = str(uuid.uuid4())
                    test_directory = os.path.join(directory, uid)
                    batch_file = os.path.join(test_directory, 'slurm_batch.sh')
                    context['test_directory'] = test_directory
                    context['output_file'] = os.path.join(test_directory, 'run.%A.out')
                    context['error_file'] = os.path.join(test_directory, 'run.%A.err')

                    sbatch_content = template.render(**context)

                    os.makedirs(os.path.dirname(batch_file))
                    with open(batch_file, 'w') as f:
                        f.write(sbatch_content)
                                            
                    subprocess.call(['sbatch', batch_file])


@sosu.command()
@click.option('--db', default=None, help='The DB to be created or updated')
@click.argument('directory', type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True))
def collect(db, directory):
    """Collects the results of a previous run and stores them in a
    SQLite DB.
    """
    pass
